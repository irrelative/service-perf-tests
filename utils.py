#!/usr/bin/env python3
"""
Shared utilities for orchestration/choreography benchmarks.

Includes:
- AWS client constructors with tuned connection pools
- S3 helpers using boto3 TransferManager for parallel I/O
- Serialization helpers (json, json-gz, raw-gz, pickle, protobuf)
- Simple transform and payload generation helpers
- Filesystem helpers for metrics output
"""
import base64
import gzip
import io
import json
import os
import pickle
import time
from typing import Dict, Optional, Tuple, List

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config


def parse_bool(value: str) -> bool:
    return value.lower() in ("1", "true", "t", "yes", "y", "on")


def make_boto3_s3_client(profile: str = "", region: str = ""):
    session = boto3.Session(profile_name=profile or None, region_name=region or None)
    cfg = Config(max_pool_connections=64)
    return session.client("s3", config=cfg)


def make_boto3_clients(profile: str = "", region: str = ""):
    session = boto3.Session(profile_name=profile or None, region_name=region or None)
    cfg = Config(max_pool_connections=64)
    return session.client("s3", config=cfg), session.client("sqs", config=cfg)


def build_s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def serializer_ext(serializer: str) -> str:
    if serializer == "json":
        return "json"
    if serializer == "json-gz":
        return "json.gz"
    if serializer == "raw-gz":
        return "gz"
    if serializer == "pickle":
        return "pkl"
    if serializer == "protobuf":
        return "pb"
    raise ValueError(f"Unsupported serializer: {serializer}")


def serialize_payload(data: bytes, serializer: str) -> Tuple[bytes, str, float]:
    """
    Returns (body_bytes, content_type, serialize_seconds)
    """
    t0 = time.perf_counter()
    if serializer == "json":
        b64 = base64.b64encode(data).decode("ascii")
        obj = {"type": "bytes", "encoding": "base64", "data": b64}
        body = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        ctype = "application/json"
    elif serializer == "json-gz":
        compressed = gzip.compress(data, compresslevel=1)
        b64 = base64.b64encode(compressed).decode("ascii")
        obj = {"type": "bytes", "encoding": "base64", "compression": "gzip", "data": b64}
        body = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        ctype = "application/json"
    elif serializer == "raw-gz":
        body = gzip.compress(data, compresslevel=1)
        ctype = "application/octet-stream"
    elif serializer == "pickle":
        body = pickle.dumps(data, protocol=pickle.HIGHEST_PROTOCOL)
        ctype = "application/octet-stream"
    elif serializer == "protobuf":
        from google.protobuf.wrappers_pb2 import BytesValue as _BytesValue
        msg = _BytesValue(value=data)
        body = msg.SerializeToString()
        ctype = "application/octet-stream"
    else:
        raise ValueError(f"Unsupported serializer: {serializer}")
    return body, ctype, time.perf_counter() - t0


def deserialize_payload(body: bytes, serializer: str) -> Tuple[bytes, float]:
    """
    Returns (payload_bytes, deserialize_seconds)
    """
    t0 = time.perf_counter()
    if serializer == "json":
        obj = json.loads(body.decode("utf-8"))
        if not (isinstance(obj, dict) and obj.get("encoding") == "base64"):
            raise ValueError("Unexpected JSON payload shape (expected base64-encoded object)")
        payload = base64.b64decode(obj["data"].encode("ascii"))
    elif serializer == "json-gz":
        obj = json.loads(body.decode("utf-8"))
        if not (isinstance(obj, dict) and obj.get("encoding") == "base64" and obj.get("compression") == "gzip"):
            raise ValueError("Unexpected JSON-GZ payload shape (expected base64-encoded gzip object)")
        compressed = base64.b64decode(obj["data"].encode("ascii"))
        payload = gzip.decompress(compressed)
    elif serializer == "raw-gz":
        payload = gzip.decompress(body)
    elif serializer == "pickle":
        import pickle
        payload = pickle.loads(body)
        if not isinstance(payload, (bytes, bytearray)):
            raise ValueError("Unexpected pickle payload type (expected bytes)")
        payload = bytes(payload)
    elif serializer == "protobuf":
        from google.protobuf.wrappers_pb2 import BytesValue as _BytesValue
        msg = _BytesValue()
        msg.ParseFromString(body)
        payload = bytes(msg.value)
    else:
        raise ValueError(f"Unsupported serializer: {serializer}")
    return payload, time.perf_counter() - t0


def s3_put(
    s3,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
    metadata: Optional[Dict[str, str]] = None,
) -> float:
    """
    Uploads to S3 and returns upload_seconds.
    """
    t0 = time.perf_counter()
    buf = io.BytesIO(body)
    transfer_cfg = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        multipart_chunksize=8 * 1024 * 1024,
        max_concurrency=16,
        use_threads=True,
    )
    s3.upload_fileobj(
        Fileobj=buf,
        Bucket=bucket,
        Key=key,
        ExtraArgs={"ContentType": content_type, "Metadata": metadata or {}},
        Config=transfer_cfg,
    )
    return time.perf_counter() - t0


def s3_get(s3, bucket: str, key: str) -> Tuple[bytes, Dict[str, str], float]:
    """
    Downloads from S3 and returns (body, metadata, download_seconds).
    """
    t0 = time.perf_counter()
    head = s3.head_object(Bucket=bucket, Key=key)
    md = head.get("Metadata", {}) or {}
    buf = io.BytesIO()
    transfer_cfg = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        multipart_chunksize=8 * 1024 * 1024,
        max_concurrency=16,
        use_threads=True,
    )
    s3.download_fileobj(Bucket=bucket, Key=key, Fileobj=buf, Config=transfer_cfg)
    body = buf.getvalue()
    return body, md, time.perf_counter() - t0


def noop_transform(payload: bytes) -> Tuple[bytes, float]:
    t0 = time.perf_counter()
    _ = payload[:1]
    return payload, time.perf_counter() - t0


def make_compressible_bytes(total_size: int, incompressible_fraction: float = 0.25) -> bytes:
    """
    Generate bytes that compress well with gzip. Approximately `incompressible_fraction`
    of the data is random; the rest is a small repeating pattern. With gzip, this
    typically yields a compressed size around the incompressible_fraction of the original.
    """
    if total_size <= 0:
        return b""
    rand_size = int(total_size * incompressible_fraction)
    comp_size = total_size - rand_size
    pattern = b"0123456789ABCDEF"
    if comp_size > 0:
        reps = (comp_size // len(pattern)) + 1
        comp_bytes = (pattern * reps)[:comp_size]
    else:
        comp_bytes = b""
    rand_bytes = os.urandom(rand_size) if rand_size > 0 else b""
    return comp_bytes + rand_bytes


def ensure_out_dir() -> str:
    out_dir = os.path.join(".", "out")
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


def write_metrics_line(run_id: str, path: str, record: Dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps({"run_id": run_id, **record}, separators=(",", ":")) + "\n")


def s3_delete_objects(s3, bucket: str, keys: List[str]) -> None:
    if not keys:
        return
    objs = [{"Key": k} for k in keys]
    s3.delete_objects(Bucket=bucket, Delete={"Objects": objs, "Quiet": True})
