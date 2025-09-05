#!/usr/bin/env python3
"""
Orchestration Service Tester (S3 Latency + Serialization Benchmark)

- Loads configuration from .env (via python-dotenv) and CLI flags.
- Seeds an initial large payload to S3.
- Runs a chain of steps; each step:
  - downloads previous step's payload,
  - deserializes,
  - performs a transform (noop by default),
  - serializes,
  - uploads new payload to S3 and returns its S3 URI.
- Emits per-step metrics to ./out/metrics-<RUN_ID>.jsonl
"""

import argparse
import json
import os
import sys
import time
import uuid
import statistics
from datetime import datetime
from typing import Dict, Tuple, Optional

from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

from utils import (
    parse_bool,
    make_boto3_s3_client,
    build_s3_uri,
    serializer_ext,
    serialize_payload,
    deserialize_payload,
    s3_put,
    s3_get,
    noop_transform,
    make_compressible_bytes,
    ensure_out_dir,
    write_metrics_line,
    s3_delete_objects,
)


def load_config_from_env() -> Dict[str, str]:
    load_dotenv()  # Load .env if present

    cfg: Dict[str, str] = {}
    cfg["AWS_PROFILE"] = os.getenv("AWS_PROFILE", "")
    cfg["AWS_REGION"] = os.getenv("AWS_REGION", "")  # Let SDK resolve if not set

    cfg["ORCH_BUCKET"] = os.getenv("ORCH_BUCKET", "")
    cfg["ORCH_PREFIX"] = os.getenv("ORCH_PREFIX", "orchestration-bench/runs")

    cfg["STEPS"] = os.getenv("STEPS", "5")
    cfg["PAYLOAD_MB"] = os.getenv("PAYLOAD_MB", "50")
    cfg["SERIALIZER"] = os.getenv("SERIALIZER", "json").lower()
    cfg["CLEANUP"] = os.getenv("CLEANUP", "false").lower()

    return cfg






def parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    _, rest = uri.split("s3://", 1)
    bucket, key = rest.split("/", 1)
    return bucket, key






















def orchestrate_chain(
    s3,
    bucket: str,
    prefix: str,
    steps: int,
    payload_mb: float,
    serializer: str,
    run_id: str,
    cleanup: bool,
) -> None:
    out_dir = ensure_out_dir()
    metrics_path = os.path.join(out_dir, f"metrics-{run_id}.jsonl")

    # Seed initial payload
    payload_size = int(round(payload_mb * 1024 * 1024))
    seed_bytes = make_compressible_bytes(payload_size, incompressible_fraction=0.25)
    seed_key = f"{prefix.rstrip('/')}/step-000.{serializer_ext(serializer)}"
    seed_uri = build_s3_uri(bucket, seed_key)

    seed_body, seed_ctype, seed_serialize_s = serialize_payload(seed_bytes, serializer)
    seed_upload_s = s3_put(
        s3,
        bucket,
        seed_key,
        seed_body,
        seed_ctype,
        metadata={
            "serializer": serializer,
            "run_id": run_id,
            "step": "0",
        },
    )

    write_metrics_line(
        run_id,
        metrics_path,
        {
            "phase": "seed",
            "step_index": 0,
            "output_s3_uri": seed_uri,
            "bytes_uploaded": len(seed_body),
            "serialize_s": round(seed_serialize_s, 6),
            "upload_s": round(seed_upload_s, 6),
            "payload_mb": payload_mb,
            "serializer": serializer,
        },
    )
    print(f"[seed] uploaded initial payload to {seed_uri} ({payload_mb} MB, serializer={serializer})")

    produced_keys = [seed_key]
    prev_key = seed_key
    prev_uri = seed_uri

    # Run steps 1..N
    for i in range(1, steps + 1):
        step_start = time.perf_counter()

        # Download previous
        body_bytes, metadata, download_s = s3_get(s3, bucket, prev_key)
        meta_serializer = (metadata.get("serializer") or serializer).lower()
        if meta_serializer != serializer:
            # Stick with object-declared serializer to ensure correct decode
            current_serializer = meta_serializer
        else:
            current_serializer = serializer

        # Deserialize
        payload_bytes, deserialize_s = deserialize_payload(body_bytes, current_serializer)

        # Transform (noop)
        transformed_bytes, transform_s = noop_transform(payload_bytes)

        # Serialize
        out_body, out_ctype, serialize_s = serialize_payload(transformed_bytes, current_serializer)

        # Upload
        next_key = f"{prefix.rstrip('/')}/step-{i:03d}.{serializer_ext(current_serializer)}"
        next_uri = build_s3_uri(bucket, next_key)
        upload_s = s3_put(
            s3,
            bucket,
            next_key,
            out_body,
            out_ctype,
            metadata={
                "serializer": current_serializer,
                "run_id": run_id,
                "step": str(i),
            },
        )
        step_wall_s = time.perf_counter() - step_start

        # Metrics
        write_metrics_line(
            run_id,
            metrics_path,
            {
                "phase": "step",
                "step_index": i,
                "input_s3_uri": prev_uri,
                "output_s3_uri": next_uri,
                "bytes_downloaded": len(body_bytes),
                "bytes_uploaded": len(out_body),
                "download_s": round(download_s, 6),
                "deserialize_s": round(deserialize_s, 6),
                "transform_s": round(transform_s, 6),
                "serialize_s": round(serialize_s, 6),
                "upload_s": round(upload_s, 6),
                "step_wall_s": round(step_wall_s, 6),
                "serializer": current_serializer,
            },
        )

        print(
            f"[step {i}/{steps}] {prev_uri} -> {next_uri} | "
            f"dl={download_s:.3f}s de={deserialize_s:.3f}s tr={transform_s:.3f}s "
            f"se={serialize_s:.3f}s ul={upload_s:.3f}s wall={step_wall_s:.3f}s"
        )

        produced_keys.append(next_key)
        prev_key = next_key
        prev_uri = next_uri

    print(f"Run complete. Metrics written to {metrics_path}")

    if cleanup:
        s3_delete_objects(s3, bucket, produced_keys)
        print(f"Cleanup complete. Deleted {len(produced_keys)} objects from s3://{bucket}/{prefix.rstrip('/')}/")


def build_arg_parser(env_cfg: Dict[str, str]) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="S3 Orchestration Latency + Serialization Benchmark")
    p.add_argument("--bucket", default=env_cfg["ORCH_BUCKET"], help="S3 bucket for payloads (required if not in env)")
    p.add_argument("--prefix", default=env_cfg["ORCH_PREFIX"], help="S3 prefix for this run (default: orchestration-bench/runs)")
    p.add_argument("--steps", type=int, default=int(env_cfg["STEPS"]), help="Number of steps in the chain (default: 5)")
    p.add_argument("--payload-mb", type=float, default=float(env_cfg["PAYLOAD_MB"]), help="Payload size in MB (float, default: 50)")
    p.add_argument("--serializer", choices=["json", "json-gz", "raw-gz", "pickle"], default=env_cfg["SERIALIZER"], help="Serialization format (default: json)")
    p.add_argument("--cleanup", action="store_true" if parse_bool(env_cfg["CLEANUP"]) else "store_false", help="Delete S3 objects after run")
    p.add_argument("--run-id", default="", help="Run identifier (default: auto)")
    p.add_argument("--aws-profile", default=env_cfg["AWS_PROFILE"], help="AWS profile name (optional)")
    p.add_argument("--aws-region", default=env_cfg["AWS_REGION"], help="AWS region (optional; SDK default if blank)")
    p.add_argument("--repeats", type=int, default=10, help="Repeat the run this many times to compute summary stats (default: 10)")
    return p


def main(argv=None) -> int:
    env_cfg = load_config_from_env()
    parser = build_arg_parser(env_cfg)
    args = parser.parse_args(argv)

    if not args.bucket:
        print("ERROR: --bucket (or ORCH_BUCKET) is required.", file=sys.stderr)
        return 2

    run_id = args.run_id or (datetime.utcnow().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8])
    base_prefix = f"{args.prefix.rstrip('/')}/{run_id}"

    try:
        s3 = make_boto3_s3_client(profile=args.aws_profile, region=args.aws_region or None)
    except (BotoCoreError, ClientError) as e:
        print(f"ERROR: failed to create S3 client: {e}", file=sys.stderr)
        return 3

    walls = []
    for i in range(int(args.repeats)):
        child_run_id = run_id if int(args.repeats) == 1 else f"{run_id}-r{i+1:02d}"
        repeat_prefix = base_prefix if int(args.repeats) == 1 else f"{base_prefix}/{child_run_id}"

        t0 = time.perf_counter()
        try:
            orchestrate_chain(
                s3=s3,
                bucket=args.bucket,
                prefix=repeat_prefix,
                steps=args.steps,
                payload_mb=args.payload_mb,
                serializer=args.serializer,
                run_id=child_run_id,
                cleanup=bool(args.cleanup),
            )
        except (BotoCoreError, ClientError, ValueError) as e:
            print(f"ERROR during orchestration: {e}", file=sys.stderr)
            return 4
        finally:
            wall = time.perf_counter() - t0
            walls.append(wall)
            print(f"End-to-end wall time: {wall:.3f}s (run_id={child_run_id})")

    if walls:
        min_s = min(walls)
        max_s = max(walls)
        avg_s = sum(walls) / len(walls)
        stdev_s = statistics.stdev(walls) if len(walls) >= 2 else 0.0
        median_s = statistics.median(walls)

        print(
            f"Summary over {len(walls)} runs (base run_id={run_id}): "
            f"min={min_s:.3f}s max={max_s:.3f}s avg={avg_s:.3f}s stdev={stdev_s:.3f}s median={median_s:.3f}s"
        )

        out_dir = ensure_out_dir()
        summary_path = os.path.join(out_dir, f"summary-{run_id}.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "tool": "s3_orch_bench",
                    "base_run_id": run_id,
                    "repeats": len(walls),
                    "times_s": [round(x, 6) for x in walls],
                    "min_s": round(min_s, 6),
                    "max_s": round(max_s, 6),
                    "avg_s": round(avg_s, 6),
                    "stdev_s": round(stdev_s, 6),
                    "median_s": round(median_s, 6),
                    "settings": {
                        "bucket": args.bucket,
                        "prefix": args.prefix,
                        "computed_base_prefix": f"{args.prefix.rstrip('/')}/{run_id}",
                        "steps": args.steps,
                        "payload_mb": args.payload_mb,
                        "serializer": args.serializer,
                        "cleanup": bool(args.cleanup),
                        "aws_profile": args.aws_profile,
                        "aws_region": args.aws_region,
                    },
                },
                f,
                separators=(",", ":"),
            )
        print(f"Wrote summary to {summary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
