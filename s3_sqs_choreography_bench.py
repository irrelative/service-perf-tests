#!/usr/bin/env python3
"""
Choreography Service Tester (S3 + SQS, multi-process workers)

- Loads configuration from .env (via python-dotenv) and CLI flags.
- Spawns separate processes, one per step, each long-polling its own SQS queue.
- Seeds an initial payload to S3, then enqueues the first step message.
- Each step process:
  - receives message with prior step's S3 URI,
  - downloads and deserializes payload,
  - performs a transform (noop by default),
  - serializes and uploads new payload to S3,
  - enqueues the next step's message (or a final "done" control message).
- Emits per-step metrics to ./out/metrics-<RUN_ID>.jsonl
"""

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime
from typing import Dict, Tuple, Optional, List
from multiprocessing import Process, Event

from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

from utils import (
    parse_bool,
    make_boto3_clients,
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


# -----------------------------
# Shared helpers (mirrored from s3_orch_bench.py for consistency)
# -----------------------------

def load_config_from_env() -> Dict[str, str]:
    load_dotenv()  # Load .env if present

    cfg: Dict[str, str] = {}
    cfg["AWS_PROFILE"] = os.getenv("AWS_PROFILE", "")
    cfg["AWS_REGION"] = os.getenv("AWS_REGION", "")  # Let SDK resolve if not set

    cfg["ORCH_BUCKET"] = os.getenv("ORCH_BUCKET", "")
    cfg["ORCH_PREFIX"] = os.getenv("ORCH_PREFIX", "orchestration-bench/runs")
    cfg["ORCH_QUEUE_PREFIX"] = os.getenv("ORCH_QUEUE_PREFIX", "orchestration-bench-queue")

    cfg["STEPS"] = os.getenv("STEPS", "5")
    cfg["PAYLOAD_MB"] = os.getenv("PAYLOAD_MB", "50")
    cfg["SERIALIZER"] = os.getenv("SERIALIZER", "json").lower()
    cfg["CLEANUP"] = os.getenv("CLEANUP", "false").lower()
    cfg["CLEANUP_QUEUES"] = os.getenv("CLEANUP_QUEUES", "true").lower()

    return cfg


























# -----------------------------
# SQS helpers
# -----------------------------

def ensure_queue(sqs, name: str, visibility_timeout: int = 300, receive_wait: int = 20) -> str:
    """
    Ensure an SQS queue exists and return its URL.
    """
    # Try to get, else create
    try:
        resp = sqs.get_queue_url(QueueName=name)
        return resp["QueueUrl"]
    except sqs.exceptions.QueueDoesNotExist:
        pass

    attributes = {
        "VisibilityTimeout": str(visibility_timeout),
        "ReceiveMessageWaitTimeSeconds": str(receive_wait),  # long poll
    }
    resp = sqs.create_queue(
        QueueName=name,
        Attributes=attributes,
    )
    return resp["QueueUrl"]


def delete_queue(sqs, queue_url: str) -> None:
    try:
        sqs.delete_queue(QueueUrl=queue_url)
    except Exception:
        pass


def send_json_message(sqs, queue_url: str, obj: Dict) -> None:
    resp = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(obj, separators=(",", ":")))
    # Log enqueue event
    msg_id = resp.get("MessageId", "")
    run_id = obj.get("run_id")
    step_index = obj.get("step_index")
    print(f"[enqueue] run_id={run_id} step={step_index} queue={queue_url} message_id={msg_id}")


def receive_one_message(sqs, queue_url: str, wait_seconds: int = 20) -> Optional[Dict]:
    resp = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=wait_seconds,
    )
    msgs = resp.get("Messages") or []
    if not msgs:
        return None
    return msgs[0]


def delete_message(sqs, queue_url: str, receipt_handle: str) -> None:
    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)


# -----------------------------
# Worker process
# -----------------------------

def step_worker_loop(
    stop_event: Event,
    aws_profile: str,
    aws_region: str,
    run_id_filter: Optional[str],
    step_index: int,
    steps: int,
    bucket: str,
    serializer: str,
    queue_url: str,
    next_queue_url: Optional[str],
    control_queue_url: str,
    out_dir: str,
):
    """
    Worker loop for a single choreography step. Long-polls its queue, processes messages,
    uploads output to S3, enqueues next step or a final 'done' message.
    """
    s3, sqs = make_boto3_clients(profile=aws_profile, region=aws_region or None)

    while not stop_event.is_set():
        msg = receive_one_message(sqs, queue_url, wait_seconds=20)
        if msg is None:
            continue  # poll again

        try:
            body = json.loads(msg["Body"])
            msg_run_id = body.get("run_id", "")
            if run_id_filter and msg_run_id != run_id_filter:
                # Not our run; ignore and requeue by not deleting.
                # Sleep briefly to avoid tight spin on foreign messages.
                time.sleep(1.0)
                continue

            input_uri = body["input_s3_uri"]
            base_prefix = body["base_prefix"]
            metrics_path = os.path.join(out_dir, f"metrics-{msg_run_id}.jsonl")
            # Log dequeue for this run
            try:
                msg_id = msg.get("MessageId", "")
            except Exception:
                msg_id = ""
            print(f"[dequeue] run_id={msg_run_id} step={body.get('step_index')} queue={queue_url} message_id={msg_id} input={input_uri}")
            # Parse key from URI
            if not input_uri.startswith("s3://"):
                raise ValueError(f"Invalid input_s3_uri: {input_uri}")
            _, rest = input_uri.split("s3://", 1)
            bucket_name, key = rest.split("/", 1)

            step_start = time.perf_counter()
            # Download previous
            body_bytes, metadata, download_s = s3_get(s3, bucket_name, key)
            current_serializer = (metadata.get("serializer") or serializer).lower()

            # Deserialize
            payload_bytes, deserialize_s = deserialize_payload(body_bytes, current_serializer)

            # Transform (noop)
            transformed_bytes, transform_s = noop_transform(payload_bytes)

            # Serialize
            out_body, out_ctype, serialize_s = serialize_payload(transformed_bytes, current_serializer)

            # Upload
            out_key = f"{base_prefix.rstrip('/')}/step-{step_index:03d}.{serializer_ext(current_serializer)}"
            out_uri = build_s3_uri(bucket, out_key)
            upload_s = s3_put(
                s3,
                bucket,
                out_key,
                out_body,
                out_ctype,
                metadata={
                    "serializer": current_serializer,
                    "run_id": msg_run_id,
                    "step": str(step_index),
                },
            )
            step_wall_s = time.perf_counter() - step_start

            # Metrics
            write_metrics_line(
                msg_run_id,
                metrics_path,
                {
                    "phase": "step",
                    "step_index": step_index,
                    "input_s3_uri": input_uri,
                    "output_s3_uri": out_uri,
                    "bytes_downloaded": len(body_bytes),
                    "bytes_uploaded": len(out_body),
                    "download_s": round(download_s, 6),
                    "deserialize_s": round(deserialize_s, 6),
                    "transform_s": round(transform_s, 6),
                    "serialize_s": round(serialize_s, 6),
                    "upload_s": round(upload_s, 6),
                    "step_wall_s": round(step_wall_s, 6),
                    "serializer": current_serializer,
                    "sqs_queue": queue_url,
                },
            )

            # Enqueue next or signal done
            if step_index < steps:
                send_json_message(
                    sqs,
                    next_queue_url,
                    {
                        "run_id": msg_run_id,
                        "base_prefix": base_prefix,
                        "input_s3_uri": out_uri,
                        "step_index": step_index + 1,
                        "total_steps": steps,
                        "serializer": current_serializer,
                    },
                )
            else:
                # Signal completion on control queue
                send_json_message(
                    sqs,
                    control_queue_url,
                    {
                        "run_id": msg_run_id,
                        "done": True,
                        "final_s3_uri": out_uri,
                        "total_steps": steps,
                    },
                )
        except Exception as e:
            # Send failure and exit loop
            try:
                send_json_message(
                    sqs,
                    control_queue_url,
                    {
                        "run_id": msg_run_id,
                        "done": False,
                        "error": str(e),
                        "step_index": step_index,
                    },
                )
            except Exception:
                pass
            # Ensure we delete the message to avoid poison loop if it was ours
            try:
                delete_message(sqs, queue_url, msg["ReceiptHandle"])
            except Exception:
                pass
            break
        finally:
            # Delete processed message if it was ours
            try:
                delete_message(sqs, queue_url, msg["ReceiptHandle"])
            except Exception:
                pass

    # Exit loop on stop_event or error
    return


# -----------------------------
# Orchestration (choreography driver)
# -----------------------------

def seed_initial_payload(s3, bucket: str, base_prefix: str, payload_mb: float, serializer: str, run_id: str, metrics_path: str) -> Tuple[str, List[str]]:
    """
    Seeds the initial payload (step 0) to S3 and writes metrics. Returns (seed_uri, produced_keys).
    """
    payload_size = int(round(payload_mb * 1024 * 1024))
    seed_bytes = make_compressible_bytes(payload_size, incompressible_fraction=0.25)
    seed_key = f"{base_prefix.rstrip('/')}/step-000.{serializer_ext(serializer)}"
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
    return seed_uri, [seed_key]


def ensure_step_queues(sqs, queue_prefix: str, run_id: Optional[str], steps: int) -> Tuple[List[str], str]:
    """
    Ensure per-step queues and a control queue; return (step_queue_urls, control_queue_url).

    If run_id is provided, queues are suffixed per-run. If run_id is None or empty,
    persistent shared queues are used (no run_id in the name).
    """
    # Names must be <= 80 chars; keep components short.
    step_urls: List[str] = []
    for i in range(1, steps + 1):
        if run_id:
            qn = f"{queue_prefix}-{run_id}-s{i:03d}"
        else:
            qn = f"{queue_prefix}-s{i:03d}"
        step_urls.append(ensure_queue(sqs, qn))
    if run_id:
        control_name = f"{queue_prefix}-{run_id}-control"
    else:
        control_name = f"{queue_prefix}-control"
    control_url = ensure_queue(sqs, control_name, visibility_timeout=60, receive_wait=20)
    return step_urls, control_url


def cleanup_queues(sqs, urls: List[str]) -> None:
    for u in urls:
        delete_queue(sqs, u)


def wait_for_done(sqs, control_queue_url: str, run_id: str, timeout_s: int = 3600) -> Dict:
    """
    Blocks waiting for a 'done' control message for the given run_id.
    Returns the parsed control message dict.
    """
    t_end = time.time() + timeout_s
    while time.time() < t_end:
        msg = receive_one_message(sqs, control_queue_url, wait_seconds=20)
        if msg is None:
            continue
        try:
            body = json.loads(msg["Body"])
            if body.get("run_id") == run_id:
                # Log dequeue of control message
                msg_id = msg.get("MessageId", "")
                print(f"[dequeue] control run_id={run_id} queue={control_queue_url} message_id={msg_id} done={body.get('done')}")
                delete_message(sqs, control_queue_url, msg["ReceiptHandle"])
                return body
            # Not our run; ignore (do not delete)
        except Exception:
            # Malformed; delete to avoid poison
            delete_message(sqs, control_queue_url, msg["ReceiptHandle"])
    raise TimeoutError("Timed out waiting for completion message")




def build_arg_parser(env_cfg: Dict[str, str]) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="S3 + SQS Choreography Benchmark (multi-process, long-poll workers)")
    p.add_argument("--bucket", default=env_cfg["ORCH_BUCKET"], help="S3 bucket for payloads (required if not in env)")
    p.add_argument("--prefix", default=env_cfg["ORCH_PREFIX"], help="S3 prefix for this run (default: orchestration-bench/runs)")
    p.add_argument("--queue-prefix", default=env_cfg["ORCH_QUEUE_PREFIX"], help="SQS queue name prefix (default: orchestration-bench-queue)")
    p.add_argument("--steps", type=int, default=int(env_cfg["STEPS"]), help="Number of steps in the chain (default: 5)")
    p.add_argument("--payload-mb", type=float, default=float(env_cfg["PAYLOAD_MB"]), help="Payload size in MB (float, default: 50)")
    p.add_argument("--serializer", choices=["json", "json-gz", "raw-gz", "pickle"], default=env_cfg["SERIALIZER"], help="Serialization format (default: json)")
    p.add_argument("--cleanup", action="store_true" if parse_bool(env_cfg["CLEANUP"]) else "store_false", help="Delete S3 objects after run")
    p.add_argument("--cleanup-queues", action="store_true" if parse_bool(env_cfg["CLEANUP_QUEUES"]) else "store_false", help="Delete ephemeral SQS queues after run")
    p.add_argument("--run-id", default="", help="Run identifier (default: auto)")
    p.add_argument("--aws-profile", default=env_cfg["AWS_PROFILE"], help="AWS profile name (optional)")
    p.add_argument("--aws-region", default=env_cfg["AWS_REGION"], help="AWS region (optional; SDK default if blank)")
    p.add_argument("--repeats", type=int, default=1, help="Repeat the run this many times to compute summary stats (default: 1)")
    return p


def main(argv=None) -> int:
    env_cfg = load_config_from_env()
    parser = build_arg_parser(env_cfg)
    args = parser.parse_args(argv)

    if not args.bucket:
        print("ERROR: --bucket (or ORCH_BUCKET) is required.", file=sys.stderr)
        return 2

    try:
        s3, sqs = make_boto3_clients(profile=args.aws_profile, region=args.aws_region or None)
    except (BotoCoreError, ClientError) as e:
        print(f"ERROR: failed to create AWS clients: {e}", file=sys.stderr)
        return 3

    persistent_workers = int(args.repeats) > 1
    out_dir = ensure_out_dir()
    # If using persistent workers, create shared queues and spawn workers once
    if persistent_workers:
        step_queue_urls, control_queue_url = ensure_step_queues(sqs, args.queue_prefix, None, args.steps)
        all_queue_urls = step_queue_urls + [control_queue_url]
        stop_event = Event()
        procs: List[Process] = []
        for i, qurl in enumerate(step_queue_urls, start=1):
            next_qurl = step_queue_urls[i] if i < args.steps else None  # i is 1-based
            p = Process(
                target=step_worker_loop,
                args=(
                    stop_event,
                    args.aws_profile,
                    args.aws_region,
                    None,  # run_id_filter=None to accept all runs
                    i,
                    args.steps,
                    args.bucket,
                    args.serializer,
                    qurl,
                    next_qurl,
                    control_queue_url,
                    out_dir,
                ),
                daemon=True,
            )
            p.start()
            procs.append(p)
    else:
        stop_event = None
        procs = []
        step_queue_urls = []
        control_queue_url = ""
        all_queue_urls = []

    walls: List[float] = []
    for rep in range(int(args.repeats)):
        run_id = args.run_id or (datetime.utcnow().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8])
        if int(args.repeats) > 1:
            run_id = f"{run_id}-r{rep+1:02d}"

        base_prefix = f"{args.prefix.rstrip('/')}/{run_id}"
        metrics_path = os.path.join(out_dir, f"metrics-{run_id}.jsonl")

        # Ensure queues and spawn workers if not using persistent workers
        if not persistent_workers:
            step_queue_urls, control_queue_url = ensure_step_queues(sqs, args.queue_prefix, run_id, args.steps)
            all_queue_urls = step_queue_urls + [control_queue_url]

            stop_event = Event()
            procs = []
            for i, qurl in enumerate(step_queue_urls, start=1):
                next_qurl = step_queue_urls[i] if i < args.steps else None  # i is 1-based; step_queue_urls index 0-based
                p = Process(
                    target=step_worker_loop,
                    args=(
                        stop_event,
                        args.aws_profile,
                        args.aws_region,
                        run_id,  # run_id_filter
                        i,
                        args.steps,
                        args.bucket,
                        args.serializer,
                        qurl,
                        next_qurl,
                        control_queue_url,
                        out_dir,
                    ),
                    daemon=True,
                )
                p.start()
                procs.append(p)

        t0 = time.perf_counter()
        produced_keys: List[str] = []
        try:
            # Seed initial payload
            seed_uri, initial_keys = seed_initial_payload(s3, args.bucket, base_prefix, args.payload_mb, args.serializer, run_id, metrics_path)
            produced_keys.extend(initial_keys)

            # Kick off step 1
            if args.steps >= 1:
                send_json_message(
                    sqs,
                    step_queue_urls[0],
                    {
                        "run_id": run_id,
                        "base_prefix": base_prefix,
                        "input_s3_uri": seed_uri,
                        "step_index": 1,
                        "total_steps": args.steps,
                        "serializer": args.serializer,
                    },
                )

            # Wait for completion control message
            ctrl = wait_for_done(sqs, control_queue_url, run_id, timeout_s=3600)
            if not ctrl.get("done", False):
                raise RuntimeError(f"Choreography reported failure: {ctrl}")
            final_uri = ctrl.get("final_s3_uri")
            print(f"Run complete for {run_id}. Final object: {final_uri}")
        except Exception as e:
            print(f"ERROR during choreography: {e}", file=sys.stderr)
            # fallthrough to cleanup
        finally:
            wall = time.perf_counter() - t0
            walls.append(wall)
            print(f"End-to-end wall time: {wall:.3f}s (run_id={run_id})")

            # Stop workers and cleanup queues only if not persistent
            if not persistent_workers:
                stop_event.set()
                for p in procs:
                    p.join(timeout=5)
                    if p.is_alive():
                        p.terminate()

                if bool(args.cleanup_queues):
                    cleanup_queues(sqs, all_queue_urls)

            # Cleanup S3 objects (we know the naming scheme: step-000..step-N)
            if bool(args.cleanup):
                ext = serializer_ext(args.serializer)
                keys = [f"{base_prefix.rstrip('/')}/step-{i:03d}.{ext}" for i in range(0, args.steps + 1)]
                try:
                    s3_delete_objects(s3, args.bucket, keys)
                    print(f"Cleanup complete. Deleted {len(keys)} objects from s3://{args.bucket}/{base_prefix.rstrip('/')}/")
                except Exception as e:
                    print(f"WARNING: failed to cleanup S3 objects: {e}", file=sys.stderr)

    # If persistent workers were used, stop and (optionally) cleanup queues now
    if persistent_workers:
        stop_event.set()
        for p in procs:
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()
        if bool(args.cleanup_queues):
            cleanup_queues(sqs, all_queue_urls)

    # Optional summary for repeats
    if walls:
        import statistics
        min_s = min(walls)
        max_s = max(walls)
        avg_s = sum(walls) / len(walls)
        stdev_s = statistics.stdev(walls) if len(walls) >= 2 else 0.0
        median_s = statistics.median(walls)

        print(
            f"Summary over {len(walls)} runs: "
            f"min={min_s:.3f}s max={max_s:.3f}s avg={avg_s:.3f}s stdev={stdev_s:.3f}s median={median_s:.3f}s"
        )

        # Use the last run_id from the last loop for filename uniqueness
        summary_run_id = run_id
        out_dir = ensure_out_dir()
        summary_path = os.path.join(out_dir, f"summary-{summary_run_id}.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "base_run_id": summary_run_id,
                    "repeats": len(walls),
                    "times_s": [round(x, 6) for x in walls],
                    "min_s": round(min_s, 6),
                    "max_s": round(max_s, 6),
                    "avg_s": round(avg_s, 6),
                    "stdev_s": round(stdev_s, 6),
                    "median_s": round(median_s, 6),
                },
                f,
                separators=(",", ":"),
            )
        print(f"Wrote summary to {summary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
