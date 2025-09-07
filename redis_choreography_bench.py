#!/usr/bin/env python3
"""
Choreography Service Tester (Redis, multi-process workers)

- Loads configuration from .env (via python-dotenv) and CLI flags.
- Spawns separate processes, one per step, each blocking on its own Redis list (queue).
- Seeds an initial payload to Redis, then enqueues the first step message.
- Each step process:
  - receives message with prior step's Redis key,
  - loads and deserializes payload,
  - performs a transform (noop by default),
  - serializes and stores new payload to Redis,
  - enqueues the next step's message (or a final "done" control message),
  - deletes the prior step's Redis key after producing the next one.
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

from dotenv import load_dotenv

try:
    import redis  # redis-py
except Exception as e:
    print("ERROR: redis package is required. Install with: pip install redis", file=sys.stderr)
    raise

from utils import (
    parse_bool,
    serializer_ext,
    serialize_payload,
    deserialize_payload,
    noop_transform,
    make_compressible_bytes,
    ensure_out_dir,
    write_metrics_line,
)


# -----------------------------
# Shared helpers
# -----------------------------

def load_config_from_env() -> Dict[str, str]:
    load_dotenv()  # best-effort

    cfg: Dict[str, str] = {}
    cfg["REDIS_URL"] = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    cfg["ORCH_PREFIX"] = os.getenv("ORCH_PREFIX", "orchestration-bench/runs")
    cfg["ORCH_QUEUE_PREFIX"] = os.getenv("ORCH_QUEUE_PREFIX", "orchestration-bench-queue")

    cfg["STEPS"] = os.getenv("STEPS", "5")
    cfg["PAYLOAD_MB"] = os.getenv("PAYLOAD_MB", "50")
    cfg["SERIALIZER"] = os.getenv("SERIALIZER", "json").lower()
    cfg["CLEANUP"] = os.getenv("CLEANUP", "false").lower()
    cfg["CLEANUP_QUEUES"] = os.getenv("CLEANUP_QUEUES", "true").lower()

    return cfg


def make_redis_client(url: str):
    # Use bytes API (decode_responses=False) so payloads are bytes
    return redis.Redis.from_url(url, decode_responses=False)


# -----------------------------
# Redis Queue helpers
# -----------------------------

def queue_name_for_step(queue_prefix: str, run_id: str, step_index: int) -> str:
    return f"{queue_prefix}-{run_id}-s{step_index:03d}"


def control_queue_name(queue_prefix: str, run_id: str) -> str:
    return f"{queue_prefix}-{run_id}-control"


def send_json_message(r: "redis.Redis", queue: str, obj: Dict) -> None:
    body = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    r.rpush(queue, body)
    print(f"[enqueue] run_id={obj.get('run_id')} step={obj.get('step_index')} queue={queue}")


def blpop_json_message(r: "redis.Redis", queue: str, timeout_s: int = 5) -> Optional[Dict]:
    # Returns None on timeout
    res = r.blpop(queue, timeout=timeout_s)
    if not res:
        return None
    _, body = res
    try:
        return json.loads(body.decode("utf-8"))
    except Exception:
        return None


def delete_list_keys(r: "redis.Redis", queues: List[str]) -> None:
    if queues:
        r.delete(*queues)


# -----------------------------
# Worker process
# -----------------------------

def step_worker_loop(
    stop_event: Event,
    redis_url: str,
    run_id_filter: Optional[str],
    step_index: int,
    steps: int,
    serializer_default: str,
    queue_name: str,
    next_queue_name: Optional[str],
    out_dir: str,
):
    """
    Worker loop for a single choreography step using Redis as queue and storage.
    Blocks on its queue, processes messages, stores output to Redis, enqueues next step,
    and deletes the input key after successful processing.
    """
    r = make_redis_client(redis_url)

    while not stop_event.is_set():
        # Short timeout to allow checking stop_event
        msg = blpop_json_message(r, queue_name, timeout_s=2)
        if msg is None:
            continue

        try:
            msg_run_id = msg.get("run_id", "")
            if run_id_filter and msg_run_id != run_id_filter:
                # Not our run; requeue at tail and back off briefly
                send_json_message(r, queue_name, msg)
                time.sleep(0.5)
                continue

            input_key = msg["input_key"]
            base_prefix = msg["base_prefix"]
            control_q = msg["control_queue"]
            current_serializer = (msg.get("serializer") or serializer_default).lower()

            metrics_path = os.path.join(out_dir, f"metrics-{msg_run_id}.jsonl")
            print(f"[dequeue] run_id={msg_run_id} step={msg.get('step_index')} queue={queue_name} input_key={input_key}")

            step_start = time.perf_counter()

            # Load previous payload
            t0 = time.perf_counter()
            body_bytes = r.get(input_key)
            download_s = time.perf_counter() - t0
            if body_bytes is None:
                raise FileNotFoundError(f"missing redis key: {input_key}")

            # Deserialize
            payload_bytes, deserialize_s = deserialize_payload(body_bytes, current_serializer)

            # Transform (noop)
            transformed_bytes, transform_s = noop_transform(payload_bytes)

            # Serialize
            out_body, _out_ctype, serialize_s = serialize_payload(transformed_bytes, current_serializer)

            # Store next payload
            out_key = f"{base_prefix.rstrip('/')}/step-{step_index:03d}.{serializer_ext(current_serializer)}"
            t1 = time.perf_counter()
            r.set(out_key, out_body)
            upload_s = time.perf_counter() - t1
            step_wall_s = time.perf_counter() - step_start

            # Metrics
            write_metrics_line(
                msg_run_id,
                metrics_path,
                {
                    "phase": "step",
                    "step_index": step_index,
                    "input_key": input_key,
                    "output_key": out_key,
                    "bytes_downloaded": len(body_bytes),
                    "bytes_uploaded": len(out_body),
                    "download_s": round(download_s, 6),
                    "deserialize_s": round(deserialize_s, 6),
                    "transform_s": round(transform_s, 6),
                    "serialize_s": round(serialize_s, 6),
                    "upload_s": round(upload_s, 6),
                    "step_wall_s": round(step_wall_s, 6),
                    "serializer": current_serializer,
                    "queue": queue_name,
                },
            )

            # Delete previous key now that next is produced
            try:
                r.delete(input_key)
            except Exception:
                pass

            # Enqueue next or signal done
            if step_index < steps:
                send_json_message(
                    r,
                    next_queue_name,
                    {
                        "run_id": msg_run_id,
                        "base_prefix": base_prefix,
                        "input_key": out_key,
                        "step_index": step_index + 1,
                        "total_steps": steps,
                        "serializer": current_serializer,
                        "control_queue": control_q,
                    },
                )
            else:
                # Signal completion
                send_json_message(
                    r,
                    control_q,
                    {
                        "run_id": msg_run_id,
                        "done": True,
                        "final_key": out_key,
                        "total_steps": steps,
                    },
                )

        except Exception as e:
            # Send failure and exit loop
            try:
                control_q = msg.get("control_queue") if isinstance(msg, dict) else None
                if control_q:
                    send_json_message(
                        r,
                        control_q,
                        {
                            "run_id": msg.get("run_id", ""),
                            "done": False,
                            "error": str(e),
                            "step_index": step_index,
                        },
                    )
            except Exception:
                pass
            break

    return


# -----------------------------
# Orchestration (driver)
# -----------------------------

def seed_initial_payload(r: "redis.Redis", base_prefix: str, payload_mb: float, serializer: str, run_id: str, metrics_path: str) -> Tuple[str, List[str]]:
    """
    Seeds the initial payload (step 0) to Redis and writes metrics. Returns (seed_key, produced_keys).
    """
    payload_size = int(round(payload_mb * 1024 * 1024))
    seed_bytes = make_compressible_bytes(payload_size, incompressible_fraction=0.25)
    seed_key = f"{base_prefix.rstrip('/')}/step-000.{serializer_ext(serializer)}"

    seed_body, _seed_ctype, seed_serialize_s = serialize_payload(seed_bytes, serializer)
    t0 = time.perf_counter()
    r.set(seed_key, seed_body)
    seed_upload_s = time.perf_counter() - t0

    write_metrics_line(
        run_id,
        metrics_path,
        {
            "phase": "seed",
            "step_index": 0,
            "output_key": seed_key,
            "bytes_uploaded": len(seed_body),
            "serialize_s": round(seed_serialize_s, 6),
            "upload_s": round(seed_upload_s, 6),
            "payload_mb": payload_mb,
            "serializer": serializer,
        },
    )
    print(f"[seed] stored initial payload at {seed_key} ({payload_mb} MB, serializer={serializer})")
    return seed_key, [seed_key]


def ensure_step_queues(queue_prefix: str, run_id: str, steps: int) -> Tuple[List[str], str]:
    """
    Return per-step queue names and the per-run control queue name.
    """
    step_qs: List[str] = [queue_name_for_step(queue_prefix, run_id, i) for i in range(1, steps + 1)]
    ctrl_q = control_queue_name(queue_prefix, run_id)
    return step_qs, ctrl_q


def cleanup_queues(r: "redis.Redis", queues: List[str]) -> None:
    delete_list_keys(r, queues)


def wait_for_done(r: "redis.Redis", control_q: str, run_id: str, timeout_s: int = 3600) -> Dict:
    """
    Blocks waiting for a 'done' control message (per-run control queue).
    Returns the parsed control message dict.
    """
    t_end = time.time() + timeout_s
    while time.time() < t_end:
        msg = blpop_json_message(r, control_q, timeout_s=2)
        if msg is None:
            continue
        # Per-run control queue should only contain our messages, but validate anyway.
        if msg.get("run_id") == run_id:
            print(f"[dequeue] control run_id={run_id} queue={control_q} done={msg.get('done')}")
            return msg
    raise TimeoutError("Timed out waiting for completion message")


def build_arg_parser(env_cfg: Dict[str, str]) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Redis Choreography Benchmark (multi-process, blocking list workers)")
    p.add_argument("--prefix", default=env_cfg["ORCH_PREFIX"], help="Key prefix for this run (default: orchestration-bench/runs)")
    p.add_argument("--queue-prefix", default=env_cfg["ORCH_QUEUE_PREFIX"], help="Redis queue name prefix (default: orchestration-bench-queue)")
    p.add_argument("--steps", type=int, default=int(env_cfg["STEPS"]), help="Number of steps in the chain (default: 5)")
    p.add_argument("--payload-mb", type=float, default=float(env_cfg["PAYLOAD_MB"]), help="Payload size in MB (float, default: 50)")
    p.add_argument("--serializer", choices=["json", "json-gz", "raw-gz", "pickle"], default=env_cfg["SERIALIZER"], help="Serialization format (default: json)")
    p.add_argument("--cleanup", action="store_true" if parse_bool(env_cfg["CLEANUP"]) else "store_false", help="Delete Redis keys after run")
    p.add_argument("--cleanup-queues", action="store_true" if parse_bool(env_cfg["CLEANUP_QUEUES"]) else "store_false", help="Delete Redis queues after run")
    p.add_argument("--run-id", default="", help="Run identifier (default: auto)")
    p.add_argument("--redis-url", default=env_cfg["REDIS_URL"], help="Redis URL (default: redis://localhost:6379/0)")
    p.add_argument("--repeats", type=int, default=1, help="Repeat the run this many times to compute summary stats (default: 1)")
    return p


def main(argv=None) -> int:
    env_cfg = load_config_from_env()
    parser = build_arg_parser(env_cfg)
    args = parser.parse_args(argv)

    try:
        r = make_redis_client(args.redis_url)
        # quick ping
        r.ping()
    except Exception as e:
        print(f"ERROR: failed to connect to Redis at {args.redis_url}: {e}", file=sys.stderr)
        return 3

    out_dir = ensure_out_dir()

    walls: List[float] = []
    last_run_id = None

    for rep in range(int(args.repeats)):
        run_id = args.run_id or (datetime.utcnow().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8])
        if int(args.repeats) > 1:
            run_id = f"{run_id}-r{rep+1:02d}"
        last_run_id = run_id

        base_prefix = f"{args.prefix.rstrip('/')}/{run_id}"
        metrics_path = os.path.join(out_dir, f"metrics-{run_id}.jsonl")

        # Per-run queues and workers
        step_queue_names, control_q = ensure_step_queues(args.queue_prefix, run_id, args.steps)
        all_queues = step_queue_names + [control_q]

        stop_event = Event()
        procs: List[Process] = []
        for i, qname in enumerate(step_queue_names, start=1):
            next_qname = step_queue_names[i] if i < args.steps else None  # i is 1-based
            p = Process(
                target=step_worker_loop,
                args=(
                    stop_event,
                    args.redis_url,
                    run_id,  # run_id_filter: only process this run
                    i,
                    args.steps,
                    args.serializer,
                    qname,
                    next_qname,
                    out_dir,
                ),
                daemon=True,
            )
            p.start()
            procs.append(p)

        t0 = time.perf_counter()
        produced_keys: List[str] = []
        final_key: Optional[str] = None
        try:
            # Seed initial payload
            seed_key, initial_keys = seed_initial_payload(r, base_prefix, args.payload_mb, args.serializer, run_id, metrics_path)
            produced_keys.extend(initial_keys)

            # Kick off step 1
            if args.steps >= 1:
                send_json_message(
                    r,
                    step_queue_names[0],
                    {
                        "run_id": run_id,
                        "base_prefix": base_prefix,
                        "input_key": seed_key,
                        "step_index": 1,
                        "total_steps": args.steps,
                        "serializer": args.serializer,
                        "control_queue": control_q,
                    },
                )

            # Wait for completion control message
            ctrl = wait_for_done(r, control_q, run_id, timeout_s=3600)
            if not ctrl.get("done", False):
                raise RuntimeError(f"Choreography reported failure: {ctrl}")
            final_key = ctrl.get("final_key")
            print(f"Run complete for {run_id}. Final key: {final_key}")
        except Exception as e:
            print(f"ERROR during choreography: {e}", file=sys.stderr)
            # fallthrough to cleanup
        finally:
            wall = time.perf_counter() - t0
            walls.append(wall)
            print(f"End-to-end wall time: {wall:.3f}s (run_id={run_id})")

            # Stop workers
            stop_event.set()
            for p in procs:
                p.join(timeout=5)
                if p.is_alive():
                    p.terminate()

            # Cleanup queues
            if bool(args.cleanup_queues):
                try:
                    cleanup_queues(r, all_queues)
                except Exception as e:
                    print(f"WARNING: failed to cleanup Redis queues: {e}", file=sys.stderr)

            # Cleanup Redis payload keys
            if bool(args.cleanup):
                try:
                    # Only the final key should remain; delete the entire run prefix just in case.
                    # Scan and delete keys with this base_prefix.
                    cursor = 0
                    pattern = f"{base_prefix.rstrip('/')}/*"
                    to_del: List[bytes] = []
                    while True:
                        cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
                        if keys:
                            to_del.extend(keys)
                        if cursor == 0:
                            break
                    if to_del:
                        r.delete(*to_del)
                    print(f"Cleanup complete. Deleted {len(to_del)} keys with prefix {base_prefix.rstrip('/')}/")
                except Exception as e:
                    print(f"WARNING: failed to cleanup Redis keys: {e}", file=sys.stderr)

    # Summary for repeats
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

        summary_run_id = last_run_id or "unknown"
        out_dir = ensure_out_dir()
        summary_path = os.path.join(out_dir, f"summary-{summary_run_id}.json")
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "tool": "redis_choreography_bench",
                    "base_run_id": summary_run_id,
                    "repeats": len(walls),
                    "times_s": [round(x, 6) for x in walls],
                    "min_s": round(min_s, 6),
                    "max_s": round(max_s, 6),
                    "avg_s": round(avg_s, 6),
                    "stdev_s": round(stdev_s, 6),
                    "median_s": round(median_s, 6),
                    "settings": {
                        "prefix": env_cfg["ORCH_PREFIX"],
                        "queue_prefix": env_cfg["ORCH_QUEUE_PREFIX"],
                        "computed_base_prefix": f"{env_cfg['ORCH_PREFIX'].rstrip('/')}/{summary_run_id}",
                        "steps": int(env_cfg["STEPS"]),
                        "payload_mb": float(env_cfg["PAYLOAD_MB"]),
                        "serializer": env_cfg["SERIALIZER"],
                        "cleanup": parse_bool(env_cfg["CLEANUP"]),
                        "cleanup_queues": parse_bool(env_cfg["CLEANUP_QUEUES"]),
                        "redis_url": env_cfg["REDIS_URL"],
                        "repeats": int(args.repeats),
                    },
                },
                f,
                separators=(",", ":"),
            )
        print(f"Wrote summary to {summary_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
