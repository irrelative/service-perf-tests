# Orchestration Service Tester (S3 Latency + Serialization Benchmark)

This project benchmarks an orchestration pattern where each step:
- Reads a payload from S3 (produced by the prior step),
- Optionally performs a transform (noop by default),
- Writes a new payload to S3,
- Returns the S3 URI to be consumed by the next step.

The goal is to measure end-to-end latency across multiple steps, and break down time spent in serialization, upload/download, and deserialization. This is useful for capacity planning, format trade-offs, and validating orchestration overhead.

## High-level design

- Parent Orchestrator:
  - Generates an initial payload and writes it to S3.
  - Invokes N steps in sequence (orchestrated) passing the S3 URI from the previous step to the next.
  - Collects per-step metrics and emits a final summary.

- Step Worker:
  - Input: S3 URI to payload from the previous step.
  - Operations:
    1) Download object from S3.
    2) Deserialize payload.
    3) Transform (noop or configurable).
    4) Serialize next payload.
    5) Upload to S3 and return the new S3 URI.
  - Output: JSON metrics for timings and sizes, plus the new S3 URI.

- Metrics captured (per step and overall):
  - Bytes in/out (payload size on S3).
  - Serialize time, Upload time, Download time, Deserialize time.
  - Step wall time (start-to-return).
  - End-to-end wall time (orchestrator start to finish).
  - Optional: Checksums, retry counts, HTTP status codes from S3, request IDs.

## Serialization formats

You can compare multiple formats to understand trade-offs between size and speed:
- JSON (and optionally JSON Lines), JSON-GZ (JSON wrapper with base64-encoded gzip)
- Pickle
- Apache Arrow / Parquet
- MessagePack
- Raw bytes (pre-compressed or not)
- CSV (for simple tabular payloads)

Note: Some formats require optional dependencies. We’ll keep the default path simple (e.g., JSON or Pickle) and allow opting into richer formats.

## Configuration

Config is provided via CLI flags and/or environment variables.

- AWS:
  - AWS_REGION (defaults to your AWS SDK’s default resolution)
  - AWS_PROFILE (optional if not using instance role or env creds)
  - S3 bucket: ORCH_BUCKET
  - S3 prefix: ORCH_PREFIX (e.g., orchestration-bench/runs/${DATE}/${RUN_ID}/)
  - SQS queue prefix: ORCH_QUEUE_PREFIX (e.g., orchestration-bench-queue)

- Benchmark:
  - STEPS: number of steps in the chain (default: 5)
  - PAYLOAD_MB: approximate size of each payload (default: 50)
  - SERIALIZER: one of [json, json-gz, pickle, arrow, parquet, msgpack, raw] (default: json)
  - TRANSFORM: one of [noop, cpu, io] (default: noop)
  - CONCURRENCY: orchestration parallelism for multiple chains (future; default: 1)
  - CLEANUP: delete S3 payloads on success [true|false] (default: false)

- Output:
  - Local metrics: ./out/metrics-${RUN_ID}.jsonl
  - S3 metrics: optional upload to {bucket}/{prefix}/metrics/metrics-${RUN_ID}.jsonl

## Example workflow (planned)

- Run a single chain of 5 steps with 50 MB payloads using JSON:
  - Creates the initial object on S3.
  - Iteratively runs each step, timing S3 get/put and (de)serialization.
  - Emits a JSONL metrics file and a final summary.

- Compare serializers by running multiple runs with the same PAYLOAD_MB and STEPS:
  - Summarize average step time and end-to-end time per serializer.
  - Compare on-wire size (compressed/uncompressed) and cost implications.

## Safety and cost notes

- S3 charges for storage and requests. Large payloads and many steps can incur non-trivial costs.
- Consider using a short-lived prefix and enabling CLEANUP to reduce storage footprint.
- For sensitive data, use SSE-S3 or SSE-KMS and restrict IAM permissions tightly.

## IAM and permissions

The runner needs permission to:
- s3:PutObject, s3:GetObject, s3:DeleteObject on the chosen bucket/prefix
- s3:ListBucket on the bucket (optional but helpful for cleanup/verification)

Minimum recommended policy will be provided alongside the CLI once implemented.

## Roadmap

- v0: Local CLI orchestrator + step worker, single-threaded chain (noop transform), JSON and Pickle.
- v1: Add more serializers (Arrow, Parquet, MessagePack), optional compression.
- v2: Parallel chains to study aggregate throughput; richer metrics and CSV/HTML report.
- v3: Pluggable backends (e.g., queues, serverless steps) for broader orchestration testing.

## Getting started

- Ensure you have AWS credentials configured (env vars, profile, or role).
- Create or choose an S3 bucket for the benchmark payloads.
- Configure ORCH_BUCKET and ORCH_PREFIX.
- Run the CLI (to be added) to start a benchmark run.

Two CLIs are available now:
- s3_orch_bench.py: single-process orchestrator that performs each step inline.
- s3_sqs_choreography_bench.py: multi-process choreography using SQS queues (one queue per step, long-polling workers).

Use --help on either script for usage and options.

## Example settings (sample runs)

The table below shows example parameters captured for real runs of both benchmarks.

| tool                      | base_run_id                      | repeats | steps | payload_mb | serializer | bucket                   | prefix                   | queue_prefix               | computed_base_prefix                                        | cleanup | cleanup_queues | aws_profile | aws_region | persistent_workers |    min_s |    max_s |    avg_s |  stdev_s | median_s |
|--------------------------:|:---------------------------------|--------:|------:|-----------:|:-----------|:--------------------------|:-------------------------|:---------------------------|:-------------------------------------------------------------|:--------|:---------------|:------------|:-----------|:-------------------|---------:|---------:|---------:|---------:|---------:|
| s3_sqs_choreography_bench | 20250905-033401-47fd8bb4-r10     |      10 |     4 |        1.0 | json       | re-service-test-us-east-2 | orchestration-bench/runs | orchestration-bench-queue  | orchestration-bench/runs/20250905-033401-47fd8bb4-r10       | true    | false          | orch-bench  | us-east-1  | true               | 0.906141 | 2.258833 | 1.173785 | 0.395488 | 1.037834 |
| s3_orch_bench             | 20250905-033553-4b4bbb67         |      10 |     4 |        1.0 | json       | re-service-test-us-east-2 | orchestration-bench/runs | -                         | orchestration-bench/runs/20250905-033553-4b4bbb67           | true    | -              | orch-bench  | us-east-1  | -                  | 0.913329 | 1.365823 | 1.048879 | 0.132818 | 1.017237 |
| s3_orch_bench             | 20250905-034327-26e4b540         |      10 |     4 |        5.0 | json       | re-service-test-us-east-2 | orchestration-bench/runs | -                         | orchestration-bench/runs/20250905-034327-26e4b540           | true    | -              | orch-bench  | us-east-1  | -                  | 1.648737 | 1.869731 | 1.739951 | 0.072534 | 1.713388 |
