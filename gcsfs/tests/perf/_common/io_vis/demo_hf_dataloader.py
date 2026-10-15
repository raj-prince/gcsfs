"""Demo & CLI: Trace HuggingFace streaming dataset + multi-worker PyTorch DataLoader I/O patterns.

Can run either:
  1. Against a local synthetic Parquet/JSONL corpus (simulating fsspec/GCSFile buffered block reads)
  2. Against a real `gs://bucket/path` prefix (`--prefix gs://...`)

Generates a standalone interactive HTML dashboard showing:
  - Space-Time Offset Map (X = Time, Y = File Swimlane & Byte Offset [offset .. offset+len], Color = PID)
  - Logical reads (solid boxes) overlaid on Wire fetches (translucent dashed boxes)
  - Per-PID Activity Gantt & Synced Bandwidth Timeline
"""

from __future__ import annotations

import argparse
import os
import tempfile
import time
from pathlib import Path

# Keep HF Hub offline by default so load_dataset("parquet", ...) never hangs on external Hub calls
os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("HF_DATASETS_OFFLINE", "1")

from io_tracer import trace_io


def generate_parquet_shards(
    directory: str,
    *,
    num_shards: int = 4,
    rows_per_shard: int = 1200,
    row_group_size: int = 300,
    seq_len: int = 192,
) -> list[str]:
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    paths = []
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("tokens", pa.list_(pa.int64())),
            ("aux_payload", pa.binary()),
            ("label", pa.int64()),
        ]
    )
    for shard_idx in range(num_shards):
        rng = np.random.default_rng(100 + shard_idx)
        shard_path = os.path.join(directory, f"shard_{shard_idx:05d}.parquet")
        with pq.ParquetWriter(shard_path, schema, compression="none") as writer:
            for rg_start in range(0, rows_per_shard, row_group_size):
                n = min(row_group_size, rows_per_shard - rg_start)
                ids = np.arange(rg_start, rg_start + n, dtype=np.int64) + shard_idx * rows_per_shard
                tokens = rng.integers(0, 32000, size=(n, seq_len), dtype=np.int64)
                blobs = [rng.bytes(512) for _ in range(n)]
                labels = rng.integers(0, 2, size=n, dtype=np.int64)
                table = pa.table(
                    {
                        "id": pa.array(ids),
                        "tokens": pa.array(list(tokens)),
                        "aux_payload": pa.array(blobs, type=pa.binary()),
                        "label": pa.array(labels),
                    },
                    schema=schema,
                )
                writer.write_table(table, row_group_size=n)
        paths.append(shard_path)
    return paths


def run_hf_streaming_workload(
    data_pattern: str,
    *,
    num_workers: int = 4,
    batch_size: int = 64,
    reshard: bool = True,
    project_columns: bool = True,
    shuffled: bool = False,
    step_compute_ms: float = 3.0,
) -> dict:
    import datasets
    from torch.utils.data import DataLoader

    t0 = time.perf_counter()
    columns = ["id", "tokens", "label"] if project_columns else None
    ds = datasets.load_dataset(
        "parquet",
        data_files=data_pattern,
        split="train",
        streaming=True,
        columns=columns,
    )
    if reshard:
        ds = ds.reshard()
    if shuffled:
        ds = ds.shuffle(seed=42, buffer_size=256, max_buffer_input_shards=1)
    ds = ds.with_format("torch")

    loader = DataLoader(
        ds,
        batch_size=batch_size,
        num_workers=num_workers,
        prefetch_factor=2 if num_workers > 0 else None,
        persistent_workers=(num_workers > 0),
    )

    total_rows = 0
    batches = 0
    for batch in loader:
        total_rows += int(batch["label"].shape[0])
        batches += 1
        if step_compute_ms > 0:
            time.sleep(step_compute_ms / 1000.0)

    elapsed = time.perf_counter() - t0
    return {"total_rows": total_rows, "batches": batches, "elapsed_sec": elapsed}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run and trace HF streaming DataLoader I/O.")
    parser.add_argument(
        "--output",
        default="/home/princer_google_com/c1dev/gcsfs/hf_dataloader_io_trace.html",
        help="Output HTML file path",
    )
    parser.add_argument(
        "--prefix",
        default="",
        help="Optional existing dataset directory or gs://bucket/prefix containing shard_*.parquet",
    )
    parser.add_argument("--num-shards", type=int, default=4)
    parser.add_argument("--num-workers", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--block-size", type=int, default=512 * 1024, help="Simulated wire block size in bytes")
    parser.add_argument("--cache-type", default="readahead", choices=["readahead", "blockcache", "bytes", "none"])
    parser.add_argument("--no-reshard", action="store_true", help="Disable ds.reshard() row-group splitting")
    parser.add_argument("--all-columns", action="store_true", help="Read all columns without projection")
    parser.add_argument("--shuffled", action="store_true")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="hf_parquet_corpus_") as tmp_corpus:
        if args.prefix:
            pattern = f"{args.prefix.rstrip('/')}/shard_*.parquet"
            print(f"Using dataset pattern: {pattern}")
        else:
            shards = generate_parquet_shards(
                tmp_corpus,
                num_shards=args.num_shards,
                rows_per_shard=1200,
                row_group_size=300,
                seq_len=192,
            )
            shard_size = os.path.getsize(shards[0])
            pattern = os.path.join(tmp_corpus, "shard_*.parquet")
            print(f"Generated {len(shards)} Parquet shards ({shard_size / (1024*1024):.2f} MiB each, 4 row groups/shard)")

        title = (
            f"HF Streaming DataLoader I/O Trace "
            f"(workers={args.num_workers}, shards={args.num_shards}, "
            f"reshard={not args.no_reshard}, cache={args.cache_type}, block={args.block_size // 1024}KiB)"
        )
        with trace_io(
            output_html=args.output,
            title=title,
            local_block_size=args.block_size,
            local_cache_type=args.cache_type,
            simulated_wire_latency_ms=2.0,
        ) as events:
            stats = run_hf_streaming_workload(
                pattern,
                num_workers=args.num_workers,
                batch_size=args.batch_size,
                reshard=not args.no_reshard,
                project_columns=not args.all_columns,
                shuffled=args.shuffled,
            )

        pids = sorted({e["pid"] for e in events})
        logical_ev = [e for e in events if e["layer"] == "logical"]
        wire_ev = [e for e in events if e["layer"] == "wire"]
        l_bytes = sum(e["len"] for e in logical_ev)
        w_bytes = sum(e["len"] for e in wire_ev)
        hits = sum(1 for e in logical_ev if e.get("hit"))
        print(
            f"Workload finished in {stats['elapsed_sec']:.2f}s "
            f"({stats['total_rows']} rows across {stats['batches']} batches)"
        )
        print(
            f"Captured {len(events)} I/O events across {len(pids)} PIDs {pids}:\n"
            f"  • Logical Reads : {len(logical_ev)} ops ({l_bytes / (1024*1024):.2f} MiB, {hits}/{len(logical_ev)} cache hits)\n"
            f"  • Wire Fetches  : {len(wire_ev)} ops ({w_bytes / (1024*1024):.2f} MiB)\n"
            f"  • Read Amp      : {(w_bytes / l_bytes if l_bytes else 0):.2f}x"
        )
        print(f"Interactive HTML dashboard written to: {Path(args.output).resolve()}")


if __name__ == "__main__":
    main()
