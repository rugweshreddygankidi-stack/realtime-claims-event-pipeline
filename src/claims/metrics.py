"""Summarise the streaming job's progress log (metrics/progress.jsonl)."""
from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path


def summarize(lines: list[str]) -> dict:
    rows = [json.loads(line) for line in lines if line.strip()]
    busy = [r for r in rows if r.get("num_input_rows", 0) > 0]
    if not busy:
        return {"batches": len(rows), "busy_batches": 0}
    rates = [r["processed_rows_per_sec"] for r in busy if r.get("processed_rows_per_sec")]
    return {
        "batches": len(rows),
        "busy_batches": len(busy),
        "total_rows": sum(r["num_input_rows"] for r in busy),
        "processed_rows_per_sec_median": round(statistics.median(rates), 1) if rates else None,
        "processed_rows_per_sec_max": round(max(rates), 1) if rates else None,
        "processed_rows_per_sec_p90": round(statistics.quantiles(rates, n=10)[-1], 1) if len(rates) >= 2 else None,
    }


def main() -> None:
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "metrics/progress.jsonl")
    print(json.dumps(summarize(path.read_text().splitlines()), indent=2))


if __name__ == "__main__":
    main()
