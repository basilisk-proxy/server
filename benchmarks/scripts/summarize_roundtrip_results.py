#!/usr/bin/env python3
"""Summarize k6 round-trip benchmark JSON outputs into a compact comparison table."""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from dataclasses import dataclass
from typing import Any


@dataclass
class BenchmarkRow:
    name: str
    req_rate: float | None
    req_total: float | None
    check_rate: float | None
    latency_avg_ms: float | None
    latency_p50_ms: float | None
    latency_p95_ms: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "req_rate": self.req_rate,
            "req_total": self.req_total,
            "check_rate": self.check_rate,
            "latency_avg_ms": self.latency_avg_ms,
            "latency_p50_ms": self.latency_p50_ms,
            "latency_p95_ms": self.latency_p95_ms,
        }


def _as_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _metric_values(summary: dict[str, Any], metric_name: str) -> dict[str, Any]:
    metrics = summary.get("metrics", {})
    metric = metrics.get(metric_name, {})
    # k6 --summary-export writes metric data flat (no "values" sub-key).
    if isinstance(metric, dict):
        return metric
    return {}


def _extract_row(path: str) -> BenchmarkRow:
    with open(path, "r", encoding="utf-8") as fp:
        summary = json.load(fp)

    filename = os.path.basename(path)
    name = filename.removesuffix("-roundtrip-summary.json").removesuffix(".json")

    reqs = _metric_values(summary, "http_reqs")
    checks = _metric_values(summary, "checks")
    duration = _metric_values(summary, "http_req_duration")

    return BenchmarkRow(
        name=name,
        req_rate=_as_float(reqs.get("rate")),
        req_total=_as_float(reqs.get("count")),
        check_rate=_as_float(checks.get("rate") if checks.get("rate") is not None else checks.get("value")),
        latency_avg_ms=_as_float(duration.get("avg")),
        latency_p50_ms=_as_float(duration.get("med")),
        latency_p95_ms=_as_float(duration.get("p(95)")),
    )


def _fmt_num(value: float | None, precision: int = 2) -> str:
    if value is None:
        return "-"
    return f"{value:.{precision}f}"


def _render_equal_width_table(rows: list[list[str]]) -> str:
    if not rows:
        return ""

    col_count = len(rows[0])
    global_width = max(len(cell) for row in rows for cell in row)
    separator = "+" + "+".join(["-" * (global_width + 2)] * col_count) + "+"

    lines = [separator]
    for idx, row in enumerate(rows):
        padded = [f" {cell.ljust(global_width)} " for cell in row]
        lines.append("|" + "|".join(padded) + "|")
        if idx == 0:
            lines.append(separator)
    lines.append(separator)
    return "\n".join(lines)


def _render_gfm_table(rows: list[list[str]]) -> str:
    if not rows:
        return ""

    header = rows[0]
    lines = [
        "| " + " | ".join(header) + " |",
        "|" + "|".join(["-" * (len(h) + 2) for h in header]) + "|",
    ]

    for row in rows[1:]:
        lines.append("| " + " | ".join(row) + " |")

    return "\n".join(lines)


def _render_markdown(rows: list[BenchmarkRow], baseline_name: str, flavor: str = "gfm") -> str:
    headers = [
        "Traffic path",
        "Req/s",
        "Total req",
        "Check pass %",
        "Avg (ms)",
        "p50 (ms)",
        "p95 (ms)",
    ]

    table_rows = [headers]

    for row in rows:
        check_rate_percent = row.check_rate * 100.0 if row.check_rate is not None else None
        table_rows.append(
            [
                row.name,
                _fmt_num(row.req_rate),
                _fmt_num(row.req_total, 0),
                _fmt_num(check_rate_percent),
                _fmt_num(row.latency_avg_ms),
                _fmt_num(row.latency_p50_ms),
                _fmt_num(row.latency_p95_ms),
            ]
        )

    if flavor == "text":
        lines = ["```text", _render_equal_width_table(table_rows), "```"]
    else:
        lines = [_render_gfm_table(table_rows)]

    if rows:
        lines.append("")
        baseline = _find_baseline(rows, baseline_name)
        lines.append(f"Baseline target: `{baseline.name}`")
        for row in rows:
            if row.name == baseline.name:
                continue
            if baseline.latency_p95_ms and row.latency_p95_ms:
                delta = ((row.latency_p95_ms - baseline.latency_p95_ms) / baseline.latency_p95_ms) * 100.0
                lines.append(f"- `{row.name}` p95 delta vs baseline: {delta:+.2f}%")

    return "\n".join(lines)


def _find_baseline(rows: list[BenchmarkRow], baseline_name: str) -> BenchmarkRow:
    return next((r for r in rows if r.name == baseline_name), rows[0])


def _p95_deltas(rows: list[BenchmarkRow], baseline: BenchmarkRow) -> list[dict[str, Any]]:
    deltas: list[dict[str, Any]] = []
    for row in rows:
        if row.name == baseline.name:
            continue
        if baseline.latency_p95_ms is None or row.latency_p95_ms is None:
            deltas.append(
                {
                    "target": row.name,
                    "baseline": baseline.name,
                    "delta_percent": None,
                }
            )
            continue
        delta = ((row.latency_p95_ms - baseline.latency_p95_ms) / baseline.latency_p95_ms) * 100.0
        deltas.append(
            {
                "target": row.name,
                "baseline": baseline.name,
                "delta_percent": delta,
            }
        )
    return deltas


def _build_json_report(rows: list[BenchmarkRow], baseline_name: str) -> dict[str, Any]:
    baseline = _find_baseline(rows, baseline_name)
    return {
        "baseline": baseline.name,
        "rows": [r.to_dict() for r in rows],
        "p95_deltas": _p95_deltas(rows, baseline),
    }


def _validate_p95_threshold(
    report: dict[str, Any],
    max_p95_regression_percent: float,
) -> tuple[bool, list[str]]:
    violations: list[str] = []
    for delta in report.get("p95_deltas", []):
        value = delta.get("delta_percent")
        target = delta.get("target", "unknown")
        baseline = delta.get("baseline", "unknown")
        if value is None:
            continue
        if float(value) > max_p95_regression_percent:
            violations.append(
                f"{target} p95 regression {float(value):.2f}% exceeds limit {max_p95_regression_percent:.2f}% (baseline: {baseline})"
            )
    return (len(violations) == 0, violations)


def _collect_files(results_dir: str, pattern: str) -> list[str]:
    full_pattern = os.path.join(results_dir, pattern)
    files = sorted(glob.glob(full_pattern))
    return [path for path in files if os.path.isfile(path)]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Summarize k6 round-trip benchmark result JSON files."
    )
    parser.add_argument(
        "--results-dir",
        default=os.path.join("benchmarks", "results"),
        help="Directory containing k6 summary JSON files.",
    )
    parser.add_argument(
        "--pattern",
        default="*-roundtrip-summary.json",
        help="Glob pattern used to select summary files.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional output path for the generated report.",
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "markdown-text", "json"),
        default="markdown",
        help="Output format for the report.",
    )
    parser.add_argument(
        "--baseline",
        default="basilisk-direct",
        help="Baseline target name used for p95 delta comparisons.",
    )
    parser.add_argument(
        "--max-p95-regression-percent",
        type=float,
        default=None,
        help="Optional CI gate. Fails if any p95 delta vs baseline exceeds this percentage.",
    )

    args = parser.parse_args()
    files = _collect_files(args.results_dir, args.pattern)

    if not files:
        print(
            f"No benchmark summary files found in '{args.results_dir}' matching '{args.pattern}'.",
            file=sys.stderr,
        )
        return 1

    rows = [_extract_row(path) for path in files]
    baseline = _find_baseline(rows, args.baseline)
    markdown = _render_markdown(
        rows,
        baseline.name,
        flavor="text" if args.format == "markdown-text" else "gfm",
    )
    report = _build_json_report(rows, baseline.name)

    output_content = markdown
    if args.format == "json":
        output_content = json.dumps(report, indent=2, sort_keys=True)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fp:
            fp.write(output_content)
            fp.write("\n")
        print(f"Wrote benchmark summary report to {args.output}")
    else:
        print(output_content)

    if args.max_p95_regression_percent is not None:
        ok, violations = _validate_p95_threshold(report, args.max_p95_regression_percent)
        if not ok:
            for line in violations:
                print(f"ERROR: {line}", file=sys.stderr)
            return 2
        print(
            f"p95 regression check passed (limit: {args.max_p95_regression_percent:.2f}%)."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
