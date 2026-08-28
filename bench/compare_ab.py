#!/usr/bin/env python3
"""
Compare two run_bench.py JSON results and print a markdown A/B table.

Both runs must come from the same harness against the same mock; only the
duckdb binary differs. See bench/FINDINGS.md for how the pair is produced.

    python3 bench/compare_ab.py before.json after.json [--title "..."]
"""

import json
import sys


def load(path):
    with open(path) as handle:
        return {row["label"]: row for row in json.load(handle)}


def ratio(before, after):
    """How much smaller/faster `after` is. '-' when there is nothing to compare."""
    if before is None or after is None:
        return "-"
    if after == 0:
        return "0" if before == 0 else "inf"
    if before == 0:
        return "-"
    factor = before / after
    if 0.95 <= factor <= 1.05:
        return "="
    return f"{factor:.2f}x" if factor >= 1 else f"{1 / factor:.2f}x slower"


def cell(row, key, fmt="{}"):
    if row is None or "error" in row or key not in row:
        return "err" if row is not None and "error" in row else "-"
    return fmt.format(row[key])


def main():
    argv = sys.argv[1:]
    title = "A/B"
    if "--title" in argv:
        i = argv.index("--title")
        title = argv[i + 1]
        del argv[i:i + 2]
    if len(argv) != 2:
        sys.exit(__doc__)

    before, after = load(argv[0]), load(argv[1])
    order = list(before) + [label for label in after if label not in before]

    print(f"### {title}\n")
    print("| scenario | median s (before → after) | requests | documents | MiB on the wire |")
    print("|---|---|---|---|---|")

    group = None
    for label in order:
        b, a = before.get(label), after.get(label)
        row_group = (b or a).get("group")
        if row_group != group:
            group = row_group
            print(f"| **{group}** | | | | |")

        def pair(key, fmt="{}"):
            left, right = cell(b, key, fmt), cell(a, key, fmt)
            if left == right:
                return left
            change = ""
            if b and a and "error" not in b and "error" not in a and key in b and key in a:
                # Lower is better for every column here.
                change = f" ({ratio(b[key], a[key])})"
            return f"{left} → {right}{change}"

        print(f"| {label} | {pair('median_s', '{:.3f}')} | {pair('requests')} | "
              f"{pair('docs_served')} | {pair('mib_out', '{:.2f}')} |")


if __name__ == "__main__":
    main()
