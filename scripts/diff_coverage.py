#!/usr/bin/env python3
"""
Report line coverage restricted to the lines this branch adds or changes.

Reads .gcov files (several per source, from different builds) and merges their
execution counts, then intersects them with the added/modified lines `git diff`
reports for the same sources. Lines gcov marks as non-executable -- comments,
declarations, braces -- are excluded, because they can never be covered and
would silently deflate the number.

Driven by scripts/run_coverage.sh, which is where the .gcov files come from.
"""

import collections
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GCOV_DIR = os.environ.get("GCOV_DIR", os.path.join(ROOT, "build", "coverage"))
BASE = os.environ.get("COVERAGE_BASE", "origin/main")
THRESHOLD = float(os.environ.get("COVERAGE_MIN", "95"))

# Only shipped source is scored; tests and benchmarks are not the subject.
SCORED_PREFIXES = ("src/",)
SCORED_SUFFIXES = (".cpp", ".hpp")


def run_git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout


def changed_lines():
    """{path: {line numbers added or modified since the base}}"""
    merge_base = subprocess.run(
        ["git", "merge-base", BASE, "HEAD"], cwd=ROOT, capture_output=True, text=True,
    ).stdout.strip()
    diff_range = f"{merge_base}..HEAD" if merge_base else BASE

    # Committed changes plus the working tree: coverage is usually checked
    # before the last commit is made.
    diff = run_git("diff", "-U0", diff_range, "--", "src/")
    diff += run_git("diff", "-U0", "HEAD", "--", "src/")
    untracked = run_git("ls-files", "--others", "--exclude-standard", "src/").split()

    changed = collections.defaultdict(set)
    path = None
    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            path = line[6:]
        elif line.startswith("@@") and path:
            match = re.match(r"@@ -\S+ \+(\d+)(?:,(\d+))? @@", line)
            if match:
                start = int(match.group(1))
                count = int(match.group(2)) if match.group(2) is not None else 1
                changed[path].update(range(start, start + count))

    for path in untracked:
        if path.endswith(SCORED_SUFFIXES):
            with open(os.path.join(ROOT, path)) as handle:
                changed[path].update(range(1, sum(1 for _ in handle) + 1))

    return {
        path: lines
        for path, lines in changed.items()
        if path.startswith(SCORED_PREFIXES) and path.endswith(SCORED_SUFFIXES) and lines
    }


def gcov_counts():
    """{source path: {line: executed at least once}}, merged over every .gcov."""
    merged = collections.defaultdict(dict)
    for name in sorted(os.listdir(GCOV_DIR)):
        if not name.endswith(".gcov"):
            continue
        source = None
        with open(os.path.join(GCOV_DIR, name), errors="replace") as handle:
            for raw in handle:
                parts = raw.split(":", 2)
                if len(parts) < 3:
                    continue
                count, lineno = parts[0].strip(), parts[1].strip()
                if lineno == "0":
                    if "Source:" in raw:
                        source = raw.split("Source:", 1)[1].strip()
                    continue
                if source is None or count == "-":
                    # "-" marks a line with no code to execute.
                    continue
                executed = not count.startswith("#") and not count.startswith("=")
                number = int(lineno)
                merged[source][number] = merged[source].get(number, False) or executed
    return merged


def main():
    changed = changed_lines()
    if not changed:
        print("no changed source lines to score")
        return 0

    counts = gcov_counts()
    total_covered = total_relevant = 0
    rows, unmeasured = [], []

    for path in sorted(changed):
        absolute = os.path.join(ROOT, path)
        lines = counts.get(absolute)
        relevant = sorted(changed[path] & set(lines)) if lines else []
        if not relevant:
            # Headers are inlined into their includers rather than measured on
            # their own, so there is nothing here to score.
            unmeasured.append(path)
            continue
        covered = [line for line in relevant if lines[line]]
        missed = [line for line in relevant if not lines[line]]
        total_covered += len(covered)
        total_relevant += len(relevant)
        rows.append((path, len(covered), len(relevant), missed))

    print(f"changed-line coverage vs {BASE}\n")
    for path, covered, relevant, missed in rows:
        print(f"  {path:<45} {100.0 * covered / relevant:6.2f}%  ({covered}/{relevant})")
        if missed:
            shown = ", ".join(str(line) for line in missed[:12])
            more = "" if len(missed) <= 12 else f", +{len(missed) - 12} more"
            print(f"      uncovered lines: {shown}{more}")

    if unmeasured:
        print("\n  no executable lines measured (headers, declarations):")
        for path in unmeasured:
            print(f"    {path}")

    if not total_relevant:
        print("\nno executable changed lines to score")
        return 0

    overall = 100.0 * total_covered / total_relevant
    print(f"\n  {'TOTAL':<45} {overall:6.2f}%  ({total_covered}/{total_relevant})")

    if overall < THRESHOLD:
        print(f"\ncoverage check FAILED (threshold {THRESHOLD:g}%)", file=sys.stderr)
        return 1
    print(f"coverage check passed (threshold {THRESHOLD:g}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
