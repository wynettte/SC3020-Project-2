from __future__ import annotations

import json
import sys
from typing import Any

KEEP_KEYS: set[str] = {
    "Node Type",
    "Relation Name",
    "Alias",
    "Index Name",
    "Index Cond",
    "Filter",
    "Hash Cond",
    "Join Type",
    "Merge Cond",
    "Recheck Cond",
    "Sort Key",
    "Total Cost",
    "Actual Total Time",
    "Actual Rows",
    "Actual Loops",
    # "Parallel Aware",
    "Plans",
}

def extract_node(raw_node: dict[str, Any]) -> dict[str, Any]:
    node: dict[str, Any] = {}

    for key in KEEP_KEYS:
        if key not in raw_node:
            continue
        if key == "Plans":
            node["Plans"] = [extract_node(child) for child in raw_node["Plans"]]
        else:
            node[key] = raw_node[key]

    return node


def parse_explain_json(text: str) -> dict[str, Any]:
    raw = json.loads(text)

    while isinstance(raw, list):
        if not raw:
            raise ValueError("EXPLAIN JSON array is empty.")
        raw = raw[0]

    if not isinstance(raw, dict):
        raise ValueError("Expected a JSON object at the top level.")
    if "Plan" not in raw:
        raise ValueError('No "Plan" key found in the EXPLAIN JSON output.')

    result: dict[str, Any] = {
        "Plan": extract_node(raw["Plan"])
    }

    for timing_key in ("Planning Time", "Execution Time"):
        if timing_key in raw:
            result[timing_key] = raw[timing_key]

    return result


def main() -> None:
    path = sys.argv[1]
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    source = path

    print(f"Parsing: {source}\n")

    try:
        parsed = parse_explain_json(text)
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    print("--- Extracted plan (JSON) ---")
    print(json.dumps(parsed, indent=2))
    print()

if __name__ == "__main__":
    main()
