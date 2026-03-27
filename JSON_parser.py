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
    "Parallel Aware",
    "Plans",
}

def extract_node(raw_node: dict[str, Any]) -> dict[str, Any]:
    """
    Recursively extract a single plan node, keeping only KEEP_KEYS.

    Params:
    - raw_node: A single node dict from the parsed JSON 

    Returns:
    - dict with the same structure but only the fields in KEEP_KEYS.
    """
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


SAMPLE_INPUT = json.dumps([
    {
        "Plan": {
            "Node Type": "Hash Join",
            "Parallel Aware": False,
            "Join Type": "Inner",
            "Startup Cost": 35.52,
            "Total Cost": 120.50,
            "Plan Rows": 150,
            "Plan Width": 64,
            "Actual Startup Time": 1.234,
            "Actual Total Time": 5.678,
            "Actual Rows": 142,
            "Actual Loops": 1,
            "Hash Cond": "(C.c_custkey = O.o_custkey)",
            "Plans": [
                {
                    "Node Type": "Seq Scan",
                    "Parallel Aware": False,
                    "Relation Name": "customer",
                    "Alias": "C",
                    "Startup Cost": 0.00,
                    "Total Cost": 50.00,
                    "Plan Rows": 1500,
                    "Plan Width": 32,
                    "Actual Startup Time": 0.012,
                    "Actual Total Time": 0.456,
                    "Actual Rows": 1500,
                    "Actual Loops": 1
                },
                {
                    "Node Type": "Hash",
                    "Parallel Aware": False,
                    "Startup Cost": 25.00,
                    "Total Cost": 25.00,
                    "Plan Rows": 800,
                    "Plan Width": 32,
                    "Actual Startup Time": 0.800,
                    "Actual Total Time": 1.200,
                    "Actual Rows": 800,
                    "Actual Loops": 1,
                    "Plans": [
                        {
                            "Node Type": "Seq Scan",
                            "Parallel Aware": False,
                            "Relation Name": "orders",
                            "Alias": "O",
                            "Startup Cost": 0.00,
                            "Total Cost": 60.00,
                            "Plan Rows": 3000,
                            "Plan Width": 16,
                            "Actual Startup Time": 0.010,
                            "Actual Total Time": 0.900,
                            "Actual Rows": 3000,
                            "Actual Loops": 1
                        }
                    ]
                }
            ]
        },
        "Planning Time": 2.345,
        "Execution Time": 6.789
    }
], indent=2)


def main() -> None:
    if len(sys.argv) > 1:
        path = sys.argv[1]
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
        source = path
    else:
        text = SAMPLE_INPUT
        source = "built-in sample"

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
