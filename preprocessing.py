# Handles two responsibilities:
#   1. JSON parsing  — strip a raw EXPLAIN JSON payload down to only the keys we care about.
#   2. Plan generation — connect to PostgreSQL, run EXPLAIN under different planner
#      settings to produce a QEP and a set of AQPs, then deduplicate them.

from __future__ import annotations

import json
import os
from typing import Any, Optional

import psycopg2


# ---------------------------------------------------------------------------
# Keys retained from raw EXPLAIN output
# ---------------------------------------------------------------------------

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
    "Plans",
}


# ---------------------------------------------------------------------------
# JSON parser (from JSON_parser.py)
# ---------------------------------------------------------------------------

def extract_node(raw_node: dict[str, Any]) -> dict[str, Any]:
    """Recursively keep only KEEP_KEYS from a plan node."""
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
    """
    Parse a raw EXPLAIN (FORMAT JSON) string and return a cleaned plan dict.

    Raises ValueError if the JSON is not a valid EXPLAIN payload.
    """
    raw = json.loads(text)

    # Unwrap list wrappers produced by PostgreSQL
    while isinstance(raw, list):
        if not raw:
            raise ValueError("EXPLAIN JSON array is empty.")
        raw = raw[0]

    if not isinstance(raw, dict):
        raise ValueError("Expected a JSON object at the top level.")
    if "Plan" not in raw:
        raise ValueError('No "Plan" key found in the EXPLAIN JSON output.')

    result: dict[str, Any] = {"Plan": extract_node(raw["Plan"])}

    for timing_key in ("Planning Time", "Execution Time"):
        if timing_key in raw:
            result[timing_key] = raw[timing_key]

    return result


# ---------------------------------------------------------------------------
# Plan-generation helpers (from aqp.py)
# ---------------------------------------------------------------------------

_JOIN_SETTINGS: list[dict] = [
    {},                                                        # default (no overrides)
    {"enable_hashjoin": "off", "enable_mergejoin": "off"},     # force Nested Loop
    {"enable_hashjoin": "off", "enable_nestloop":  "off"},     # force Merge Join
    {"enable_mergejoin": "off", "enable_nestloop": "off"},     # force Hash Join
]

_SCAN_SETTINGS: list[dict] = [
    {},                                                        # default (no overrides)
    {"enable_seqscan": "off"},                                 # no sequential scan
    {"enable_indexscan": "off"},                               # no index scan
    {"enable_seqscan": "off", "enable_indexscan": "off"},      # bitmap attempt
]


def _setting_combinations() -> list[dict]:
    """Cartesian product of join × scan setting overrides."""
    return [{**j, **s} for j in _JOIN_SETTINGS for s in _SCAN_SETTINGS]


def _filter_plan(node: Any) -> Any:
    """Recursively retain only KEEP_KEYS from a raw plan dict/list."""
    if isinstance(node, list):
        return [_filter_plan(item) for item in node]
    if not isinstance(node, dict):
        return node
    filtered: dict = {}
    for key, value in node.items():
        if key in KEEP_KEYS:
            filtered[key] = _filter_plan(value) if key == "Plans" else value
    return filtered


def _remove_gather(node: dict) -> dict:
    """Replace Gather nodes with their single child (transparent parallelism wrapper)."""
    if not isinstance(node, dict):
        return node
    if node.get("Node Type") == "Gather" and "Plans" in node:
        return _remove_gather(node["Plans"][0])
    if "Plans" in node:
        node["Plans"] = [_remove_gather(child) for child in node["Plans"]]
    return node


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

def get_connection():
    """
    Open and return a new psycopg2 connection using credentials from the
    environment.  Raises KeyError / OperationalError on misconfiguration.
    """
    password = os.environ.get("PGPASSWORD")
    if password is None:
        raise EnvironmentError(
            "PGPASSWORD environment variable is not set. "
            "Please set it before starting the application."
        )

    return psycopg2.connect(
        dbname=os.environ.get("PGDATABASE", "sc3020"),
        user=os.environ.get("PGUSER",       "postgres"),
        password=password,
        host=os.environ.get("PGHOST",       "localhost"),
        port=os.environ.get("PGPORT",       "5432"),
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate_plans(query: str) -> dict[str, Any]:
    """
    Run ``EXPLAIN (FORMAT JSON)`` for *query* under every planner-setting
    combination and return the chosen QEP together with all unique AQPs.

    Return shape
    ------------
    {
        "qep":  <filtered plan node>,        # default planner choice
        "aqps": [                             # alternative plans (deduplicated)
            {"settings": {...}, "qep": <filtered plan node>},
            ...
        ]
    }
    """
    conn = get_connection()
    cur  = conn.cursor()

    try:
        default_qep: Optional[dict] = None
        aqps: list[dict]            = []
        seen_plans: set[str]        = set()

        for setting in _setting_combinations():
            cur.execute("RESET ALL;")
            for key, value in setting.items():
                cur.execute(f"SET {key} = {value};")

            cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
            raw_result = cur.fetchone()[0][0]

            plan = _filter_plan(raw_result["Plan"])
            plan = _remove_gather(plan)

            # Skip duplicate plans (same structure under different settings)
            plan_str = json.dumps(plan, sort_keys=True)
            if plan_str in seen_plans:
                continue
            seen_plans.add(plan_str)

            if not setting:              # empty dict → default planner settings
                default_qep = plan
            else:
                aqps.append({"settings": setting, "qep": plan})

        return {"qep": default_qep, "aqps": aqps}

    finally:
        cur.close()
        conn.close()