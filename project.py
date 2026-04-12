# Orchestration layer — ties preprocessing and annotation together.

from __future__ import annotations


from dotenv import load_dotenv
load_dotenv()

from dataclasses import asdict
from typing import Any

from preprocessing import generate_plans
from annotation import annotate_query


def process_query(query: str) -> dict[str, Any]:
    """
    Full pipeline for a single SQL query:

      1. preprocessing.generate_plans  — connect to PostgreSQL, run EXPLAIN
         under default and alternative planner settings, deduplicate plans.
      2. annotation.annotate_query     — walk the chosen QEP tree and produce
         human-readable Annotation objects for each interesting node.

    Return shape
    ------------
    {
        "qep": <filtered QEP plan node>,
        "aqps": [
            {"settings": {...}, "qep": <filtered plan node>},
            ...
        ],
        "annotations": [
            {
                "ann_type": "scan" | "join" | "sort" | "aggregate" | "filter" |
                            "subquery" | "limit",
                "target":   "<SQL fragment>",
                "text":     "<human-readable explanation>",
                "detail":   { ... }
            },
            ...
        ]
    }
    """
    # Step 1 — generate QEP and AQPs from the database
    plans = generate_plans(query)
    qep_node  = plans["qep"]
    aqps_list = plans["aqps"]

    # Step 2 — annotate the chosen QEP against the AQPs
    annotations = annotate_query(qep_node, aqps_list)

    return {
        "qep":         qep_node,
        "aqps":        aqps_list,
        "annotations": [asdict(ann) for ann in annotations],
    }