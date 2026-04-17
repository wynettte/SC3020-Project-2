from __future__ import annotations

from dotenv import load_dotenv
load_dotenv() # Loads the database credentials and the environment variables from .env file

from dataclasses import asdict
from typing import Any

from preprocessing import generate_plans
from annotation import annotate_query


def process_query(query: str) -> dict[str, Any]:
    # Generate the query plans
    # It connects PostgreSQL and will run EXPLAIN under different settings which collects
    # both of the QEP and AQPs
    plans = generate_plans(query)
    
    # Extract out the main QEP
    qep_node  = plans["qep"]

    # Extract out the AQPs
    aqps_list = plans["aqps"]

    # Generate the annotations
    # Traverse QEP and compare with AQPs for explanation
    annotations = annotate_query(qep_node, aqps_list)

    # Format the output
    # Converts annotation objects into dictionaries
    return {
        "qep":         qep_node,
        "aqps":        aqps_list,
        "annotations": [asdict(ann) for ann in annotations],
    }