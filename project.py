from fastapi import FastAPI
from pydantic import BaseModel

from aqp import generate_plans
from annotation import _annotate_scan, _annotate_join

app = FastAPI()

# Request Schema
class QueryRequest(BaseModel):
    query: str

# Traverse Plan Tree
def traverse_plan(node, aqps, annotations):
    if not isinstance(node, dict):
        return

    node_type = node.get("Node Type", "")

    # Scan Annotation
    if "Scan" in node_type:
        ann = _annotate_scan(node)
        if ann:
            annotations.append(ann.__dict__)

    # Join Annotation
    if "Join" in node_type or node_type == "Nested Loop":
        ann = _annotate_join(node, aqps)
        if ann:
            annotations.append(ann.__dict__)

    for child in node.get("Plans", []):
        traverse_plan(child, aqps, annotations)


# Core Pipeline
def process_query(query: str):
    # [1] Get QEP + [2] AQPs
    plans_result = generate_plans(query)

    plan_bundle = plans_result["plans"][0]
    qep = plan_bundle["qep"]
    aqps = {
        f"AQP_{i}": aqp["qep"]
        for i, aqp in enumerate(plan_bundle["aqps"])
    }

    # [3] Generate annotations
    annotations = []
    traverse_plan(qep, aqps, annotations)

    # [4] Final JSON output
    return {
        "query": query,
        "qep": qep,
        "aqps": aqps,
        "annotations": annotations
    }

# API Endpoint
@app.post("/analyze")
def analyze(request: QueryRequest):
    result = process_query(request.query)
    return result