from aqp import generate_plans
from annotation import annotate_query

def process_query(query: str):

    plans_result = {
    "plans": [
        {
            "qep": {"Plan": "Seq Scan"},
            "aqps": [{"Plan": "Index Scan"}]
        }
    ]
}
    # [1] Generate QEP + AQPs
    plans_result = generate_plans(query)
    # Extract main plan bundle
    plan_bundle = plans_result["plans"][0]

    # PostgreSQL optimizer selects the default plan
    qep = plan_bundle["qep"]
    # Planner configuration generates a list of alternative plans
    aqps_list = plan_bundle["aqps"]

    # [2] Generate annotations from QEP + AQPs
    annotations = annotate_query(qep, aqps_list)

    # [3] Convert annotations to JSON format
    annotations_dict = [ann.__dict__ for ann in annotations]

    # Final JSON output
    return {
        "query": query,
        "qep": qep,
        "aqps": aqps_list,
        "annotations": annotations_dict
    }