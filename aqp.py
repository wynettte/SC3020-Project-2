import psycopg2
import json

# --- DB CONNECTION ---
conn = psycopg2.connect(
    dbname="sc3020",
    user="postgres",
    password="password",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# --- HARDCODED QUERY ---
query = """
SELECT c.c_custkey, c.c_name, o.o_orderkey, o.o_totalprice
FROM customer c
JOIN orders o
  ON c.c_custkey = o.o_custkey
WHERE o.o_totalprice BETWEEN 1000 AND 5000;
"""

# --- PARAMETERS TO DISPLAY ---
KEEP_KEYS = {
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

# --- SETTINGS ---
join_settings = [
    {},  # default
    {"enable_hashjoin": "off", "enable_mergejoin": "off"},  # nested loop
    {"enable_hashjoin": "off", "enable_nestloop": "off"},   # merge join
    {"enable_mergejoin": "off", "enable_nestloop": "off"}   # hash join
]

scan_settings = [
    {},  # default
    {"enable_seqscan": "off"},
    {"enable_indexscan": "off"},
    {"enable_seqscan": "off", "enable_indexscan": "off"}  # bitmap attempt
]

def filter_plan(node, keep_keys):
    if isinstance(node, list):
        return [filter_plan(item, keep_keys) for item in node]

    if not isinstance(node, dict):
        return node

    filtered = {}

    for key, value in node.items():
        if key in keep_keys:
            if key == "Plans":
                # recurse into children
                filtered[key] = filter_plan(value, keep_keys)
            else:
                filtered[key] = value

    return filtered

def remove_gather(node):
    if not isinstance(node, dict):
        return node

    # If current node is Gather, replace it with its child
    if node.get("Node Type") == "Gather" and "Plans" in node:
        return remove_gather(node["Plans"][0])

    # Otherwise recurse into children
    if "Plans" in node:
        node["Plans"] = [remove_gather(child) for child in node["Plans"]]

    return node

def generate_combinations(join_settings, scan_settings):
    combined = []
    for j in join_settings:
        for s in scan_settings:
            merged = {**j, **s}
            combined.append(merged)
    return combined

def apply_settings(setting):
    for key, value in setting.items():
        cur.execute(f"SET {key} = {value};")

def reset_settings():
    cur.execute("RESET ALL;")

def generate_plans(query):
    combined_settings = generate_combinations(join_settings, scan_settings)

    aqps = []
    default_qep = None
    seen_plans = set()

    for setting in combined_settings:
        reset_settings()
        apply_settings(setting)

        cur.execute(f"EXPLAIN (FORMAT JSON) {query}")
        raw_qep = cur.fetchone()[0][0]
        qep = filter_plan(raw_qep["Plan"], KEEP_KEYS)
        qep = remove_gather(qep)

        # Deduplicate plans
        plan_str = json.dumps(qep, sort_keys=True)
        if plan_str in seen_plans:
            continue
        seen_plans.add(plan_str)

        if setting == {}:
            default_qep = qep
        else:
            aqps.append({
                "settings": setting,
                "qep": qep
            })

    return {
        "plans": [
            {
                "qep": default_qep,
                "aqps": aqps
            }
        ]
    }

result = generate_plans(query)

print(json.dumps(result, indent=2))

cur.close()
conn.close()