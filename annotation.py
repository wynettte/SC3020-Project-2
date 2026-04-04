from dataclasses import dataclass, field
from typing import Optional

@dataclass
class Annotation:
    """
    One annotation attached to a part of the SQL query.
 
    Attributes
    ----------
    ann_type  : category — "scan" | "join" | "sort" | "aggregate" |
                           "filter" | "subquery" | "limit"
    target    : the SQL fragment this annotation describes, e.g.
                  "customer"  (for a scan)
                  "C.c_custkey = O.o_custkey"  (for a join)
    text      : human-readable explanation
    detail    : optional extra detail (e.g. cost breakdown dict)
    """
    ann_type : str
    target   : str
    text     : str
    detail   : dict = field(default_factory=dict)

def annotate_query(qep_node: dict, aqps_list: list) -> list:
    # walk the QEP plan tree and return one Annotation per interesting node.
 
    annotations = []
    _walk_node(qep_node, aqps_list, annotations)
    return annotations

_SETTING_TO_LABEL = {
    frozenset(["enable_hashjoin",  "enable_mergejoin"]) : "Nested Loop Join",
    frozenset(["enable_hashjoin",  "enable_nestloop"])  : "Merge Join",
    frozenset(["enable_mergejoin", "enable_nestloop"])  : "Hash Join",
    frozenset(["enable_seqscan"])                       : "Index Scan (no seq scan)",
    frozenset(["enable_indexscan"])                     : "Seq Scan (no index scan)",
    frozenset(["enable_seqscan", "enable_indexscan"])   : "Bitmap Scan",
}
 
def _label_for_aqp(settings: dict) -> str:
    """Turn { 'enable_hashjoin': 'off', 'enable_mergejoin': 'off' } into a readable label."""
    disabled = frozenset(k for k, v in settings.items() if str(v).lower() == "off")
    return _SETTING_TO_LABEL.get(disabled, str(settings))

_SCAN_TYPES = {
    "Seq Scan", "Index Scan", "Index Only Scan",
    "Bitmap Heap Scan", "Bitmap Index Scan",
    "Parallel Seq Scan", "Parallel Index Scan",
}
 
def _is_scan(node_type: str) -> bool:
    return node_type in _SCAN_TYPES
 
def _is_join(node_type: str) -> bool:
    return node_type in ("Hash Join", "Merge Join", "Nested Loop")
 
def _is_aggregate(node_type: str) -> bool: #not too sure
    return node_type in {
        "Aggregate", "HashAggregate", "GroupAggregate",
        "MixedAggregate", "Finalize Aggregate", "Partial Aggregate",
    }

def _walk_node(node: dict, aqps_list: list, out: list):
    #recursively visits every plan node and dispatch to the right handler
    node_type = node.get("Node Type", "")
 
    if _is_scan(node_type):
        ann = _annotate_scan(node)
        if ann:
            out.append(ann)
 
    elif _is_join(node_type):
        ann = _annotate_join(node, aqps_list)
        if ann:
            out.append(ann)
 
    elif node_type == "Sort":
        ann = _annotate_sort(node)
        if ann:
            out.append(ann)
 
    elif _is_aggregate(node_type):
        ann = _annotate_aggregate(node)
        if ann:
            out.append(ann)
 
    elif node_type == "Limit":
        out.append(Annotation(
            ann_type="limit",
            target="LIMIT",
            text=(
                "A LIMIT clause is applied — PostgreSQL stops reading rows once "
                "the requested count is reached, reducing total execution cost."
            ),
            detail={"cost": node.get("Total Cost")},
        ))
 
    elif node_type == "Subquery Scan":
        alias = node.get("Alias", "subquery")
        out.append(Annotation(
            ann_type="subquery",
            target=alias,
            text=(
                f"A subquery ('{alias}') is materialised here. PostgreSQL evaluates "
                "the inner query first and treats its result as a temporary table."
            ),
            detail={"cost": node.get("Total Cost")},
        ))
 
    for child in node.get("Plans", []):
        _walk_node(child, aqps_list, out)

def _annotate_scan(node: dict) -> Optional[Annotation]:
    """
    how table was accessed and why that method was chosen
    ref: https://www.crunchydata.com/blog/postgres-scan-types-in-explain-plans
    """
    node_type  = node.get("Node Type", "")
    table_name = node.get("Relation Name") or node.get("CTE Name") or "?"
    alias      = node.get("Alias", "")
    display    = f"{table_name}" + (f" ({alias})" if alias and alias != table_name else "")
 
    # seq scan
    if node_type == "Seq Scan":
        filter_clause = node.get("Filter", "")
        rows_removed  = node.get("Rows Removed by Filter", "")
 
        text = (
            f"'{table_name}' is read using a Sequential Scan — PostgreSQL reads "
            f"every row in the table from start to finish. "
        )
        if filter_clause:
            text += (
                f"The filter condition ({filter_clause}) is evaluated on each row "
                f"after it is read. "
            )
            if rows_removed:
                text += f"{rows_removed} rows were discarded by this filter. "
        text += (
            "A sequential scan is chosen here because either no suitable index "
            "exists on the queried column(s), the table is small enough that "
            "reading it fully is cheaper than an index lookup, or a large "
            "proportion of rows must be returned making index overhead unnecessary."
        )
 
    # index scan
    elif node_type == "Index Scan":
        index = node.get("Index Name", "an index")
        cond  = node.get("Index Cond", "")
        text  = (
            f"'{table_name}' is accessed using an Index Scan on '{index}'. "
            "PostgreSQL first looks up matching entries in the B-tree index, "
            "then follows the reference to fetch the full row from the heap table. "
        )
        if cond:
            text += f"The index condition evaluated is: {cond}. "
        text += (
            "An index scan is chosen because the query targets a small subset "
            "of rows and an appropriate index exists on the lookup column(s). "
            "It would not be chosen if the query needed to return a large "
            "fraction of the table, as the overhead of random heap fetches "
            "would then outweigh the benefit."
        )
 
    # index only scan
    elif node_type == "Index Only Scan":
        index       = node.get("Index Name", "an index")
        heap_fetch  = node.get("Heap Fetches", 0)
        text = (
            f"'{table_name}' uses an Index Only Scan on '{index}'. "
            "This is the most efficient scan type: all columns required by "
            "the query are stored within the index itself, so PostgreSQL "
            "never needs to read the main heap table. "
            "This saves significant I/O — indexes are compact and frequently "
            "cached in shared buffers. "
        )
        if heap_fetch == 0:
            text += "No heap pages were accessed at all during this scan."
        elif heap_fetch:
            text += (
                f"{heap_fetch} heap page(s) were still fetched to verify "
                "visibility for recently modified rows."
            )
 
    # bitmap heap scan
    elif node_type == "Bitmap Heap Scan":
        recheck      = node.get("Recheck Cond", "")
        rows_removed = node.get("Rows Removed by Filter", "")
        text = (
            f"'{table_name}' uses a Bitmap Heap Scan (phase 2 of 2). "
            "Using the bitmap built in phase 1, PostgreSQL fetches only the "
            "relevant heap pages — reading them in physical disk order to "
            "avoid the random I/O cost of a plain index scan. "
            "This approach is chosen when the query matches too many rows "
            "for a regular index scan but not enough to justify reading the "
            "entire table sequentially. It is also common when multiple "
            "filter conditions each have their own index, allowing PostgreSQL "
            "to combine those indexes via bitmap AND/OR operations. "
        )
        if recheck:
            text += f"The condition '{recheck}' is rechecked on each heap row fetched. "
        if rows_removed:
            text += f"{rows_removed} rows were removed after the recheck filter."
 
    # bitmap index scan
    elif node_type == "Bitmap Index Scan":
        index = node.get("Index Name", "an index")
        cond  = node.get("Index Cond", "")
        text  = (
            f"Bitmap Index Scan on '{index}' (phase 1 of 2). "
            "PostgreSQL scans the index to build an in-memory bitmap "
            "marking which heap pages may contain matching rows. "
        )
        if cond:
            text += f"Index condition used to build the bitmap: {cond}. "
        text += (
            "This bitmap is then passed to the Bitmap Heap Scan above, "
            "which uses it to fetch only the relevant pages in physical order."
        )
 
    # parallel seq scan
    elif node_type == "Parallel Seq Scan":
        workers = node.get("Workers Planned", "multiple")
        text = (
            f"'{table_name}' uses a Parallel Sequential Scan with {workers} "
            "background worker(s). PostgreSQL divides the table into chunks "
            "and scans each chunk in parallel, combining results at a Gather "
            "node. This is chosen when the table is large enough that "
            "parallel processing reduces total execution time despite the "
            "coordination overhead."
        )
 
    # parallel index scan
    elif node_type == "Parallel Index Scan":
        index   = node.get("Index Name", "an index")
        workers = node.get("Workers Planned", "multiple")
        text = (
            f"'{table_name}' uses a Parallel Index Scan on '{index}' "
            f"with {workers} background worker(s). Each worker reads a "
            "different portion of the index concurrently, and results are "
            "gathered at the end. Chosen when the index and table are both "
            "very large and parallel execution is faster than a single worker."
        )

    # function scan
    elif node_type == "Function Scan":
        fn = node.get("Function Name", "?")
        text = (
            f"Rows are produced by calling the set-returning function '{fn}'. "
            "PostgreSQL executes the function and iterates over its output "
            "as if it were a table."
        )
 
    else:
        text = f"'{display}' is accessed using {node_type}."
 
    return Annotation(
        ann_type="scan",
        target=table_name,
        text=text,
        detail={
            "node_type" : node_type,
            "rows"      : node.get("Plan Rows"),
            "cost"      : node.get("Total Cost"),
        }
    )

    # joins
def _find_join_cost(node: dict) -> Optional[float]:
    """Depth-first search for the first join node's Total Cost in a plan tree."""
    if _is_join(node.get("Node Type", "")):
        return node.get("Total Cost")
    for child in node.get("Plans", []):
        result = _find_join_cost(child)
        if result is not None:
            return result
    return None
 
 
def _find_join_node_type(node: dict) -> Optional[str]:
    """Return the Node Type of the first join node found in a plan tree."""
    if _is_join(node.get("Node Type", "")):
        return node.get("Node Type")
    for child in node.get("Plans", []):
        result = _find_join_node_type(child)
        if result is not None:
            return result
    return None
def _annotate_join(node: dict, aqps: dict) -> Optional[Annotation]:
    
# explains which join algorithm was used and why (cost vs alternatives).
    
    node_type  = node.get("Node Type", "")   
    qep_cost   = node.get("Total Cost", 0)
 
    condition = (
        node.get("Hash Cond")
        or node.get("Merge Cond")
        or node.get("Join Filter")
        or node.get("Index Cond")
        or ""
    )
 
    if node_type == "Hash Join":
        base = (
            f"This join uses Hash Join. "
            "PostgreSQL builds a hash table from the smaller relation, "
            "then probes it with each row from the larger relation. "
            "Efficient for large unsorted inputs with equality conditions."
        )
    elif node_type == "Merge Join":
        base = (
            "This join uses Merge Join. "
            "Both input relations must be sorted on the join key; "
            "PostgreSQL then merges them in a single pass. "
            "Efficient when inputs are already sorted or an index is available."
        )
    elif node_type == "Nested Loop":
        base = (
            "This join uses Nested Loop. "
            "For each row in the outer relation, PostgreSQL scans the inner "
            "relation. Efficient when the inner side is small or has an index."
        )
    else:
        base = f"This join uses {node_type}."
 
    if condition:
        base += f" Join condition: {condition}."
 
    # cost comparison against aqp
    cost_parts = []
    cost_detail = {"qep_cost": qep_cost, "alternatives": {}}
 
    for aqp_entry in aqps_list:
        settings     = aqp_entry.get("settings", {})
        aqp_node     = aqp_entry.get("qep", {})
        label        = _label_for_aqp(settings)
        aqp_join_type = _find_join_node_type(aqp_node)
 
        if aqp_join_type == node_type:
            continue  # same plan as QEP, skip
 
        alt_cost = _find_join_cost(aqp_node)
        cost_detail["alternatives"][label] = alt_cost
 
        if alt_cost is not None and qep_cost and qep_cost > 0:
            ratio = alt_cost / qep_cost
            if ratio > 1.05:
                cost_parts.append(f"{label} would cost ~{ratio:.1f}x more")
            elif ratio < 0.95:
                cost_parts.append(
                    f"{label} would cost ~{(1/ratio):.1f}x less "
                    "(but may not be applicable to this query structure)"
                )
 
    if cost_parts:
        why = " Compared to alternatives: " + "; ".join(cost_parts) + "."
    else:
        why = " No cheaper alternative join was found by the planner."
 
    return Annotation(
        ann_type="join",
        target=condition,
        text=base + why,
        detail=cost_detail,
    ) 

# sort annotator
def _annotate_sort(node: dict) -> Optional[Annotation]:
    keys     = node.get("Sort Key", [])
    keys_str = ", ".join(keys) if keys else "unknown columns"
    text = (
        f"Results are sorted by [{keys_str}]. "
        "Sorting is required to satisfy an ORDER BY clause, "
        "support a following Merge Join, or prepare data for a GroupAggregate."
    )
    return Annotation(
        ann_type="sort",
        target=keys_str,
        text=text,
        detail={"sort_keys": keys, "cost": node.get("Total Cost")},
    )

# aggregate annotator
def _annotate_aggregate(node: dict) -> Optional[Annotation]:
    node_type  = node.get("Node Type", "Aggregate")
    group_keys = node.get("Group Key", [])
    keys_str   = ", ".join(group_keys) if group_keys else ""
 
    if node_type == "HashAggregate":
        strategy = (
            "Hash Aggregate: groups are built using a hash table. "
            "Efficient for unsorted input when the number of distinct groups fits in memory."
        )
    elif node_type == "GroupAggregate":
        strategy = (
            "Group Aggregate: input is pre-sorted on the GROUP BY key. "
            "Processes groups in one pass without a hash table."
        )
    else:
        strategy = f"{node_type}: computes aggregate functions over input rows."
 
    text = strategy + (f" Grouping by: [{keys_str}]." if keys_str
                       else " No GROUP BY — aggregating all rows into one result.")
 
    return Annotation(
        ann_type="aggregate",
        target=keys_str,
        text=text,
        detail={"group_keys": group_keys, "cost": node.get("Total Cost")},
    )