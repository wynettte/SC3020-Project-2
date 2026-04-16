# annotation.py
# Walks a QEP plan tree and produces one Annotation per interesting node.

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class Annotation:
    """
    One annotation attached to a part of the SQL query.

    Attributes
    ----------
    ann_type  : category — "scan" | "join" | "sort" | "aggregate" |
                           "filter" | "subquery" | "limit"
    target    : the SQL fragment this annotation describes, e.g.
                  "customer"                    (for a scan)
                  "C.c_custkey = O.o_custkey"   (for a join)
    text      : human-readable description (WHAT)
    reasoning : human-readable reasoning   (WHY)
    detail    : optional extra detail (e.g. cost breakdown dict)
    """
    ann_type:  str
    target:    str
    text:      str
    reasoning: str = ""
    detail:    dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def annotate_query(qep_node: dict, aqps_list: list) -> list[Annotation]:
    """Walk the QEP plan tree and return one Annotation per interesting node."""
    annotations: list[Annotation] = []
    _walk_node(qep_node, aqps_list, annotations)
    return annotations


# ---------------------------------------------------------------------------
# Node-type helpers
# ---------------------------------------------------------------------------

_SCAN_TYPES: set[str] = {
    "Seq Scan", "Index Scan", "Index Only Scan",
    "Bitmap Heap Scan", "Bitmap Index Scan",
    "Parallel Seq Scan", "Parallel Index Scan",
    "Function Scan",
}

_AGGREGATE_TYPES: set[str] = {
    "Aggregate", "HashAggregate", "GroupAggregate",
    "MixedAggregate", "Finalize Aggregate", "Partial Aggregate",
}

_SETTING_TO_LABEL: dict[frozenset, str] = {
    frozenset(["enable_hashjoin",  "enable_mergejoin"]): "Nested Loop Join",
    frozenset(["enable_hashjoin",  "enable_nestloop"]):  "Merge Join",
    frozenset(["enable_mergejoin", "enable_nestloop"]):  "Hash Join",
    frozenset(["enable_seqscan"]):                       "Index Scan (no seq scan)",
    frozenset(["enable_indexscan"]):                     "Seq Scan (no index scan)",
    frozenset(["enable_seqscan", "enable_indexscan"]):   "Bitmap Scan",
}


def _is_scan(node_type: str) -> bool:
    return node_type in _SCAN_TYPES

def _is_join(node_type: str) -> bool:
    return node_type in ("Hash Join", "Merge Join", "Nested Loop")

def _is_aggregate(node_type: str) -> bool:
    return node_type in _AGGREGATE_TYPES

def _label_for_aqp(settings: dict) -> str:
    """Turn {'enable_hashjoin': 'off', 'enable_mergejoin': 'off'} into a readable label."""
    disabled = frozenset(k for k, v in settings.items() if str(v).lower() == "off")
    return _SETTING_TO_LABEL.get(disabled, str(settings))


# ---------------------------------------------------------------------------
# Tree walker
# ---------------------------------------------------------------------------

def _walk_node(node: dict, aqps_list: list, out: list[Annotation]) -> None:
    """Recursively visit every plan node and dispatch to the right handler."""
    node_type = node.get("Node Type", "")

    if _is_scan(node_type):
        ann = _annotate_scan(node)
        if ann:
            out.append(ann)

    elif _is_join(node_type):
        ann = _annotate_join(node, aqps_list)
        if ann:
            out.append(ann)

    elif node_type == "Hash":
        ann =_annotate_hash(node)
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
                "A LIMIT clause is applied. PostgreSQL stops reading rows "
                "once the requested count is reached."
            ),
            reasoning=(
                "Stopping early reduces total execution cost — rows beyond "
                "the limit are never fetched or processed."
            ),
            detail={"cost": node.get("Total Cost")},
        ))

    elif node_type == "Subquery Scan":
        alias = node.get("Alias", "subquery")
        out.append(Annotation(
            ann_type="subquery",
            target=alias,
            text=(
                f"A subquery ('{alias}') is materialised here. PostgreSQL "
                "evaluates the inner query first and treats its result as a "
                "temporary table."
            ),
            reasoning=(
                "The subquery result is needed as an independent relation "
                "before the outer query can proceed. Materialising it avoids "
                "re-evaluating the inner query for every outer row."
            ),
            detail={"cost": node.get("Total Cost")},
        ))

    # Standalone filter on non-scan nodes (scans handle their own filters internally)
    filter_clause = node.get("Filter")
    if filter_clause and not _is_scan(node_type):
        ann = _annotate_filter(node, filter_clause)
        if ann:
            out.append(ann)

    for child in node.get("Plans", []):
        _walk_node(child, aqps_list, out)


# ---------------------------------------------------------------------------
# Per-node annotators
# ---------------------------------------------------------------------------

def _annotate_scan(node: dict) -> Optional[Annotation]:
    """Explain how the table was accessed and why that method was chosen."""
    node_type  = node.get("Node Type", "")
    table_name = node.get("Relation Name") or node.get("CTE Name") or "?"
    alias      = node.get("Alias", "")
    display    = table_name + (f" ({alias})" if alias and alias != table_name else "")

    if node_type == "Seq Scan":
        filter_clause = node.get("Filter", "")
        rows_removed  = node.get("Rows Removed by Filter", "")
        text = (
            f"'{table_name}' is read using a Sequential Scan — PostgreSQL "
            "reads every row in the table from start to finish."
        )
        if filter_clause:
            text += (
                f" The filter condition ({filter_clause}) is evaluated on "
                "each row after it is read."
            )
            if rows_removed:
                text += f" {rows_removed} rows were discarded by this filter."
        reasoning = (
            "A sequential scan is chosen because either no suitable index "
            "exists on the queried column(s), the table is small enough that "
            "reading it fully is cheaper than an index lookup, or a large "
            "proportion of rows must be returned making index overhead unnecessary."
        )

    elif node_type == "Index Scan":
        index = node.get("Index Name", "an index")
        cond  = node.get("Index Cond", "")
        text  = (
            f"'{table_name}' is accessed using an Index Scan on '{index}'. "
            "PostgreSQL first looks up matching entries in the B-tree index, "
            "then follows the reference to fetch the full row from the heap table."
        )
        if cond:
            text += f" The index condition evaluated is: {cond}."
        reasoning = (
            "An index scan is chosen because the query targets a small subset "
            "of rows and an appropriate index exists on the lookup column(s). "
            "It would not be chosen if the query needed to return a large "
            "fraction of the table, as the overhead of random heap fetches "
            "would then outweigh the benefit."
        )

    elif node_type == "Index Only Scan":
        index      = node.get("Index Name", "an index")
        heap_fetch = node.get("Heap Fetches", 0)
        text = (
            f"'{table_name}' uses an Index Only Scan on '{index}'. "
            "All columns required by the query are stored within the index "
            "itself, so PostgreSQL never needs to read the main heap table."
        )
        if heap_fetch == 0:
            text += " No heap pages were accessed at all during this scan."
        elif heap_fetch:
            text += (
                f" {heap_fetch} heap page(s) were still fetched to verify "
                "visibility for recently modified rows."
            )
        reasoning = (
            "An index only scan is the most efficient access path: because "
            "all required columns exist in the index, heap I/O is eliminated "
            "entirely. PostgreSQL chooses it when a covering index is available "
            "and the query does not need extra columns from the table."
        )

    elif node_type == "Bitmap Heap Scan":
        recheck      = node.get("Recheck Cond", "")
        rows_removed = node.get("Rows Removed by Filter", "")
        text = (
            f"'{table_name}' uses a Bitmap Heap Scan (phase 2 of 2). "
            "Using the bitmap built in phase 1, PostgreSQL fetches only the "
            "relevant heap pages in physical disk order."
        )
        if recheck:
            text += f" The condition '{recheck}' is rechecked on each heap row fetched."
        if rows_removed:
            text += f" {rows_removed} rows were removed after the recheck filter."
        reasoning = (
            "A bitmap scan is chosen when the query matches too many rows for "
            "a plain index scan (whose random I/O would be expensive) but not "
            "enough to justify reading the whole table sequentially. It is also "
            "used when multiple filter conditions each have their own index, "
            "allowing PostgreSQL to combine them via bitmap AND/OR operations."
        )

    elif node_type == "Bitmap Index Scan":
        index = node.get("Index Name", "an index")
        cond  = node.get("Index Cond", "")
        text  = (
            f"Bitmap Index Scan on '{index}' (phase 1 of 2). "
            "PostgreSQL scans the index to build an in-memory bitmap marking "
            "which heap pages may contain matching rows."
        )
        if cond:
            text += f" Index condition used to build the bitmap: {cond}."
        reasoning = (
            "The bitmap is passed to the Bitmap Heap Scan above, which uses it "
            "to fetch only the marked pages in physical order — avoiding the "
            "random I/O cost of fetching pages one at a time as a plain index "
            "scan would do."
        )

    elif node_type == "Parallel Seq Scan":
        workers = node.get("Workers Planned", "multiple")
        text = (
            f"'{table_name}' uses a Parallel Sequential Scan with {workers} "
            "background worker(s). PostgreSQL divides the table into chunks "
            "and scans each chunk concurrently, combining results at a Gather node."
        )
        reasoning = (
            "Parallel execution is chosen because the table is large enough "
            "that dividing the work across multiple CPU cores reduces total "
            "elapsed time, even after accounting for the coordination overhead "
            "of launching workers and merging their results."
        )

    elif node_type == "Parallel Index Scan":
        index   = node.get("Index Name", "an index")
        workers = node.get("Workers Planned", "multiple")
        text = (
            f"'{table_name}' uses a Parallel Index Scan on '{index}' "
            f"with {workers} background worker(s). Each worker reads a "
            "different portion of the index concurrently, and results are "
            "gathered at the end."
        )
        reasoning = (
            "Parallel index scanning is chosen when both the index and the "
            "table are very large and parallel execution is estimated to be "
            "faster than a single-worker index scan."
        )

    elif node_type == "Function Scan":
        fn = node.get("Function Name", "?")
        text = (
            f"Rows are produced by calling the set-returning function '{fn}'. "
            "PostgreSQL executes the function and iterates over its output "
            "as if it were a table."
        )
        reasoning = (
            f"The query references '{fn}' directly in the FROM clause as a "
            "table-valued function. PostgreSQL has no base table to scan, so "
            "it calls the function and streams its rows into the rest of the plan."
        )

    else:
        text      = f"'{display}' is accessed using {node_type}."
        reasoning = ""

    return Annotation(
        ann_type="scan",
        target=table_name,
        text=text,
        reasoning=reasoning,
        detail={
            "node_type": node_type,
            "rows":      node.get("Plan Rows"),
            "cost":      node.get("Total Cost"),
        },
    )


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


def _annotate_join(node: dict, aqps: list) -> Optional[Annotation]:
    """Explain which join algorithm was used and why (cost vs alternatives)."""
    node_type = node.get("Node Type", "")
    qep_cost  = node.get("Total Cost", 0)

    condition = (
        node.get("Hash Cond")
        or node.get("Merge Cond")
        or node.get("Join Filter")
        or node.get("Index Cond")
        or ""
    )

    if node_type == "Hash Join":
        text = (
            "This join uses Hash Join. "
            "PostgreSQL builds a hash table from the smaller relation, "
            "then probes it with each row from the larger relation."
        )
    elif node_type == "Merge Join":
        text = (
            "This join uses Merge Join. "
            "Both input relations must be sorted on the join key; "
            "PostgreSQL then merges them in a single pass."
        )
    elif node_type == "Nested Loop":
        text = (
            "This join uses Nested Loop. "
            "For each row in the outer relation, PostgreSQL scans the inner relation."
        )
    else:
        text = f"This join uses {node_type}."

    if condition:
        text += f" Join condition: {condition}."

    # WHY: cost comparison against AQPs
    cost_parts: list[str] = []
    cost_detail: dict = {
        "node_type":    node_type,   # stored here so interface.py can build the op_id key
        "qep_cost":     qep_cost,
        "alternatives": {},
    }

    for aqp_entry in aqps:
        settings      = aqp_entry.get("settings", {})
        aqp_node      = aqp_entry.get("qep", {})
        label         = _label_for_aqp(settings)
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
                    f"{label} would cost ~{(1 / ratio):.1f}x less "
                    "(but may not be applicable to this query structure)"
                )

    if cost_parts:
        reasoning = (
            f"{node_type} has the lowest estimated cost among the available "
            "join strategies. Compared to alternatives: "
            + "; ".join(cost_parts) + "."
        )
    else:
        reasoning = (
            f"{node_type} was selected by the planner. "
            "No alternative join strategy produced a cheaper estimated plan "
            "for this query."
        )

    return Annotation(
        ann_type="join",
        target=condition,
        text=text,
        reasoning=reasoning,
        detail=cost_detail,
    )


def _annotate_filter(node: dict, filter_clause: str) -> Optional[Annotation]:
    """Explain how many rows were removed by a filter on a non-scan node."""
    node_type    = node.get("Node Type", "")
    rows_removed = node.get("Rows Removed by Filter", "")

    text = (
        f"A filter ({filter_clause}) is applied at the '{node_type}' node. "
        "Rows that do not satisfy this condition are discarded before being "
        "passed to the next stage of the plan."
    )
    if rows_removed:
        text += f" {rows_removed} rows were removed by this filter."

    reasoning = (
        "This filter cannot be pushed down to the scan level — for example, "
        "it may reference a computed column, a join result, or an expression "
        "that is only available after earlier plan nodes have executed. "
        "PostgreSQL therefore applies it here, as late as possible."
    )

    return Annotation(
        ann_type="filter",
        target=filter_clause,
        text=text,
        reasoning=reasoning,
        detail={"node_type": node_type, "rows_removed": rows_removed},
    )

def _annotate_hash(node: dict) -> Optional[Annotation]:
    """Explain what the Hash node does and why it is present."""
    return Annotation(
        ann_type="hash",
        target="Hash",
        text=(
            "PostgreSQL reads all rows from the child scan and loads them "
            "into an in-memory hash table in preparation for the Hash Join above."
        ),
        reasoning=(
            "Hash is not chosen independently — it is always introduced as a "
            "required preparation step when the planner selects a Hash Join. "
            "The smaller of the two relations is hashed so that the larger "
            "relation can probe it row by row efficiently during the join."
        ),
        detail={"cost": node.get("Total Cost")},
    )

def _annotate_sort(node: dict) -> Optional[Annotation]:
    """Explain why a Sort node is present and what it sorts on."""
    keys     = node.get("Sort Key", [])
    keys_str = ", ".join(keys) if keys else "unknown columns"

    text = f"Results are sorted by [{keys_str}]."
    reasoning = (
        "An explicit sort step is introduced because the data arriving at "
        "this node is not already in the required order. This may be needed "
        "to satisfy an ORDER BY clause, to prepare input for a following "
        "Merge Join, or to group rows for a GroupAggregate."
    )

    return Annotation(
        ann_type="sort",
        target=keys_str,
        text=text,
        reasoning=reasoning,
        detail={"sort_keys": keys, "cost": node.get("Total Cost")},
    )


def _annotate_aggregate(node: dict) -> Optional[Annotation]:
    """Explain the aggregation strategy used and why."""
    node_type  = node.get("Node Type", "Aggregate")
    group_keys = node.get("Group Key", [])
    keys_str   = ", ".join(group_keys) if group_keys else ""

    if node_type == "HashAggregate":
        text = (
            "Hash Aggregate: PostgreSQL builds a hash table keyed on the "
            "GROUP BY columns and accumulates aggregate values into it."
        )
        reasoning = (
            "Hash aggregation is chosen because the input rows are not "
            "pre-sorted on the GROUP BY key. Building a hash table is more "
            "efficient than sorting when the number of distinct groups fits "
            "comfortably in work_mem."
        )
    elif node_type == "GroupAggregate":
        text = (
            "Group Aggregate: PostgreSQL reads pre-sorted input and emits "
            "one aggregate result per group in a single pass."
        )
        reasoning = (
            "Group aggregation is chosen because the input arriving at this "
            "node is already sorted on the GROUP BY key (either from an "
            "earlier Sort node or an index scan), making a hash table "
            "unnecessary and avoiding its memory overhead."
        )
    else:
        text = f"{node_type}: computes aggregate functions over the input rows."
        reasoning = (
            f"PostgreSQL selected {node_type} as the aggregation strategy "
            "based on the estimated input size, available memory, and whether "
            "the data is already ordered on the grouping columns."
        )

    if keys_str:
        text += f" Grouping by: [{keys_str}]."
    else:
        text += " No GROUP BY — all rows are aggregated into a single result."

    return Annotation(
        ann_type="aggregate",
        target=keys_str,
        text=text,
        reasoning=reasoning,
        detail={"group_keys": group_keys, "cost": node.get("Total Cost")},
    )