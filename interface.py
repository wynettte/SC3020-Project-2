# PyQt6 desktop UI for Query Plan-Based SQL Comprehension.
# Calls the backend (project.process_query) exclusively

from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import html
import sys
import re
from dataclasses import dataclass
from typing import Dict, List, Optional
from annotation import _is_join

from PyQt6.QtCore import QPointF, QRectF, Qt, QUrl, pyqtSignal
from PyQt6.QtGui import QColor, QFont, QPainter, QPen
from PyQt6.QtWidgets import (
    QApplication,
    QFrame,
    QHBoxLayout,
    QAbstractItemView,
    QLabel,
    QMainWindow,
    QPushButton,
    QMessageBox,
    QPlainTextEdit,
    QTableWidget,
    QTableWidgetItem,
    QSplitter,
    QStatusBar,
    QTextBrowser,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
    QStackedWidget,
    QTabWidget,
)

# ---------------------------------------------------------------------------
# Data contracts
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class OperatorInfo:
    name:         str
    what:         str
    why:          str
    alternatives: str
    impact:       str


@dataclass(frozen=True)
class AnalysisData:
    """
    Canonical data contract consumed by every UI widget.
    Built from the dict returned by project.process_query().
    """
    raw_sql:                str
    operator_info:          Dict[str, OperatorInfo]
    chosen_plan_id:         str
    plan_comparisons:       List[dict]
    sql_badge_replacements: List[dict]
    qep_tree_model:         dict


# ---------------------------------------------------------------------------
# Backend → AnalysisData conversion helpers
# ---------------------------------------------------------------------------

def _build_operator_info(
    annotations: list[dict], qep_node: dict | None = None
) -> Dict[str, OperatorInfo]:
    """Convert annotation dicts from process_query into OperatorInfo objects."""
    info: Dict[str, OperatorInfo] = {}

    # Walk QEP in DFS order and record, for every Hash node, the direct-child
    # table name (leaf hash) or "" (non-leaf hash whose child is a join).
    # The list order matches the order hash annotations are produced by _walk_node,
    # so we can pop one entry per hash annotation to get a consistent op_id.
    hash_table_seq: list[str] = []
    hash_nl_idx = 0   # counter for non-leaf Hash nodes
    if qep_node:
        def _collect_hash_seq(node: dict) -> None:
            if node.get("Node Type") == "Hash":
                child_table = ""
                for child in node.get("Plans", []):
                    child_table = child.get("Relation Name") or child.get("Alias", "")
                    if child_table:
                        break
                hash_table_seq.append(child_table)   # "" if non-leaf
            for child in node.get("Plans", []):
                _collect_hash_seq(child)
        _collect_hash_seq(qep_node)

    hash_ann_iter = iter([a for a in annotations if a["ann_type"] == "hash"])
    join_idx = 0

    for ann in annotations:
        ann_type = ann["ann_type"]
        detail   = ann.get("detail", {})

        if ann_type == "join":
            node_type = detail.get("node_type", ann_type)
            op_id     = f"join_{node_type.replace(' ', '_')}_{join_idx}"
            name      = node_type
            join_idx += 1

        elif ann_type == "hash":
            # Pop the pre-computed table name for this Hash node (DFS order).
            # Leaf hash (child is a Scan) → "hash_{table}"; matches the SQL badge.
            # Non-leaf hash (child is a Join) → "hash_nl_{idx}"; unique, no SQL badge.
            table = hash_table_seq.pop(0) if hash_table_seq else ""
            if table:
                op_id = f"hash_{table}"
                name  = f"Hash ({table})"
            else:
                op_id = f"hash_nl_{hash_nl_idx}"
                hash_nl_idx += 1
                name  = "Hash"

        else:
            op_id = f"{ann_type}_{ann['target'].replace(' ', '_')}"
            if ann_type == "scan" and detail.get("node_type") and ann["target"]:
                name = f"{detail['node_type']} ({ann['target']})"
            elif ann_type == "aggregate" and detail.get("node_type"):
                keys = ann["target"]
                name = f"{detail['node_type']} ({keys})" if keys else detail['node_type']
            else:
                name = ann["target"] or ann_type

        info[op_id] = OperatorInfo(
            name=name,
            what=ann["text"],
            why=ann.get("reasoning", ""),
            alternatives=str(detail.get("alternatives", "")),
            impact=f"Cost: {detail.get('cost', 'N/A')}",
        )
    return info


def _build_qep_tree_model(qep_node: dict, annotations: list[dict]) -> dict:
    """Recursively convert a filtered QEP plan node into the tree-model shape."""
    ann_index: Dict[str, str] = {}
    for ann in annotations:
        if ann["ann_type"] in ("join", "hash"):
            continue  # joins/hashes resolved separately in _convert below
        op_id = f"{ann['ann_type']}_{ann['target'].replace(' ', '_')}"
        # Primary key: the annotation target (used by scan nodes via Relation Name)
        ann_index[ann["target"]] = op_id
        # Secondary key: capitalised ann_type (e.g. "Sort", "Limit") so that
        # _convert can find the op_id via QEP node_type when there is no table name.
        ann_index[ann["ann_type"].capitalize()] = op_id
        # Tertiary key: exact node_type from detail (e.g. "HashAggregate") so
        # aggregate variants are resolved correctly.
        node_type_detail = ann.get("detail", {}).get("node_type", "")
        if node_type_detail:
            ann_index[node_type_detail] = op_id

    join_counter    = [0]   # mutable so the nested _convert closure can increment it
    hash_nl_counter = [0]   # same for non-leaf Hash nodes

    def _convert(node: dict) -> dict:
        node_type  = node.get("Node Type", "Unknown")
        table_name = node.get("Relation Name") or node.get("Alias") or ""
        label      = f"{node_type} ({table_name})" if table_name else node_type
        cost       = str(node.get("Total Cost", "N/A"))
        if _is_join(node_type):
            # Index must match _build_operator_info (both are pre-order DFS)
            op_id = f"join_{node_type.replace(' ', '_')}_{join_counter[0]}"
            join_counter[0] += 1
        elif node_type == "Hash":
            # Leaf hash (child is a Scan): op_id = "hash_{table}" — matches badge.
            # Non-leaf hash (child is a Join): op_id = "hash_nl_{counter}" — unique,
            # no SQL badge but still selectable from the tree.
            child_table = ""
            for child in node.get("Plans", []):
                child_table = child.get("Relation Name") or child.get("Alias", "")
                if child_table:
                    break
            if child_table:
                op_id = f"hash_{child_table}"
            else:
                op_id = f"hash_nl_{hash_nl_counter[0]}"
                hash_nl_counter[0] += 1
        else:
            op_id = (
                ann_index.get(table_name)
                or ann_index.get(node_type)
                or label.replace(" ", "_")
            )
        return {
            "op_id":    op_id,
            "label":    label,
            "cost":     cost,
            "children": [_convert(child) for child in node.get("Plans", [])],
        }

    return _convert(qep_node)

def _get_hashed_tables(qep_node: dict) -> set:
    """
    Walk the QEP tree and return a set of lowercase table/relation names
    that appear as the direct child scan of a Hash node (i.e. the build side
    of a Hash Join).
    """
    hashed: set = set()

    def _walk(node: dict) -> None:
        if node.get("Node Type") == "Hash":
            for child in node.get("Plans", []):
                table = child.get("Relation Name") or child.get("Alias", "")
                if table:
                    hashed.add(table.lower())
        for child in node.get("Plans", []):
            _walk(child)

    _walk(qep_node)
    return hashed


def _build_sql_badge_replacements(
    annotations: list[dict],
    raw_sql: str,
    qep_node: dict | None = None,
) -> List[dict]:
    """
    Map annotations onto SQL fragments so the annotated SQL view can render
    clickable badges.

    Badge placement rules
    ---------------------
    • Scan     → anchored to the full FROM/JOIN clause that introduces the table.
                 Handles all JOIN variants (INNER/LEFT/RIGHT/FULL OUTER/CROSS/NATURAL),
                 schema-qualified names (public.orders), and optional AS aliases.
                 If the table is the Hash-Join build side a [Hash] badge is co-located.
    • Subquery → anchored to the subquery alias in the FROM clause.
    • Join     → anchored to each ON <condition> clause.
    • Other    → anchored to the literal target text (fallback).
    """
    replacements: List[dict] = []

    # All JOIN variants that can appear before the JOIN keyword
    _JOIN_PREFIX = (
        r'(?:(?:INNER|LEFT(?:\s+OUTER)?|RIGHT(?:\s+OUTER)?'
        r'|FULL(?:\s+OUTER)?|CROSS|NATURAL)\s+)?'
    )

    # Collect all join annotations in order (multiple joins in one query each
    # get their own annotation from annotate_query).
    join_annotations = [a for a in annotations if a["ann_type"] == "join"]
    sql_join_idx = [0]   # mutable counter — incremented per join annotation

    # Tables that are the build side of a Hash Join (direct child of Hash node)
    hashed_tables: set = _get_hashed_tables(qep_node) if qep_node else set()

    # Pre-compute per-Hash-node child table in DFS order (mirrors _build_operator_info)
    # so we can assign the same op_ids for non-leaf Hash nodes.
    _hash_seq: list[str] = []
    _hash_nl_badge_idx = [0]
    if qep_node:
        def _collect_hash_seq_badges(node: dict) -> None:
            if node.get("Node Type") == "Hash":
                ct = ""
                for child in node.get("Plans", []):
                    ct = child.get("Relation Name") or child.get("Alias", "")
                    if ct:
                        break
                _hash_seq.append(ct)
            for child in node.get("Plans", []):
                _collect_hash_seq_badges(child)
        _collect_hash_seq_badges(qep_node)

    for ann in annotations:
        target   = ann.get("target", "")
        ann_type = ann["ann_type"]
        detail   = ann.get("detail", {})

        # ── Scan annotation ──────────────────────────────────────────────────
        if ann_type == "scan":
            table     = target
            node_type = detail.get("node_type", "Scan")
            op_id     = f"scan_{table.replace(' ', '_')}"

            # Match any FROM/JOIN variant with optional schema prefix and alias:
            #   FROM public.orders o
            #   LEFT OUTER JOIN orders AS o
            #   CROSS JOIN orders
            pattern = re.compile(
                r'('
                + _JOIN_PREFIX
                + r'(?:FROM|JOIN)\s+'
                + r'(?:\w+\.)?'          # optional schema prefix
                + re.escape(table)
                + r'(?:[ \t]+(?:AS[ \t]+)?'
                  r'(?!SELECT\b|FROM\b|WHERE\b|ON\b|GROUP\b|ORDER\b'
                  r'|HAVING\b|LIMIT\b|JOIN\b|INNER\b|LEFT\b|RIGHT\b'
                  r'|FULL\b|CROSS\b|NATURAL\b|UNION\b|AND\b|OR\b|NOT\b|AS\b)\w+)?'  # alias (keywords excluded)
                + r')',
                re.IGNORECASE,
            )
            m = pattern.search(raw_sql)
            if not m:
                # Fallback: bare table name anywhere in the SQL
                if table.lower() not in raw_sql.lower():
                    continue
                badges: List[dict] = [{"op_id": op_id, "badge_text": f"{node_type} ({table})"}]
                replacements.append({"match": table, "badges": badges})
                continue

            matched_text = m.group(1)
            badges = [{"op_id": op_id, "badge_text": f"{node_type} ({table})"}]

            # [Hash] badge goes on the actual build-side table (from the QEP tree).
            # Use the table name in the op_id so each Hash badge is unique.
            if table.lower() in hashed_tables:
                badges.append({"op_id": f"hash_{table}", "badge_text": "Hash"})

            replacements.append({"match": matched_text, "badges": badges})

        # ── Subquery annotation ───────────────────────────────────────────────
        elif ann_type == "subquery":
            alias = target
            op_id = f"subquery_{alias.replace(' ', '_')}"

            # Match "(SELECT ...) [AS] alias" — the alias is what PostgreSQL uses
            # as the Subquery Scan relation name.
            sub_pattern = re.compile(
                r'(\(\s*SELECT\b[^()]*(?:\([^()]*\)[^()]*)*\)\s*(?:AS\s+)?'
                + re.escape(alias)
                + r'\b)',
                re.IGNORECASE | re.DOTALL,
            )
            m = sub_pattern.search(raw_sql)
            if m:
                replacements.append({
                    "match":  m.group(1),
                    "badges": [{"op_id": op_id, "badge_text": f"Subquery ({alias})"}],
                })
            elif alias.lower() in raw_sql.lower():
                # Fallback: just highlight the alias name
                replacements.append({
                    "match":  alias,
                    "badges": [{"op_id": op_id, "badge_text": f"Subquery ({alias})"}],
                })

        # ── Join annotation ───────────────────────────────────────────────────
        elif ann_type == "join":
            # Each join annotation is processed in pre-order DFS (same order as
            # _build_operator_info and _build_qep_tree_model), so we can simply
            # use a counter to generate the same unique op_id in all three places.
            join_node_type = detail.get("node_type", "Join")
            op_id          = f"join_{join_node_type.replace(' ', '_')}_{sql_join_idx[0]}"
            sql_join_idx[0] += 1

            # Collect all ON clauses and assign only the one at THIS annotation's
            # index — prevents every join annotation from badging every ON clause.
            on_matches = re.findall(
                r'(ON\s+[^\n]+?)'
                r'(?=\s*(?:WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|'
                + _JOIN_PREFIX
                + r'JOIN|$))',
                raw_sql,
                re.IGNORECASE,
            )
            clause_idx = sql_join_idx[0] - 1   # already incremented above
            if clause_idx < len(on_matches):
                replacements.append({
                    "match":  on_matches[clause_idx].strip(),
                    "badges": [{"op_id": op_id, "badge_text": join_node_type}],
                })
            else:
                # No ON clause — join may be implicit (IN / EXISTS subquery).
                # Collect all IN/EXISTS patterns and pick by adjusted index.
                implicit_matches = re.findall(
                    r'((?:WHERE|AND|OR|HAVING)\s+\w+(?:\.\w+)?\s+IN\s*\()',
                    raw_sql, re.IGNORECASE,
                )
                implicit_matches += re.findall(
                    r'((?:WHERE|AND|OR|HAVING)\s+EXISTS\s*\()',
                    raw_sql, re.IGNORECASE,
                )
                imp_idx = clause_idx - len(on_matches)
                if implicit_matches and imp_idx < len(implicit_matches):
                    replacements.append({
                        "match":  implicit_matches[imp_idx].strip(),
                        "badges": [{"op_id": op_id, "badge_text": join_node_type}],
                    })
                elif implicit_matches:
                    # Use the first one found as a best-effort anchor
                    replacements.append({
                        "match":  implicit_matches[0].strip(),
                        "badges": [{"op_id": op_id, "badge_text": join_node_type}],
                    })

        # ── Aggregate annotation ─────────────────────────────────────────────
        elif ann_type == "aggregate":
            group_keys = detail.get("group_keys", [])
            node_type  = detail.get("node_type", "Aggregate")
            op_id      = f"aggregate_{target.replace(' ', '_')}"
            badge_text = node_type   # e.g. "HashAggregate", "GroupAggregate"

            matched = False

            if group_keys:
                # Anchor to the GROUP BY clause.
                # Build a pattern that matches GROUP BY followed by the exact
                # keys in any order with flexible whitespace / comma spacing.
                keys_pattern = r'\s*,\s*'.join(re.escape(k) for k in group_keys)
                group_by_re  = re.compile(
                    r'(GROUP\s+BY\s+' + keys_pattern + r')',
                    re.IGNORECASE,
                )
                m = group_by_re.search(raw_sql)
                if not m:
                    # Fallback: match GROUP BY up to the next clause keyword
                    m = re.search(
                        r'(GROUP\s+BY\s+(?:(?!HAVING|ORDER|LIMIT|UNION|$).)+)',
                        raw_sql, re.IGNORECASE | re.DOTALL,
                    )
                if m:
                    replacements.append({
                        "match":  m.group(1).strip(),
                        "badges": [{"op_id": op_id, "badge_text": badge_text}],
                    })
                    matched = True

                # Also anchor to HAVING if present
                having_m = re.search(
                    r'(HAVING\s+(?:(?!ORDER|LIMIT|UNION|$).)+)',
                    raw_sql, re.IGNORECASE | re.DOTALL,
                )
                if having_m:
                    replacements.append({
                        "match":  having_m.group(1).strip(),
                        "badges": [{"op_id": op_id, "badge_text": badge_text}],
                    })
                    matched = True

            else:
                # Plain aggregate (no GROUP BY).
                # 1. Try explicit aggregate function call (COUNT, SUM, …)
                agg_fn_re = re.compile(
                    r'((?:COUNT|SUM|AVG|MAX|MIN)\s*\([^)]*\))',
                    re.IGNORECASE,
                )
                m = agg_fn_re.search(raw_sql)
                if m:
                    replacements.append({
                        "match":  m.group(1),
                        "badges": [{"op_id": op_id, "badge_text": badge_text}],
                    })
                    matched = True
                else:
                    # 2. Aggregate introduced internally by PostgreSQL for an IN
                    #    subquery (semi-join deduplication).
                    #    Anchor to the same "WHERE ... IN (" fragment as the join
                    #    badge — deduplication merges both into one badge group,
                    #    avoiding the overlap / replacement-order problem.
                    in_clause_re = re.compile(
                        r'((?:WHERE|AND|OR|HAVING)\s+\w+(?:\.\w+)?\s+IN\s*\()',
                        re.IGNORECASE,
                    )
                    m2 = in_clause_re.search(raw_sql)
                    if m2:
                        replacements.append({
                            "match":  m2.group(1).strip(),
                            "badges": [{"op_id": op_id, "badge_text": badge_text}],
                        })
                        matched = True

            # Last-resort fallback
            if not matched and target and target.lower() in raw_sql.lower():
                replacements.append({
                    "match":  target,
                    "badges": [{"op_id": op_id, "badge_text": badge_text}],
                })

        # ── Hash annotation (non-leaf only) ──────────────────────────────────
        elif ann_type == "hash":
            # Leaf Hash nodes already get a badge co-located with their scan.
            # Non-leaf Hash nodes (child is a Join/Aggregate) have no table anchor,
            # so we place their badge on the nearest IN/EXISTS clause instead —
            # the dedup step will merge it with the join/aggregate badges there.
            tbl = _hash_seq.pop(0) if _hash_seq else ""
            if not tbl:
                # non-leaf hash
                hash_nl_op_id = f"hash_nl_{_hash_nl_badge_idx[0]}"
                _hash_nl_badge_idx[0] += 1
                in_clause = re.findall(
                    r'((?:WHERE|AND|OR|HAVING)\s+\w+(?:\.\w+)?\s+IN\s*\()',
                    raw_sql, re.IGNORECASE,
                )
                exists_clause = re.findall(
                    r'((?:WHERE|AND|OR|HAVING)\s+EXISTS\s*\()',
                    raw_sql, re.IGNORECASE,
                )
                anchor = (in_clause + exists_clause)
                if anchor:
                    replacements.append({
                        "match":  anchor[0].strip(),
                        "badges": [{"op_id": hash_nl_op_id, "badge_text": "Hash"}],
                    })

        # ── Everything else (sort, filter, limit, …) ─────────────────────────
        elif ann_type not in ("hash",):   # leaf Hash badges co-located with scan
            if not target or target.lower() not in raw_sql.lower():
                continue
            op_id = f"{ann_type}_{target.replace(' ', '_')}"
            replacements.append({
                "match":  target,
                "badges": [{"op_id": op_id, "badge_text": ann_type.capitalize()}],
            })

    # Deduplicate: merge badges when two annotations matched the same
    # SQL fragment, preventing the double-badge bug in _render_annotated_sql.
    seen: dict = {}
    for rep in replacements:
        key = rep['match']
        if key in seen:
            existing_ids = {b['op_id'] for b in seen[key]['badges']}
            for badge in rep['badges']:
                if badge['op_id'] not in existing_ids:
                    seen[key]['badges'].append(badge)
                    existing_ids.add(badge['op_id'])
        else:
            seen[key] = {'match': key, 'badges': list(rep['badges'])}
    return list(seen.values())


def _build_plan_comparisons(aqps: list[dict], qep_node: dict) -> tuple[str, List[dict]]:
    """Build a plan-comparison list for the Plan Comparison table."""
    qep_cost = qep_node.get("Total Cost", 0) if qep_node else 0
    comparisons: List[dict] = [
        {
            "plan_id":        "QEP-1",
            "summary":        qep_node.get("Node Type", "QEP") if qep_node else "QEP",
            "est_total_cost": qep_cost,
            "key_diff":       "Selected plan — lowest estimated total cost.",
            "details":        "Chosen QEP: default planner settings.",
        }
    ]
    for idx, aqp_entry in enumerate(aqps, start=1):
        aqp_node = aqp_entry.get("qep") or {}
        settings = aqp_entry.get("settings", {})
        comparisons.append({
            "plan_id":        f"AQP-{idx}",
            "summary":        aqp_node.get("Node Type", "AQP"),
            "est_total_cost": aqp_node.get("Total Cost", 0),
            "key_diff":       f"Settings: {settings}",
            "details":        f"Alternative plan with settings: {settings}",
        })
    return "QEP-1", comparisons


# ---------------------------------------------------------------------------
# Backend provider
# ---------------------------------------------------------------------------

def get_analysis_data(query: str) -> AnalysisData:
    """
    Call project.process_query() and translate the result into AnalysisData.
    Raises on any backend / database error — the UI handles these via
    QMessageBox so the user always sees a clear error message.
    """
    from project import process_query  # deferred import keeps UI importable standalone

    result      = process_query(query)
    qep_node    = result.get("qep") or {}
    aqps        = result.get("aqps") or []
    annotations = result.get("annotations") or []

    operator_info          = _build_operator_info(annotations, qep_node)
    qep_tree_model         = _build_qep_tree_model(qep_node, annotations)
    sql_badge_replacements = _build_sql_badge_replacements(annotations, query, qep_node)
    chosen_plan_id, plan_comparisons = _build_plan_comparisons(aqps, qep_node)

    return AnalysisData(
        raw_sql=query,
        operator_info=operator_info,
        chosen_plan_id=chosen_plan_id,
        plan_comparisons=plan_comparisons,
        sql_badge_replacements=sql_badge_replacements,
        qep_tree_model=qep_tree_model,
    )


# ---------------------------------------------------------------------------
# QEP diagram widget
# ---------------------------------------------------------------------------

class QepDiagramWidget(QWidget):
    """Visual node-link QEP tree. Emits op_id when a node is clicked."""

    nodeClicked = pyqtSignal(str)

    def __init__(self) -> None:
        super().__init__()
        self.active_op_id:  Optional[str] = None
        self.node_rects:    Dict[str, QRectF] = {}
        self.analysis_ready = False
        self.tree_model:    Optional[dict] = None
        self.setMinimumHeight(280)

    def set_analysis_ready(self, ready: bool) -> None:
        self.analysis_ready = ready
        self.update()

    def set_active(self, op_id: Optional[str]) -> None:
        self.active_op_id = op_id
        self.update()

    def set_tree_model(self, tree_model: dict) -> None:
        self.tree_model = tree_model
        self.update()

    # -- layout helpers --

    def _collect_levels_and_edges(self) -> tuple[List[List[str]], List[tuple[str, str]]]:
        ids_by_level: List[List[str]] = []
        edges: List[tuple[str, str]] = []
        if not self.tree_model:
            return ids_by_level, edges

        def walk(node: dict, depth: int) -> None:
            op_id = node["op_id"]
            while len(ids_by_level) <= depth:
                ids_by_level.append([])
            ids_by_level[depth].append(op_id)
            for child in node.get("children", []):
                edges.append((op_id, child["op_id"]))
                walk(child, depth + 1)

        walk(self.tree_model, 0)
        return ids_by_level, edges

    def _layout_rects(self, ids_by_level: List[List[str]]) -> Dict[str, QRectF]:
        w           = max(1, self.width())
        h           = max(1, self.height())
        top_margin  = 30.0
        side_margin = 40.0
        level_gap   = 26.0
        depth_count = max(1, len(ids_by_level))
        node_h      = max(66.0, min(106.0,
                          (h - top_margin * 2 - (depth_count - 1) * level_gap) / depth_count))

        rects: Dict[str, QRectF] = {}
        for depth, ids in enumerate(ids_by_level):
            if not ids:
                continue
            y           = top_margin + depth * (node_h + level_gap)
            columns     = len(ids)
            available_w = max(120.0, w - side_margin * 2)
            gap_x       = 24.0
            node_w      = min(380.0, max(160.0,
                              (available_w - (columns - 1) * gap_x) / columns))
            total_row_w = columns * node_w + (columns - 1) * gap_x
            row_start_x = (w - total_row_w) / 2.0
            for idx, op_id in enumerate(ids):
                x = row_start_x + idx * (node_w + gap_x)
                rects[op_id] = QRectF(x, y, node_w, node_h)
        return rects

    def _collect_node_labels(self) -> Dict[str, str]:
        labels: Dict[str, str] = {}
        if not self.tree_model:
            return labels

        def walk(node: dict) -> None:
            op_id  = str(node.get("op_id", "unknown"))
            label  = str(node.get("label", op_id))
            cost   = str(node.get("cost",  "N/A"))
            labels[op_id] = f"Operator: {label}\nCost: {cost}"
            for child in node.get("children", []):
                if isinstance(child, dict):
                    walk(child)

        walk(self.tree_model)
        return labels

    # -- paint --

    def paintEvent(self, _event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.fillRect(self.rect(), QColor("#f8fbff"))

        if not self.analysis_ready:
            painter.setPen(QColor("#64748b"))
            painter.setFont(QFont("Segoe UI", 11))
            painter.drawText(
                self.rect(), Qt.AlignmentFlag.AlignCenter,
                "Run Analyse to generate and visualize the QEP tree.",
            )
            return

        if not self.tree_model:
            painter.setPen(QColor("#64748b"))
            painter.setFont(QFont("Segoe UI", 11))
            painter.drawText(self.rect(), Qt.AlignmentFlag.AlignCenter, "No QEP model loaded.")
            return

        ids_by_level, edges = self._collect_levels_and_edges()
        node_labels         = self._collect_node_labels()
        self.node_rects     = self._layout_rects(ids_by_level)

        edge_pen = QPen(QColor("#7ea3d4"), 2)
        painter.setPen(edge_pen)
        for parent_id, child_id in edges:
            pr = self.node_rects.get(parent_id)
            cr = self.node_rects.get(child_id)
            if pr and cr:
                self._draw_edge(painter, pr.center(), cr.center())

        for level in ids_by_level:
            for op_id in level:
                self._draw_node(painter, op_id, node_labels.get(op_id, op_id))

    def _draw_edge(self, painter: QPainter, start: QPointF, end: QPointF) -> None:
        painter.drawLine(start, end)
        painter.drawLine(end, QPointF(end.x() - 7.0, end.y() - 5.0))
        painter.drawLine(end, QPointF(end.x() + 1.0, end.y() - 8.0))

    def _draw_node(self, painter: QPainter, op_id: str, text: str) -> None:
        rect      = self.node_rects[op_id]
        is_active = self.active_op_id == op_id
        fill      = QColor("#bfdbfe") if is_active else QColor("#eef5ff")
        border    = QColor("#1d4ed8") if is_active else QColor("#8db0df")
        width     = 4 if is_active else 2
        painter.setPen(QPen(border, width))
        painter.setBrush(fill)
        painter.drawRoundedRect(rect, 20, 20)
        painter.setPen(QColor("#1f2937"))
        text_flags = Qt.AlignmentFlag.AlignCenter | Qt.TextFlag.TextWordWrap
        target_rect = rect.toRect().adjusted(10, 8, -10, -8)

        # Keep node labels readable on smaller, non-fullscreen windows by
        # shrinking the font until the wrapped text fits the node.
        max_size = 12.0 if self.window().isFullScreen() else 10.5
        min_size = 8.0
        font = QFont("Segoe UI", weight=QFont.Weight.DemiBold)
        chosen_size = min_size
        size = max_size
        while size >= min_size:
            font.setPointSizeF(size)
            bounds = painter.fontMetrics().boundingRect(target_rect, int(text_flags), text)
            if bounds.width() <= target_rect.width() and bounds.height() <= target_rect.height():
                chosen_size = size
                break
            size -= 0.5

        font.setPointSizeF(chosen_size)
        painter.setFont(font)
        painter.drawText(target_rect, int(text_flags), text)

    def mousePressEvent(self, event) -> None:
        pos = event.position()
        for op_id, rect in self.node_rects.items():
            if rect.contains(pos):
                self.nodeClicked.emit(op_id)
                return
        super().mousePressEvent(event)


# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------

class SqlQepComprehensionUI(QMainWindow):
    """
    PyQt6 desktop UI for Query Plan-Based SQL Comprehension.

    Features
    --------
    - 3-panel layout: annotated SQL | QEP tree | explanation cards
    - Bidirectional linking between SQL annotations and QEP nodes
    - Calls project.process_query() via get_analysis_data(); any exception
      surfaces as a QMessageBox so the user always sees a clear error.
    """

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("SQL Query Plan Comprehension")
        self.resize(1320, 860)
        self.setMinimumSize(1024, 680)

        # These are populated by _apply_analysis_data(); declared here for clarity.
        self.analysis_data:          AnalysisData
        self.raw_sql:                str = ""
        self.operator_info:          Dict[str, OperatorInfo] = {}
        self.chosen_plan_id:         str = ""
        self.plan_comparisons:       List[dict] = []
        self.sql_badge_replacements: List[dict] = []
        self.qep_tree_model:         dict = {}

        self.tree_items_by_op:     Dict[str, QTreeWidgetItem] = {}
        self.current_active_op_id: Optional[str] = None

        self._build_ui()
        self._set_input_mode()
        self.statusBar().showMessage("Ready. Paste SQL and click Analyse.")

    # -----------------------------------------------------------------------
    # UI construction
    # -----------------------------------------------------------------------

    def _build_ui(self) -> None:
        self._apply_styles()
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(12, 12, 12, 10)
        root_layout.setSpacing(10)

        header = QLabel("Query Plan-Based SQL Comprehension")
        header.setObjectName("Header")
        subtitle = QLabel("Click annotated SQL or QEP node to view synchronised explanation.")
        subtitle.setObjectName("Subtitle")
        root_layout.addWidget(header)
        root_layout.addWidget(subtitle)

        main_splitter = QSplitter(Qt.Orientation.Vertical)
        root_layout.addWidget(main_splitter, 1)

        # ---- top row: SQL panel | QEP panel ----
        top_splitter = QSplitter(Qt.Orientation.Horizontal)
        main_splitter.addWidget(top_splitter)

        # Left: SQL input / annotated view
        left_panel  = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(10, 10, 10, 10)
        left_layout.setSpacing(8)

        controls = QHBoxLayout()
        controls.setSpacing(8)
        self.analyse_btn = QPushButton("Analyse")
        self.reset_btn   = QPushButton("Reset")
        self.analyse_btn.clicked.connect(self._handle_analyse_clicked)
        self.reset_btn.clicked.connect(self._handle_reset_clicked)
        controls.addWidget(self.analyse_btn)
        controls.addWidget(self.reset_btn)
        controls.addStretch(1)
        left_layout.addLayout(controls)

        self.sql_stack = QStackedWidget()

        self.sql_input_editor = QPlainTextEdit()
        self.sql_input_editor.setFont(QFont("Courier New", 13))
        self.sql_input_editor.setPlaceholderText("Paste your SQL query here and click Analyse…")
        self.sql_stack.addWidget(self.sql_input_editor)

        self.annotated_sql_view = QTextBrowser()
        self.annotated_sql_view.setOpenLinks(False)
        self.annotated_sql_view.setOpenExternalLinks(False)
        self.annotated_sql_view.anchorClicked.connect(self._handle_sql_anchor_clicked)
        self.annotated_sql_view.setFont(QFont("Courier New", 13))
        self.sql_stack.addWidget(self.annotated_sql_view)

        left_layout.addWidget(self._panel_title("Query / Annotated SQL"))
        left_layout.addWidget(self.sql_stack, 1)
        top_splitter.addWidget(left_panel)

        # Right: QEP tree tabs
        right_panel  = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(10, 10, 10, 10)
        right_layout.setSpacing(8)
        right_layout.addWidget(self._panel_title("QEP Tree"))

        self.qep_tabs = QTabWidget()

        self.qep_tree = QTreeWidget()
        self.qep_tree.setHeaderLabels(["Operator", "Estimated Cost"])
        self.qep_tree.itemClicked.connect(self._handle_tree_item_clicked)
        self.qep_tree.setRootIsDecorated(True)
        self.qep_tree.setItemsExpandable(True)
        self.qep_tree.setExpandsOnDoubleClick(True)
        self.qep_tree.setIndentation(26)
        self.qep_tree.setFont(QFont("Segoe UI", 12))
        self.qep_tabs.addTab(self.qep_tree, "Tree Widget")

        self.qep_diagram = QepDiagramWidget()
        self.qep_diagram.nodeClicked.connect(self._handle_diagram_node_clicked)
        self.qep_tabs.addTab(self.qep_diagram, "Visual Tree")
        self.qep_tabs.setCurrentIndex(1)

        right_layout.addWidget(self.qep_tabs, 1)
        top_splitter.addWidget(right_panel)
        top_splitter.setSizes([700, 560])

        # ---- bottom: explanation panel ----
        bottom_panel  = QWidget()
        bottom_layout = QVBoxLayout(bottom_panel)
        bottom_layout.setContentsMargins(10, 10, 10, 10)
        bottom_layout.setSpacing(8)
        bottom_layout.addWidget(self._panel_title("Explanation"))

        self.selected_operator_label = QLabel("Operator: (none selected)")
        self.selected_operator_label.setObjectName("SelectedOperator")
        bottom_layout.addWidget(self.selected_operator_label)

        self.explain_mode_tabs = QTabWidget()
        self.explain_mode_tabs.setFixedHeight(28)
        self.explain_mode_tabs.addTab(QWidget(), "Operator Explanation")
        self.explain_mode_tabs.addTab(QWidget(), "Plan Comparison")
        self.explain_mode_tabs.currentChanged.connect(self._on_explain_mode_changed)
        bottom_layout.addWidget(self.explain_mode_tabs)

        cards_row = QHBoxLayout()
        cards_row.setSpacing(10)
        self.what_card, self.what_card_body = self._create_explain_card("WHAT (Execution)")
        self.why_card,  self.why_card_body  = self._create_explain_card("WHY (Decision)")
        self.alt_card,  self.alt_card_body  = self._create_explain_card("ALTERNATIVES (Comparison / AQP)")
        self.what_card.setObjectName("WhatCard")
        self.why_card.setObjectName("WhyCard")
        self.alt_card.setObjectName("AltCard")
        cards_row.addWidget(self.what_card, 1)
        cards_row.addWidget(self.why_card,  1)
        cards_row.addWidget(self.alt_card,  1)
        bottom_layout.addLayout(cards_row, 1)

        # Plan comparison table lives inside the Alternatives card
        self.plan_table = QTableWidget()
        self.plan_table.setColumnCount(4)
        self.plan_table.setHorizontalHeaderLabels(
            ["Rank", "Plan ID", "Estimated Cost", "Plan Summary"]
        )
        self.plan_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.plan_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.plan_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.plan_table.cellClicked.connect(self._handle_plan_row_clicked)
        self.plan_table.verticalHeader().setVisible(False)
        self.plan_table.horizontalHeader().setStretchLastSection(True)

        alt_layout = self.alt_card.layout()
        if alt_layout is not None:
            alt_layout.addWidget(self.plan_table, 1)

        self._refresh_explanation_ui()
        main_splitter.addWidget(bottom_panel)
        main_splitter.setSizes([560, 240])
        self.setStatusBar(QStatusBar())

    def _apply_styles(self) -> None:
        self.setStyleSheet("""
            QMainWindow { background: #f3f6fb; }
            QLabel#Header   { font-size: 26px; font-weight: 700; color: #0f172a; }
            QLabel#Subtitle { color: #334155; margin-bottom: 4px; font-size: 13px; }
            QLabel#SelectedOperator {
                font-size: 13px; font-weight: 700; color: #1e3a8a;
                background: #e8f0ff; border: 1px solid #b8ccf3;
                border-radius: 8px; padding: 6px 10px;
            }
            QLabel[role="panelTitle"] { font-size: 16px; font-weight: 700; color: #1e293b; }
            QSplitter { background: transparent; border: none; }
            QStatusBar { background: #eef2f8; border-top: 1px solid #dbe3ef; font-size: 12px; }
            QStackedWidget, QTextEdit, QTreeWidget, QPlainTextEdit, QTextBrowser {
                background: #ffffff; border: 1px solid #dbe3ef; border-radius: 10px;
            }
            QPlainTextEdit, QTextBrowser, QTextEdit, QTreeWidget {
                border: 1px solid #cfd9e8; border-radius: 8px; background: #fbfdff;
                color: #0f172a; padding: 6px; font-size: 13px;
            }
            QFrame[role="explainCard"] {
                border: 1px solid #d1ddf0; border-radius: 10px; background: #f8fbff;
            }
            QFrame#WhatCard { background: #eaf3ff; border: 1px solid #bfd6ff; }
            QFrame#WhyCard  { background: #fff8de; border: 1px solid #efd78b; }
            QFrame#AltCard  { background: #eaf9ef; border: 1px solid #b8e2c7; }
            QLabel[role="explainCardTitle"] {
                font-size: 15px; font-weight: 700; color: #1e40af; margin-bottom: 4px;
            }
            QLabel[role="explainCardBody"] { font-size: 13px; color: #1f2937; line-height: 1.55; }
            QTreeWidget::item { height: 30px; padding: 4px 8px; }
            QTreeWidget::item:selected {
                background: #bfdbfe; color: #111827;
                border: 1px solid #2563eb; font-weight: 700;
            }
            QTreeWidget::item:selected:active { background: #93c5fd; color: #111827; }
            QPushButton {
                background: #1d4ed8; color: #ffffff; border: none;
                border-radius: 8px; padding: 8px 14px;
                font-weight: 600; font-size: 13px;
            }
            QPushButton:hover { background: #1e40af; }
        """)

    def _panel_title(self, text: str) -> QLabel:
        lbl = QLabel(text)
        lbl.setProperty("role", "panelTitle")
        return lbl

    def _create_explain_card(self, title: str) -> tuple[QFrame, QLabel]:
        card = QFrame()
        card.setProperty("role", "explainCard")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(6)
        title_lbl = QLabel(title)
        title_lbl.setProperty("role", "explainCardTitle")
        body_lbl = QLabel("Select a SQL annotation or QEP node.")
        body_lbl.setProperty("role", "explainCardBody")
        body_lbl.setWordWrap(True)
        body_lbl.setTextFormat(Qt.TextFormat.RichText)
        layout.addWidget(title_lbl)
        layout.addWidget(body_lbl, 1)
        return card, body_lbl

    # -----------------------------------------------------------------------
    # Data binding
    # -----------------------------------------------------------------------

    def _apply_analysis_data(self, data: AnalysisData) -> None:
        """Bind one AnalysisData payload to all UI widgets."""
        self.analysis_data           = data
        self.raw_sql                 = data.raw_sql
        self.operator_info           = data.operator_info
        self.chosen_plan_id          = data.chosen_plan_id
        self.plan_comparisons        = data.plan_comparisons
        self.sql_badge_replacements  = data.sql_badge_replacements
        self.qep_tree_model          = data.qep_tree_model

        self.qep_diagram.set_tree_model(self.qep_tree_model)
        self._load_qep_tree_from_model()
        self.plan_table.setRowCount(0)   # force fresh render next time Plan Comparison opens

        self.current_active_op_id = None
        self.qep_tree.clearSelection()
        self.qep_diagram.set_active(None)
        self.selected_operator_label.setText("Operator: (none selected)")
        self._refresh_explanation_ui()

    def _load_qep_tree_from_model(self) -> None:
        self.qep_tree.clear()
        self.tree_items_by_op.clear()
        model = self.qep_tree_model if isinstance(self.qep_tree_model, dict) else {}
        if not model:
            self.qep_diagram.update()
            return

        def build_item(node_model: dict) -> QTreeWidgetItem:
            label = str(node_model.get("label", "Unknown Operator"))
            cost  = str(node_model.get("cost",  "N/A"))
            op_id = str(node_model.get("op_id", f"unknown_{id(node_model)}"))
            item  = QTreeWidgetItem([label, cost])
            item.setData(0, Qt.ItemDataRole.UserRole, op_id)
            self.tree_items_by_op[op_id] = item
            for child_model in node_model.get("children", []):
                if isinstance(child_model, dict):
                    item.addChild(build_item(child_model))
            return item

        root_item = build_item(model)
        self.qep_tree.addTopLevelItem(root_item)
        root_item.setExpanded(True)
        self.qep_tree.expandAll()
        self.qep_diagram.update()

    # -----------------------------------------------------------------------
    # Mode switching
    # -----------------------------------------------------------------------

    def _set_input_mode(self) -> None:
        self.sql_stack.setCurrentWidget(self.sql_input_editor)
        self.sql_input_editor.setReadOnly(False)
        self.current_active_op_id = None
        # Reset QEP tree widgets back to an empty state.
        # Disabling selection alone leaves stale items visible after reset.
        self.qep_tree.clear()
        self.tree_items_by_op.clear()
        self.qep_tree.setEnabled(False)
        self.qep_diagram.set_analysis_ready(False)
        self.qep_diagram.set_active(None)
        self.selected_operator_label.setText("Operator: (none selected)")
        self._refresh_explanation_ui()

    def _set_annotated_mode(self) -> None:
        self.sql_stack.setCurrentWidget(self.annotated_sql_view)

    # -----------------------------------------------------------------------
    # Explanation panel
    # -----------------------------------------------------------------------

    def _on_explain_mode_changed(self, _index: int) -> None:
        self._refresh_explanation_ui()

    def _refresh_explanation_ui(self) -> None:
        is_plan_mode = self.explain_mode_tabs.currentIndex() == 1

        if is_plan_mode:
            self.selected_operator_label.setVisible(False)
            self.what_card.setVisible(False)
            self.why_card.setVisible(False)
            self.alt_card.setVisible(True)
            if not self.qep_diagram.analysis_ready:
                self.alt_card_body.setText("Run Analyse to generate plan comparison (AQP).")
                self.plan_table.setVisible(False)
                return
            self.plan_table.setVisible(True)
            if self.plan_table.rowCount() == 0:
                self._populate_plan_comparison_table()
        else:
            self.selected_operator_label.setVisible(True)
            self.what_card.setVisible(True)
            self.why_card.setVisible(True)
            self.alt_card.setVisible(False)
            self.plan_table.setVisible(False)

            info = self.operator_info.get(self.current_active_op_id) if self.current_active_op_id else None
            if not info:
                self.what_card_body.setText("Select an operator to see execution details.")
                self.why_card_body.setText("WHY will appear once an operator is selected.")
                return
            self.what_card_body.setText(html.escape(info.what))
            self.why_card_body.setText(html.escape(info.why))

    def _populate_plan_comparison_table(self) -> None:
        rows = sorted(self.plan_comparisons, key=lambda p: float(p["est_total_cost"]))
        self.plan_table.setRowCount(len(rows))
        self.plan_table.setSortingEnabled(False)
        chosen = self.chosen_plan_id
        for idx, plan in enumerate(rows):
            is_chosen   = plan["plan_id"] == chosen
            plan_id_txt = plan["plan_id"] + ("  ✅ Selected Plan" if is_chosen else "")
            items = [
                QTableWidgetItem(str(idx + 1)),
                QTableWidgetItem(plan_id_txt),
                QTableWidgetItem(f"{float(plan['est_total_cost']):.2f}"),
                QTableWidgetItem(plan["summary"]),
            ]
            for c, item in enumerate(items):
                if is_chosen:
                    item.setBackground(QColor("#d1fae5"))
                    item.setForeground(QColor("#065f46"))
                    item.setFont(QFont("Segoe UI", 10, QFont.Weight.DemiBold))
                self.plan_table.setItem(idx, c, item)
        if rows:
            self.plan_table.selectRow(0)
        self._fit_plan_table_height()

    def _handle_plan_row_clicked(self, _row: int, _col: int) -> None:
        pass  # reserved for future per-row detail expansion

    def _fit_plan_table_height(self) -> None:
        if self.plan_table.rowCount() == 0:
            return
        header_height = self.plan_table.horizontalHeader().height()
        self.plan_table.setFixedHeight(
            header_height + self.plan_table.rowCount() * 28 + 8
        )

    # -----------------------------------------------------------------------
    # Annotated SQL rendering
    # -----------------------------------------------------------------------

    def _render_annotated_sql(self, active_op_id: Optional[str]) -> None:
        sql_html = html.escape(self.raw_sql)
        for rep in self.sql_badge_replacements:
            escaped_match = html.escape(rep["match"])
            badge_parts   = [
                f"<a class='sql-tag' href='op:///{b['op_id']}' "
                f"style='{self._badge_style(active_op_id == b['op_id'])}'>"
                f"[{b['badge_text']}]</a>"
                for b in rep.get("badges", [])
            ]
            if badge_parts:
                sql_html = sql_html.replace(
                    escaped_match,
                    f"{escaped_match}&nbsp;&nbsp;" + "&nbsp;".join(badge_parts),
                )

        self.annotated_sql_view.setHtml(
            "<style>"
            "a.sql-tag{text-decoration:none;transition:all 120ms ease-in-out;}"
            "a.sql-tag:hover{filter:brightness(0.95);box-shadow:0 0 0 2px rgba(59,130,246,0.2);}"
            "</style>"
            "<div style='font-family:Courier New,Consolas,monospace;font-size:14px;"
            "line-height:1.65;color:#0f172a;'>"
            "<div style='margin-bottom:8px;color:#475569;font-family:Segoe UI,Arial;font-size:12px;'>"
            "Click a blue badge to focus the matching operator in QEP.</div>"
            f"<pre style='white-space:pre-wrap;margin:0;'>{sql_html}</pre>"
            "</div>"
        )

    def _badge_style(self, active: bool) -> str:
        if active:
            return (
                "text-decoration:none;color:#0f172a;background:#93c5fd;"
                "border:1px solid #2563eb;border-radius:8px;padding:3px 8px;"
                "font-weight:700;box-shadow:inset 0 0 0 1px rgba(37,99,235,0.18);"
            )
        return (
            "text-decoration:none;color:#1e3a8a;background:#d0e7ff;"
            "border:1px solid #7fb0ef;border-radius:8px;padding:3px 8px;font-weight:600;"
        )

    # -----------------------------------------------------------------------
    # Event handlers / bidirectional sync
    # -----------------------------------------------------------------------

    def _handle_analyse_clicked(self) -> None:
        query = self.sql_input_editor.toPlainText().strip()
        if not query:
            QMessageBox.warning(self, "No Query", "Please enter a SQL query before analysing.")
            return

        self.analyse_btn.setEnabled(False)
        self.statusBar().showMessage("Analysing…")
        QApplication.processEvents()

        try:
            fresh_data = get_analysis_data(query)
        except Exception as exc:
            QMessageBox.critical(
                self, "Analysis Failed",
                f"The backend returned an error:\n\n{exc}\n\n"
                "Check that PostgreSQL is running and PGPASSWORD is set.",
            )
            self.analyse_btn.setEnabled(True)
            self.statusBar().showMessage("Analysis failed — see error dialog.")
            return

        self._apply_analysis_data(fresh_data)
        self.sql_input_editor.setPlainText(self.raw_sql)
        self._render_annotated_sql(active_op_id=None)
        self._set_annotated_mode()
        self.qep_tree.setEnabled(True)
        self.qep_diagram.set_analysis_ready(True)
        self.qep_diagram.set_active(None)
        self.analyse_btn.setEnabled(True)
        self.statusBar().showMessage("Analysis complete. Click an annotation or QEP node.")

    def _handle_reset_clicked(self) -> None:
        self.sql_input_editor.setPlainText(self.raw_sql)
        self._set_input_mode()
        self.qep_tree.clearSelection()
        self.statusBar().showMessage("Reset to SQL input mode.")

    def _handle_sql_anchor_clicked(self, url: QUrl) -> None:
        op_id = url.path().lstrip("/")
        if op_id:
            self._set_active_operator(op_id, source="sql")

    def _handle_tree_item_clicked(self, item: QTreeWidgetItem) -> None:
        op_id = item.data(0, Qt.ItemDataRole.UserRole)
        if op_id:
            self._set_active_operator(str(op_id), source="tree")

    def _handle_diagram_node_clicked(self, op_id: str) -> None:
        self._set_active_operator(op_id, source="diagram")

    def _set_active_operator(self, op_id: str, source: str) -> None:
        self.current_active_op_id = op_id
        self._highlight_tree_node(op_id)
        self.qep_diagram.set_active(op_id)
        self._render_annotated_sql(active_op_id=op_id)
        self._update_explanation_panel(op_id)
        self.statusBar().showMessage(f"Selected {op_id} via {source}.")

    def _highlight_tree_node(self, op_id: str) -> None:
        item = self.tree_items_by_op.get(op_id)
        if item:
            self.qep_tree.setCurrentItem(item)
            self.qep_tree.scrollToItem(item)

    def _update_explanation_panel(self, op_id: str) -> None:
        info = self.operator_info.get(op_id)
        self.selected_operator_label.setText(
            f"Operator: {info.name}" if info else "Operator: (unknown)"
        )
        self._refresh_explanation_ui()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    app = QApplication(sys.argv)
    app.setFont(QFont("Segoe UI", 10))
    window = SqlQepComprehensionUI()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()