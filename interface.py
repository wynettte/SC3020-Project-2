# imports
import html
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional

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

# check class for operator info
@dataclass(frozen=True)
class OperatorInfo:
    name: str
    what: str
    why: str
    alternatives: str
    impact: str


@dataclass(frozen=True)
class AnalysisData:
    """
    Canonical data contract used by the UI.

    The UI should only consume this structure. To switch from mock data to backend
    data later, you only need to change the data-provider functions below.
    """

    raw_sql: str
    operator_info: Dict[str, OperatorInfo]
    chosen_plan_id: str
    plan_comparisons: List[dict]
    sql_badge_replacements: List[dict]
    qep_tree_model: dict


# =========================
# Mock data providers (UI-only for now)
# =========================
# TODO (backend integration): Replace the bodies of these functions with calls to backend/API (e.g., PostgreSQL EXPLAIN parser) and return the same shapes.


#1 default SQL input
def get_mock_default_raw_sql() -> str:
    # Currently using mock SQL input.
    # TODO: Replace with user-provided SQL (already done via QPlainTextEdit).
    return (
        "SELECT *\n"
        "FROM customer C\n"
        "JOIN orders O ON C.c_custkey = O.o_custkey;"
    )

#2 source for operator info (dictionary from wynette)
def get_mock_operator_info() -> Dict[str, OperatorInfo]:
    # Currently using mock operator explanations.
    # TODO: Replace with backend mapping from QEP operator -> explanation 
    return {
        "hash_join": OperatorInfo(
            name="Hash Join",
            what="Joins rows by building a hash table on one input and probing with the other.",
            why="Estimated cheaper than Nested Loop/Merge Join for larger joined inputs.",
            alternatives="Nested Loop (higher probe cost), Merge Join (needs sorting).",
            impact="Good for equi-joins on larger sets without strong ordering/index support.",
        ),
        "seq_scan_customer": OperatorInfo(
            name="Seq Scan (customer)",
            what="Reads customer table row-by-row.",
            why="No useful index available for this access path in the mock setup.",
            alternatives="Index Scan if a selective index exists.",
            impact="More I/O than indexed lookup, but can be cheapest when many rows are needed.",
        ),
        "seq_scan_orders": OperatorInfo(
            name="Seq Scan (orders)",
            what="Reads orders table row-by-row.",
            why="Planner estimates full scan is cheaper than indexed random access here.",
            alternatives="Index Scan / Bitmap Scan if selective predicates exist.",
            impact="Predictable throughput; may cost more on very large tables.",
        ),
    }


#3 mapping SQL substrings to operator
def get_mock_sql_badge_replacements() -> List[dict]:
    """
    MOCK: Mapping from SQL substrings to clickable operator badges.

    Each item:
      - match: exact substring from the raw SQL that will be replaced
      - badges: list of {op_id, badge_text} that will be appended as pills

    TODO (backend integration): Replace this with output from your annotate_query()
    so that badges are positioned/matched based on real SQL clause/span alignment.
    """
    return [
        {
            "match": "FROM customer C",
            "badges": [{"op_id": "seq_scan_customer", "badge_text": "Seq Scan"}],
        },
        {
            "match": "JOIN orders O ON C.c_custkey = O.o_custkey;",
            "badges": [
                {"op_id": "hash_join", "badge_text": "Hash Join"},
                {"op_id": "seq_scan_orders", "badge_text": "Seq Scan"},
            ],
        },
    ]

#4 QEP tree model (Natalie)
def get_mock_qep_tree_model() -> dict:
    """
    MOCK: QEP tree model used to populate the QTreeWidget.

    TODO (backend integration): Replace this with parsed QEP JSON / plan nodes.
    Required fields:
      - op_id: stable key used for linking to explanations
      - label: node text (operator + maybe relation)
      - cost: string to show in the 2nd column
      - children: list of child nodes
    """
    return {
        "op_id": "hash_join",
        "label": "Hash Join",
        "cost": "120.00..455.10",
        "children": [
            {
                "op_id": "seq_scan_customer",
                "label": "Seq Scan (customer)",
                "cost": "0.00..80.00",
                "children": [],
            },
            {
                "op_id": "seq_scan_orders",
                "label": "Seq Scan (orders)",
                "cost": "0.00..180.00",
                "children": [],
            },
        ],
    }

#5 AQP generated plans (Fang Yi)
def get_mock_plan_comparisons() -> tuple[str, List[dict]]:
    # Currently using mock AQP comparison data for Plan Comparison mode.
    # TODO: Replace with backend-generated alternative plans and their estimated total costs.
    chosen_plan_id = "QEP-1"
    comparisons = [
        {
            "plan_id": "QEP-1",
            "summary": "Hash Join (equi-join) + Seq Scans",
            "est_total_cost": 455.10,
            "key_diff": "Selected because it has the lowest estimated total cost in this mock comparison.",
            "details": (
                "Chosen QEP: build hash on one input and probe the other.\n"
                "Main operators: Seq Scan + Hash Join.\n"
                "Rationale: cheaper than repeated probes (Nested Loop) and cheaper than sorting overhead (Merge Join)."
            ),
        },
        {
            "plan_id": "AQP-1",
            "summary": "Nested Loop + Seq Scans",
            "est_total_cost": 1210.33,
            "key_diff": "Higher cost due to repeated probing and many row comparisons.",
            "details": (
                "Alternative: Nested Loop with Seq Scan.\n"
                "Why worse: repeated scans/probes dominate estimated cost for this distribution."
            ),
        },
        {
            "plan_id": "AQP-2",
            "summary": "Merge Join + Sort",
            "est_total_cost": 825.47,
            "key_diff": "Requires sorting (or ordered inputs), increasing total cost.",
            "details": (
                "Alternative: Merge Join.\n"
                "Why worse: sort/ordering cost adds overhead beyond the chosen hash strategy."
            ),
        },
    ]
    return chosen_plan_id, comparisons


def get_mock_analysis_data(query: str) -> AnalysisData:
    """
    Single mock entry point that returns everything the UI needs.

    TODO (backend integration):
      Replace this function with real analysis output from your backend/API.
      Keep the return shape (AnalysisData) unchanged so UI code does not change.
    """
    # For now we keep query if user typed something, otherwise use a default.
    raw_sql = query.strip() or get_mock_default_raw_sql()
    chosen_plan_id, plan_comparisons = get_mock_plan_comparisons()
    return AnalysisData(
        raw_sql=raw_sql,
        operator_info=get_mock_operator_info(),
        chosen_plan_id=chosen_plan_id,
        plan_comparisons=plan_comparisons,
        sql_badge_replacements=get_mock_sql_badge_replacements(),
        qep_tree_model=get_mock_qep_tree_model(),
    )


def get_analysis_data(query: str) -> AnalysisData:
    """
    Generic data provider used by the UI.
    Change only this function (or called functions) when moving to backend data.
    """
    return get_mock_analysis_data(query)


class QepDiagramWidget(QWidget):
    """
    Visual node-link QEP tree (like diagram-style tree).
    Emits op_id when a node is clicked.
    """

    nodeClicked = pyqtSignal(str)

    def __init__(self) -> None:
        super().__init__()
        self.active_op_id: Optional[str] = None
        self.node_rects: Dict[str, QRectF] = {}
        self.analysis_ready = False
        self.tree_model: Optional[dict] = None
        self.setMinimumHeight(280)

    def set_analysis_ready(self, ready: bool) -> None:
        self.analysis_ready = ready
        self.update()

    def set_active(self, op_id: Optional[str]) -> None:
        self.active_op_id = op_id
        self.update()

    def set_tree_model(self, tree_model: dict) -> None:
        """
        Set full QEP tree model for diagram rendering.

        This removes hardcoded operator IDs and allows arbitrary tree shapes.
        """
        self.tree_model = tree_model
        self.update()

    def _collect_levels_and_edges(self) -> tuple[List[List[str]], List[tuple[str, str]]]:
        """
        Traverse self.tree_model and produce:
          - ids_by_level: op_ids grouped by depth
          - edges: list of (parent_op_id, child_op_id)
        """
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
        """
        Compute node rectangles for arbitrary tree depth.
        """
        w = max(1, self.width())
        h = max(1, self.height())
        top_margin = 30.0
        side_margin = 40.0
        level_gap = 26.0

        depth_count = max(1, len(ids_by_level))
        node_h = max(66.0, min(106.0, (h - top_margin * 2 - (depth_count - 1) * level_gap) / depth_count))

        rects: Dict[str, QRectF] = {}
        for depth, ids in enumerate(ids_by_level):
            if not ids:
                continue
            y = top_margin + depth * (node_h + level_gap)
            columns = len(ids)
            available_w = max(120.0, w - side_margin * 2)
            gap_x = 24.0
            node_w = min(380.0, max(160.0, (available_w - (columns - 1) * gap_x) / columns))
            total_row_w = columns * node_w + (columns - 1) * gap_x
            row_start_x = (w - total_row_w) / 2.0

            for idx, op_id in enumerate(ids):
                x = row_start_x + idx * (node_w + gap_x)
                rects[op_id] = QRectF(x, y, node_w, node_h)
        return rects

    def paintEvent(self, _event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.fillRect(self.rect(), QColor("#f8fbff"))

        if not self.analysis_ready:
            painter.setPen(QColor("#64748b"))
            painter.setFont(QFont("Segoe UI", 11))
            painter.drawText(
                self.rect(),
                Qt.AlignmentFlag.AlignCenter,
                "Run Analyse to generate and visualize the QEP tree.",
            )
            return

        if not self.tree_model:
            painter.setPen(QColor("#64748b"))
            painter.setFont(QFont("Segoe UI", 11))
            painter.drawText(
                self.rect(),
                Qt.AlignmentFlag.AlignCenter,
                "No QEP model loaded.",
            )
            return

        ids_by_level, edges = self._collect_levels_and_edges()
        node_labels = self._collect_node_labels()
        self.node_rects = self._layout_rects(ids_by_level)

        # Draw edges first
        edge_pen = QPen(QColor("#7ea3d4"), 2)
        painter.setPen(edge_pen)
        for parent_id, child_id in edges:
            parent_rect = self.node_rects.get(parent_id)
            child_rect = self.node_rects.get(child_id)
            if parent_rect and child_rect:
                self._draw_curved_edge(painter, parent_rect.center(), child_rect.center())

        # Draw nodes (data-driven)
        for level in ids_by_level:
            for op_id in level:
                text = node_labels.get(op_id, op_id)
                self._draw_node(painter, op_id, text)

    def _collect_node_labels(self) -> Dict[str, str]:
        labels: Dict[str, str] = {}
        if not self.tree_model:
            return labels

        def walk(node: dict) -> None:
            op_id = str(node.get("op_id", "unknown"))
            label = str(node.get("label", op_id))
            cost = str(node.get("cost", "N/A"))
            labels[op_id] = f"Operator: {label}\nCost: {cost}"
            for child in node.get("children", []):
                if isinstance(child, dict):
                    walk(child)

        walk(self.tree_model)
        return labels

    def _draw_curved_edge(self, painter: QPainter, start: QPointF, end: QPointF) -> None:
        painter.drawLine(start, end)
        # simple arrow head
        painter.drawLine(end, QPointF(end.x() - 7.0, end.y() - 5.0))
        painter.drawLine(end, QPointF(end.x() + 1.0, end.y() - 8.0))

    def _draw_node(self, painter: QPainter, op_id: str, text: str) -> None:
        rect = self.node_rects[op_id]
        is_active = self.active_op_id == op_id
        fill = QColor("#bfdbfe") if is_active else QColor("#eef5ff")
        border = QColor("#1d4ed8") if is_active else QColor("#8db0df")
        width = 4 if is_active else 2
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


class SqlQepComprehensionUI(QMainWindow):
    """
    PyQt desktop UI for Query Plan-Based SQL Comprehension (mock data only).
    Features:
      - 3-panel layout (annotated SQL, QEP tree, explanation panel)
      - Bidirectional linking between SQL annotations and QEP nodes
      - Analyse / Reset workflow
    """

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("SQL Query Plan Comprehension")
        self.resize(1320, 860)
        self.setMinimumSize(1024, 680)

        # -------------------------
        # Data model (loaded through a single provider function)
        # -------------------------
        # IMPORTANT:
        # To switch from mock data to backend data later, change only get_analysis_data().
        self.analysis_data = get_analysis_data("")
        self.raw_sql = self.analysis_data.raw_sql
        self.operator_info = self.analysis_data.operator_info
        self.chosen_plan_id = self.analysis_data.chosen_plan_id
        self.plan_comparisons = self.analysis_data.plan_comparisons
        self.sql_badge_replacements = self.analysis_data.sql_badge_replacements
        self.qep_tree_model = self.analysis_data.qep_tree_model

        self.tree_items_by_op: Dict[str, QTreeWidgetItem] = {}
        self.current_active_op_id: Optional[str] = None

        self._build_ui()
        # -------------------------
        # Populate widgets from provider data
        # -------------------------
        self._apply_analysis_data(self.analysis_data)
        self._set_input_mode()
        self.statusBar().showMessage("Ready. Paste SQL and click Analyse.")

    def _build_ui(self) -> None:
        self._apply_styles()
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(12, 12, 12, 10)
        root_layout.setSpacing(10)

        header = QLabel("Query Plan-Based SQL Comprehension")
        header.setObjectName("Header")
        subtitle = QLabel("Click annotated SQL or QEP node to view synchronized explanation.")
        subtitle.setObjectName("Subtitle")
        root_layout.addWidget(header)
        root_layout.addWidget(subtitle)

        main_splitter = QSplitter(Qt.Orientation.Vertical)
        root_layout.addWidget(main_splitter, 1)

        top_splitter = QSplitter(Qt.Orientation.Horizontal)
        main_splitter.addWidget(top_splitter)

        # Left top: Query input / annotated SQL
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(10, 10, 10, 10)
        left_layout.setSpacing(8)

        controls = QHBoxLayout()
        controls.setSpacing(8)
        self.analyse_btn = QPushButton("Analyse")
        self.reset_btn = QPushButton("Reset")
        self.analyse_btn.clicked.connect(self._handle_analyse_clicked)
        self.reset_btn.clicked.connect(self._handle_reset_clicked)
        controls.addWidget(self.analyse_btn)
        controls.addWidget(self.reset_btn)
        controls.addStretch(1)
        left_layout.addLayout(controls)

        self.sql_stack = QStackedWidget()
        self.sql_input_editor = QPlainTextEdit()
        self.sql_input_editor.setFont(QFont("Courier New", 13))
        self.sql_input_editor.setPlainText(self.raw_sql)
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

        # Right top: QEP tree panel
        right_panel = QWidget()
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

        # Bottom: Explanation panel (3 cards)
        bottom_panel = QWidget()
        bottom_layout = QVBoxLayout(bottom_panel)
        bottom_layout.setContentsMargins(10, 10, 10, 10)
        bottom_layout.setSpacing(8)
        bottom_layout.addWidget(self._panel_title("Explanation"))
        self.selected_operator_label = QLabel("Operator: (none selected)")
        self.selected_operator_label.setObjectName("SelectedOperator")
        bottom_layout.addWidget(self.selected_operator_label)

        # Operator-level vs Plan-level toggle
        self.explain_mode_tabs = QTabWidget()
        self.explain_mode_tabs.setFixedHeight(28)
        self.operator_mode_page = QWidget()
        self.plan_mode_page = QWidget()
        self.explain_mode_tabs.addTab(self.operator_mode_page, "Operator Explanation")
        self.explain_mode_tabs.addTab(self.plan_mode_page, "Plan Comparison")
        self.explain_mode_tabs.setCurrentIndex(0)
        self.explain_mode_tabs.currentChanged.connect(self._on_explain_mode_changed)
        bottom_layout.addWidget(self.explain_mode_tabs)

        cards_row = QHBoxLayout()
        cards_row.setSpacing(10)
        self.what_card, self.what_card_body = self._create_explain_card("WHAT (Execution)")
        self.why_card, self.why_card_body = self._create_explain_card("WHY (Decision)")
        self.alt_card, self.alt_card_body = self._create_explain_card("ALTERNATIVES (Comparison / AQP)")
        self.what_card.setObjectName("WhatCard")
        self.why_card.setObjectName("WhyCard")
        self.alt_card.setObjectName("AltCard")
        cards_row.addWidget(self.what_card, 1)
        cards_row.addWidget(self.why_card, 1)
        cards_row.addWidget(self.alt_card, 1)
        bottom_layout.addLayout(cards_row, 1)

        # Plan-level widgets live inside the Alternatives card.
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


        # Default mode: operator explanation
        self._refresh_explanation_ui()

        main_splitter.addWidget(bottom_panel)
        main_splitter.setSizes([560, 240])

        self.setStatusBar(QStatusBar())

    def _apply_styles(self) -> None:
        self.setStyleSheet(
            """
            QMainWindow { background: #f3f6fb; }
            QLabel#Header { font-size: 26px; font-weight: 700; color: #0f172a; }
            QLabel#Subtitle { color: #334155; margin-bottom: 4px; font-size: 13px; }
            QLabel#SelectedOperator {
                font-size: 13px;
                font-weight: 700;
                color: #1e3a8a;
                background: #e8f0ff;
                border: 1px solid #b8ccf3;
                border-radius: 8px;
                padding: 6px 10px;
            }
            QLabel[role="panelTitle"] { font-size: 16px; font-weight: 700; color: #1e293b; }
            QSplitter {
                background: transparent;
                border: none;
            }
            QStatusBar {
                background: #eef2f8;
                border-top: 1px solid #dbe3ef;
                font-size: 12px;
            }
            QStackedWidget, QTextEdit, QTreeWidget, QPlainTextEdit, QTextBrowser {
                background: #ffffff;
                border: 1px solid #dbe3ef;
                border-radius: 10px;
            }
            QPlainTextEdit, QTextBrowser, QTextEdit, QTreeWidget {
                border: 1px solid #cfd9e8;
                border-radius: 8px;
                background: #fbfdff;
                color: #0f172a;
                padding: 6px;
                font-size: 13px;
            }
            QFrame[role="explainCard"] {
                border: 1px solid #d1ddf0;
                border-radius: 10px;
                background: #f8fbff;
            }
            QFrame#WhatCard { background: #eaf3ff; border: 1px solid #bfd6ff; }
            QFrame#WhyCard { background: #fff8de; border: 1px solid #efd78b; }
            QFrame#AltCard { background: #eaf9ef; border: 1px solid #b8e2c7; }
            QLabel[role="explainCardTitle"] {
                font-size: 15px;
                font-weight: 700;
                color: #1e40af;
                margin-bottom: 4px;
            }
            QLabel[role="explainCardBody"] {
                font-size: 13px;
                color: #1f2937;
                line-height: 1.55;
            }
            QTreeWidget::item {
                height: 30px;
                padding: 4px 8px;
            }
            QTreeWidget::item:selected {
                background: #bfdbfe;
                color: #111827;
                border: 1px solid #2563eb;
                font-weight: 700;
            }
            QTreeWidget::item:selected:active {
                background: #93c5fd;
                color: #111827;
            }
            QTreeView::branch:has-siblings:!adjoins-item {
                border-image: none;
                border-left: 1px solid #9fb6d9;
            }
            QTreeView::branch:has-siblings:adjoins-item {
                border-image: none;
                border-bottom: 1px solid #9fb6d9;
                border-left: 1px solid #9fb6d9;
            }
            QTreeView::branch:!has-children:!has-siblings:adjoins-item {
                border-image: none;
                border-left: 1px solid #9fb6d9;
                border-bottom: 1px solid #9fb6d9;
            }
            QTreeView::branch:closed:has-children {
                image: url(none);
            }
            QTreeView::branch:open:has-children {
                image: url(none);
            }
            QPushButton {
                background: #1d4ed8;
                color: #ffffff;
                border: none;
                border-radius: 8px;
                padding: 8px 14px;
                font-weight: 600;
                font-size: 13px;
            }
            QPushButton:hover { background: #1e40af; }
            """
        )

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

    def _on_explain_mode_changed(self, _index: int) -> None:
        # Switching modes must not reset the selected operator (core UX requirement).
        self._refresh_explanation_ui()

    def _refresh_explanation_ui(self) -> None:
        """
        Update the 3 cards based on the active explanation mode:
          - Operator Explanation: show WHAT/WHY for the selected operator
          - Plan Comparison: show an AQP comparison table in ALTERNATIVES (global)
        """
        is_plan_mode = self.explain_mode_tabs.currentIndex() == 1

        if is_plan_mode:
            # Plan-level view: show ONLY the alternatives card (global view).
            self.selected_operator_label.setVisible(False)
            self.what_card.setVisible(False)
            self.why_card.setVisible(False)
            self.alt_card.setVisible(True)

            # Before Analyse, no plan has been generated yet.
            if not self.qep_diagram.analysis_ready:
                self.alt_card_body.setText("Run Analyse to generate plan comparison (AQP).")
                self.plan_table.setVisible(False)
                return

            # After Analyse, show the plan comparison table.
            self.plan_table.setVisible(True)
            if self.plan_table.rowCount() == 0:
                self._populate_plan_comparison_table()

        else:
            # Operator-level view: show ONLY WHAT and WHY cards.
            self.selected_operator_label.setVisible(True)
            self.what_card.setVisible(True)
            self.why_card.setVisible(True)
            self.alt_card.setVisible(False)

            self.plan_table.setVisible(False)

            current_op = self.current_active_op_id
            info = self.operator_info.get(current_op) if current_op else None

            if not info:
                self.what_card_body.setText("Select an operator to see execution details.")
                self.why_card_body.setText("WHY will appear once an operator is selected.")
                return

            self.what_card_body.setText(html.escape(info.what))
            self.why_card_body.setText(html.escape(info.why))

            # Alternatives card hidden in this mode.

    def _populate_plan_comparison_table(self) -> None:
        # Sort plans by estimated total cost ascending order.
        rows = sorted(self.plan_comparisons, key=lambda p: float(p["est_total_cost"]))

        self.plan_table.setRowCount(len(rows))
        self.plan_table.setSortingEnabled(False)

        chosen = self.chosen_plan_id
        for idx, plan in enumerate(rows):
            is_chosen = plan["plan_id"] == chosen
            rank_text = str(idx + 1)
            plan_id_text = plan["plan_id"] + ("  ✅ Selected Plan" if is_chosen else "")
            cost_text = f"{float(plan['est_total_cost']):.2f}"

            items = [
                QTableWidgetItem(rank_text),
                QTableWidgetItem(plan_id_text),
                QTableWidgetItem(cost_text),
                QTableWidgetItem(plan["summary"]),
            ]
            for c, item in enumerate(items):
                if is_chosen:
                    item.setBackground(QColor("#d1fae5"))  # light green
                    item.setForeground(QColor("#065f46"))
                    item.setFont(QFont("Segoe UI", 10, QFont.Weight.DemiBold))
                self.plan_table.setItem(idx, c, item)

        # Ensure multiple rows are visible without scrolling.
        if rows:
            self.plan_table.selectRow(0)
        self._fit_plan_table_height()

    def _handle_plan_row_clicked(self, _row: int, _col: int) -> None:
        # Plan-level view intentionally stays minimal (no per-row narrative).
        # Kept for future extension; current selection still highlights the row.
        return

    def _fit_plan_table_height(self) -> None:
        """
        Fit the table height to show all current rows without forcing scrollbars.
        Uses a conservative row height that matches the UI theme.
        """
        if self.plan_table.rowCount() == 0:
            return
        row_height = 28
        header_height = self.plan_table.horizontalHeader().height()
        self.plan_table.setFixedHeight(header_height + self.plan_table.rowCount() * row_height + 8)

    def _load_qep_tree_from_model(self) -> None:
        """
        Load QEP tree from self.qep_tree_model (provider output).
        """
        self.qep_tree.clear()
        self.tree_items_by_op.clear()

        model = self.qep_tree_model if isinstance(self.qep_tree_model, dict) else {}
        if not model:
            # No tree model available (e.g., backend returned empty payload).
            self.qep_diagram.update()
            return

        def build_item(node_model: dict) -> QTreeWidgetItem:
            label = str(node_model.get("label", "Unknown Operator"))
            cost = str(node_model.get("cost", "N/A"))
            op_id = str(node_model.get("op_id", f"unknown_{id(node_model)}"))

            item = QTreeWidgetItem([label, cost])
            item.setData(0, Qt.ItemDataRole.UserRole, op_id)
            self.tree_items_by_op[op_id] = item

            children = node_model.get("children", [])
            if isinstance(children, list):
                for child_model in children:
                    if isinstance(child_model, dict):
                        item.addChild(build_item(child_model))
            return item

        root_item = build_item(model)
        self.qep_tree.addTopLevelItem(root_item)
        root_item.setExpanded(True)
        self.qep_tree.expandAll()
        self.qep_diagram.update()

    def _apply_analysis_data(self, data: AnalysisData) -> None:
        """
        Apply one AnalysisData payload to all UI data dependencies.

        This is the single binding point between data provider and UI widgets.
        """
        self.analysis_data = data
        self.raw_sql = data.raw_sql
        self.operator_info = data.operator_info
        self.chosen_plan_id = data.chosen_plan_id
        self.plan_comparisons = data.plan_comparisons
        self.sql_badge_replacements = data.sql_badge_replacements
        self.qep_tree_model = data.qep_tree_model

        # Rebuild widgets that depend on provider data.
        self.qep_diagram.set_tree_model(self.qep_tree_model)
        self._load_qep_tree_from_model()
        self.plan_table.setRowCount(0)  # force fresh table render in Plan Comparison mode

        # Reset selection-related UI state so no stale operator appears after refresh.
        self.current_active_op_id = None
        self.qep_tree.clearSelection()
        self.qep_diagram.set_active(None)
        self.selected_operator_label.setText("Operator: (none selected)")
        self._refresh_explanation_ui()

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

    def _handle_analyse_clicked(self) -> None:
        """
        Analyse flow (provider-driven):
          1) Read SQL from input editor
          2) Request fresh AnalysisData from get_analysis_data(query)
          3) Apply data to UI and re-render
        """
        query = self.sql_input_editor.toPlainText().strip() or self.raw_sql
        fresh_data = get_analysis_data(query)
        self._apply_analysis_data(fresh_data)

        # Keep editor synchronized to provider-returned SQL.
        self.sql_input_editor.setPlainText(self.raw_sql)
        self._render_annotated_sql(active_op_id=None)
        self._set_annotated_mode()
        self.qep_tree.setEnabled(True)
        self.qep_diagram.set_analysis_ready(True)
        self.qep_diagram.set_active(None)
        self.statusBar().showMessage("Analysis complete (mock). Click annotation or QEP node.")

    def _handle_reset_clicked(self) -> None:
        self.sql_input_editor.setPlainText(self.raw_sql)
        self._set_input_mode()
        self.qep_tree.clearSelection()
        self.statusBar().showMessage("Reset to SQL input mode.")

    def _render_annotated_sql(self, active_op_id: Optional[str]) -> None:
        """
        Render SQL with inline clickable annotation segments.
        Clicking a segment triggers bidirectional sync to QEP.

        # TODO (backend integration): Replace the hardcoded substring replacements
        # (FROM/JOIN lines) with a data-driven mapping from:
        #   SQL clause/span -> op_id
        # returned by your backend annotate_query() implementation.
        """
        sql_html = html.escape(self.raw_sql)

        # Currently using mock mapping for which SQL substrings get which op_id badges.
        for rep in self.sql_badge_replacements:
            match_text = rep["match"]
            escaped_match = html.escape(match_text)

            badge_html_parts: List[str] = []
            for badge in rep.get("badges", []):
                op_id = badge["op_id"]
                badge_text = badge["badge_text"]
                badge_html_parts.append(
                    f"<a class='sql-tag' href='op://{op_id}' "
                    f"style='{self._badge_style(active_op_id == op_id)}'>"
                    f"[{badge_text}]</a>"
                )

            if badge_html_parts:
                # Space out badges with non-breaking spaces so it stays aligned in the HTML <pre>.
                badges_joined = "&nbsp;".join(badge_html_parts)
                replacement = f"{escaped_match}&nbsp;&nbsp;{badges_joined}"
                sql_html = sql_html.replace(escaped_match, replacement)

        html_doc = (
            "<style>"
            "a.sql-tag{ text-decoration:none; transition: all 120ms ease-in-out; }"
            "a.sql-tag:hover{ filter:brightness(0.95); box-shadow:0 0 0 2px rgba(59,130,246,0.2); }"
            "</style>"
            "<div style='font-family:Courier New, Consolas, monospace; font-size:14px; line-height:1.65; color:#0f172a;'>"
            "<div style='margin-bottom:8px; color:#475569; font-family:Segoe UI, Arial; font-size:12px;'>"
            "Click a blue badge to focus the matching operator in QEP.</div>"
            f"<pre style='white-space:pre-wrap; margin:0;'>{sql_html}</pre>"
            "</div>"
        )
        self.annotated_sql_view.setHtml(html_doc)

    def _badge_style(self, active: bool) -> str:
        if active:
            return (
                "text-decoration:none; color:#0f172a; background:#93c5fd; "
                "border:1px solid #2563eb; border-radius:8px; padding:3px 8px; font-weight:700;"
                "box-shadow: inset 0 0 0 1px rgba(37,99,235,0.18);"
            )
        return (
            "text-decoration:none; color:#1e3a8a; background:#d0e7ff; "
            "border:1px solid #7fb0ef; border-radius:8px; padding:3px 8px; font-weight:600;"
        )

    def _handle_sql_anchor_clicked(self, url: QUrl) -> None:
        """
        SQL -> QEP + Explanation sync.
        """
        op_id = url.toString().replace("op://", "").strip()
        if not op_id:
            return
        self._set_active_operator(op_id, source="sql")

    def _handle_tree_item_clicked(self, item: QTreeWidgetItem) -> None:
        """
        QEP -> SQL + Explanation sync.
        """
        op_id = item.data(0, Qt.ItemDataRole.UserRole)
        if not op_id:
            return
        self._set_active_operator(str(op_id), source="tree")

    def _handle_diagram_node_clicked(self, op_id: str) -> None:
        self._set_active_operator(op_id, source="diagram")

    def _set_active_operator(self, op_id: str, source: str) -> None:
        """
        Central sync handler for bidirectional linking.
        """
        self.current_active_op_id = op_id
        self._highlight_tree_node(op_id)
        self.qep_diagram.set_active(op_id)
        self._render_annotated_sql(active_op_id=op_id)
        self._update_explanation_panel(op_id)
        self.statusBar().showMessage(f"Selected {op_id} via {source}.")

    def _highlight_tree_node(self, op_id: str) -> None:
        item = self.tree_items_by_op.get(op_id)
        if not item:
            return
        self.qep_tree.setCurrentItem(item)
        self.qep_tree.scrollToItem(item)

    def _update_explanation_panel(self, op_id: str) -> None:
        info = self.operator_info.get(op_id)
        if not info:
            self.selected_operator_label.setText("Operator: (unknown)")
        else:
            self.selected_operator_label.setText(f"Operator: {info.name}")
        # Refresh UI based on the currently active mode (operator vs plan).
        self._refresh_explanation_ui()

def main() -> None:
    app = QApplication(sys.argv)
    app.setFont(QFont("Segoe UI", 10))
    window = SqlQepComprehensionUI()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
