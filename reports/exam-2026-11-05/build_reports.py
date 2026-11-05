"""Build the study-pack figures and printable PDFs from its Markdown sources.

Requires reportlab, pymupdf, and markdown-it-py. It reads source/evidence only;
it never starts Docker, alters the application, or generates test traffic.
"""

from __future__ import annotations

import html
import json
import re
from pathlib import Path

import pymupdf
import reportlab
from markdown_it import MarkdownIt
from reportlab.graphics import renderPDF, renderSVG
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.shapes import Drawing, Line, Polygon, PolyLine, Rect, String
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    Image,
    PageBreak,
    PageTemplate,
    Paragraph,
    Preformatted,
    Spacer,
    Table,
    TableStyle,
)

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
FIGURES = HERE / "figures"
PDFS = HERE / "pdf"
NAVY = colors.HexColor("#12304A")
TEAL = colors.HexColor("#087E83")
ORANGE = colors.HexColor("#C85E24")
RED = colors.HexColor("#B4443B")
MUTED = colors.HexColor("#536372")
LIGHT = colors.HexColor("#EFF4F7")
BORDER = colors.HexColor("#D4DFE5")

fonts = Path(reportlab.__file__).parent / "fonts"
for name, file in (
    ("Study", "Vera.ttf"),
    ("StudyBold", "VeraBd.ttf"),
    ("StudyItalic", "VeraIt.ttf"),
    ("StudyBoldItalic", "VeraBI.ttf"),
):
    pdfmetrics.registerFont(TTFont(name, str(fonts / file)))
pdfmetrics.registerFontFamily(
    "Study", normal="Study", bold="StudyBold", italic="StudyItalic", boldItalic="StudyBoldItalic"
)


def export_figure(drawing, name):
    renderPDF.drawToFile(drawing, str(FIGURES / f"{name}.pdf"))
    renderSVG.drawToFile(drawing, str(FIGURES / f"{name}.svg"))
    doc = pymupdf.open(FIGURES / f"{name}.pdf")
    doc[0].get_pixmap(matrix=pymupdf.Matrix(2, 2), alpha=False).save(FIGURES / f"{name}.png")
    doc.close()


def label(d, x, y, text, size=12, color=NAVY, anchor="middle", bold=False):
    d.add(
        String(
            x,
            y,
            text,
            fontName="StudyBold" if bold else "Study",
            fontSize=size,
            fillColor=color,
            textAnchor=anchor,
        )
    )


def node(d, x, y, w, h, title, subtitle="", fill=LIGHT):
    d.add(Rect(x, y, w, h, rx=8, ry=8, strokeColor=BORDER, fillColor=fill))
    label(d, x + w / 2, y + h / 2 + (4 if subtitle else -4), title, 12, bold=True)
    if subtitle:
        label(d, x + w / 2, y + h / 2 - 14, subtitle, 9, MUTED)


def arrow(d, points, color=TEAL, dashed=False):
    d.add(
        PolyLine(
            [v for p in points for v in p],
            strokeColor=color,
            strokeWidth=1.8,
            strokeDashArray=[4, 3] if dashed else None,
        )
    )
    (x0, y0), (x, y) = points[-2:]
    import math

    a = math.atan2(y - y0, x - x0)
    tip = []
    for b in (a + 2.65, a - 2.65):
        tip.extend((x + 8 * math.cos(b), y + 8 * math.sin(b)))
    d.add(Polygon([x, y, *tip], fillColor=color, strokeColor=color))


def architecture():
    d = Drawing(820, 430)
    label(d, 25, 405, "Application responsibilities and data flow", 17, anchor="start", bold=True)
    node(d, 25, 300, 130, 58, "Kafka", "Avro events")
    node(d, 195, 300, 145, 58, "Scheduler", "ordered partition tasks")
    node(d, 380, 300, 150, 58, "Executor", "stateless computation")
    node(d, 570, 300, 210, 58, "Data", "SQL + coherent bounded caches")
    arrow(d, [(155, 329), (195, 329)])
    arrow(d, [(340, 329), (380, 329)])
    arrow(d, [(530, 329), (570, 329)])
    node(d, 570, 190, 100, 56, "PostgreSQL", "authority")
    node(d, 690, 190, 90, 56, "Redis", "cache")
    arrow(d, [(630, 300), (630, 246)])
    arrow(d, [(735, 300), (735, 246)])
    node(d, 25, 175, 130, 58, "Researcher", "or HTTP validator")
    node(d, 195, 175, 145, 58, "REST gateway", "Caddy :3003")
    arrow(d, [(155, 204), (195, 204)])
    arrow(d, [(340, 204), (415, 204), (415, 265), (600, 265), (600, 300)])
    label(d, 372, 215, "history / state", 9)
    arrow(d, [(267, 233), (267, 274), (455, 274), (455, 300)], color=ORANGE)
    label(d, 335, 282, "configuration", 9, color=ORANGE)
    node(d, 380, 52, 180, 62, "Notifier", "pull leases; send; acknowledge")
    node(d, 620, 52, 160, 62, "Notifications API", "local or course endpoint")
    arrow(d, [(560, 83), (620, 83)])
    arrow(d, [(570, 315), (550, 315), (550, 150), (470, 150), (470, 114)], dashed=True)
    label(d, 500, 159, "leased jobs", 10)
    label(
        d,
        25,
        20,
        "PostgreSQL owns durable state. Redis is disposable. The notifier pulls jobs from Data.",
        10,
        MUTED,
        anchor="start",
    )
    export_figure(d, "architecture")


def comparison():
    before = json.loads((ROOT / "benchmarks/2026-10-09/course-stress-initial.json").read_text())
    after = json.loads((ROOT / "benchmarks/2026-10-09/course-stress.json").read_text())
    d = Drawing(820, 330)
    label(
        d,
        25,
        304,
        "Instructor stress test before and after pipelining",
        16,
        anchor="start",
        bold=True,
    )
    vals = (
        (
            "Maximum notification latency (seconds)",
            [
                before["notifications"]["max_latency_seconds"],
                after["notifications"]["max_latency_seconds"],
            ],
            12,
            2,
            10,
            "10-second boundary",
        ),
        (
            "Achieved valid HTTP responses per second",
            [before["achieved_http_rps"], after["achieved_http_rps"]],
            110,
            20,
            95,
            "95/s local acceptance",
        ),
    )
    for i, (title, data, maximum, step, threshold, threshold_name) in enumerate(vals):
        x = 55 + i * 405
        y = 83
        w = 315
        h = 172
        chart = VerticalBarChart()
        chart.x = x
        chart.y = y
        chart.width = w
        chart.height = h
        chart.data = [data]
        chart.categoryAxis.categoryNames = ["Before", "After"]
        chart.valueAxis.valueMin = 0
        chart.valueAxis.valueMax = maximum
        chart.valueAxis.valueStep = step
        chart.valueAxis.labels.fontName = "Study"
        chart.valueAxis.labels.fontSize = 9
        chart.categoryAxis.labels.fontName = "Study"
        chart.categoryAxis.labels.fontSize = 10
        chart.valueAxis.visibleGrid = True
        chart.valueAxis.gridStrokeColor = BORDER
        chart.bars[(0, 0)].fillColor = RED
        chart.bars[(0, 1)].fillColor = TEAL
        chart.bars.strokeColor = None
        chart.barLabelFormat = "%.3f"
        chart.barLabels.fontName = "StudyBold"
        chart.barLabels.fontSize = 10
        chart.barLabels.nudge = 7
        d.add(chart)
        ty = y + h * threshold / maximum
        d.add(Line(x, ty, x + w, ty, strokeColor=ORANGE, strokeDashArray=[5, 3]))
        label(d, x + w / 2, ty + 6, threshold_name, 8, ORANGE)
        label(d, x + w / 2, 274, title, 11, bold=True)
    label(
        d,
        25,
        33,
        "100 experiments x 10 sensors x 1 Hz; 100 offered HTTP/s. Historical local runs, 9 October 2026.",
        9,
        MUTED,
        anchor="start",
    )
    label(
        d,
        25,
        17,
        "Different random temperature paths; these are observed before/after values, not repeated A/B estimates.",
        9,
        MUTED,
        anchor="start",
    )
    export_figure(d, "instructor-comparison")


def sizing():
    rows = json.loads((ROOT / "benchmarks/2026-10-07/sizing-matrix.json").read_text())
    rows = [r for r in rows if r["notification_interval"] == 0.1]
    d = Drawing(820, 330)
    label(
        d,
        25,
        304,
        "More executors did not improve combined capacity on this VM",
        16,
        anchor="start",
        bold=True,
    )
    chart = VerticalBarChart()
    chart.x = 80
    chart.y = 79
    chart.width = 650
    chart.height = 175
    chart.data = [[r["http_requests_per_second"] for r in rows]]
    chart.categoryAxis.categoryNames = [
        f"{r['replicas']} executor" + ("s" if r["replicas"] > 1 else "") for r in rows
    ]
    chart.valueAxis.valueMin = 0
    chart.valueAxis.valueMax = 110
    chart.valueAxis.valueStep = 20
    chart.valueAxis.labels.fontName = "Study"
    chart.valueAxis.labels.fontSize = 10
    chart.categoryAxis.labels.fontName = "Study"
    chart.categoryAxis.labels.fontSize = 11
    chart.valueAxis.visibleGrid = True
    chart.valueAxis.gridStrokeColor = BORDER
    for i in range(len(rows)):
        chart.bars[(0, i)].fillColor = TEAL if i == 0 else MUTED
    chart.bars.strokeColor = None
    chart.barLabelFormat = "%.3f"
    chart.barLabels.fontName = "StudyBold"
    chart.barLabels.fontSize = 11
    chart.barLabels.nudge = 7
    d.add(chart)
    yy = 79 + 175 * 95 / 110
    d.add(Line(80, yy, 730, yy, strokeColor=ORANGE, strokeDashArray=[5, 3]))
    label(d, 740, yy - 10, "95/s", 9, ORANGE, anchor="start")
    label(d, 410, 273, "Achieved HTTP responses/second; 100 offered", 12, bold=True)
    label(
        d,
        25,
        34,
        "100 experiments x 10 sensors x 1 Hz, 0.1-second notification poll. Historical local runs, 7 October 2026.",
        9,
        MUTED,
        anchor="start",
    )
    label(
        d,
        25,
        17,
        "The dashed line is our 95% acceptance criterion. Notification deadlines and data correctness passed in all three.",
        9,
        MUTED,
        anchor="start",
    )
    export_figure(d, "executor-sizing")


def failure_boundaries():
    d = Drawing(820, 330)
    label(d, 25, 305, "Two different acknowledgement boundaries", 17, anchor="start", bold=True)
    node(d, 25, 215, 145, 56, "Kafka task", "offsets 41 through 45")
    node(d, 245, 205, 290, 76, "PostgreSQL COMMIT", "state + history + job + checkpoint 45")
    node(d, 610, 215, 180, 56, "Kafka acknowledge", "next-read offset 46")
    arrow(d, [(170, 243), (245, 243)])
    arrow(d, [(535, 243), (610, 243)])
    label(d, 573, 286, "A", 13, ORANGE, bold=True)
    label(
        d,
        25,
        181,
        "A: lost response after COMMIT -> replay finds durable progress; no second database effect.",
        11,
        anchor="start",
    )
    node(d, 25, 78, 180, 56, "Lease durable job", "token guards ownership")
    node(d, 295, 78, 200, 56, "Receiver accepts", "external HTTP effect")
    node(d, 595, 78, 195, 56, "Record delivery", "acknowledge lease token")
    arrow(d, [(205, 106), (295, 106)])
    arrow(d, [(495, 106), (595, 106)])
    label(d, 545, 145, "B", 13, RED, bold=True)
    label(
        d,
        25,
        48,
        "B: lost response after acceptance -> retry may repeat the external effect.",
        11,
        anchor="start",
    )
    label(
        d,
        25,
        25,
        "Receiver idempotency is needed for unique external acceptance. A database transaction cannot decide that outcome.",
        10,
        MUTED,
        anchor="start",
    )
    export_figure(d, "failure-boundaries")


def http_phases():
    native = json.loads((ROOT / "benchmarks/2026-11-05/course-variable-final.json").read_text())
    passing = json.loads((ROOT / "benchmarks/2026-11-05/variable-100-final.json").read_text())
    boundary = json.loads((ROOT / "benchmarks/2026-11-05/variable-final.json").read_text())
    d = Drawing(820, 650)
    label(d, 25, 624, "HTTP rate by observed workload phase", 17, anchor="start", bold=True)
    d.add(Rect(27, 598, 12, 10, fillColor=MUTED, strokeColor=None))
    label(d, 45, 598, "Target", 10, anchor="start")
    d.add(Rect(128, 598, 12, 10, fillColor=TEAL, strokeColor=None))
    label(d, 146, 598, "Achieved", 10, anchor="start")
    d.add(Line(270, 603, 290, 603, strokeColor=ORANGE, strokeDashArray=[4, 2]))
    label(d, 298, 598, "95% local threshold (not a rubric percentage)", 10, anchor="start")
    plots = [
        (
            "Original tools: variable targets; five settled phases passed",
            native["variable_http_validation"]["phases"],
        ),
        ("Independent live histories: 25 / 100 / 50; all phases passed", passing["http_phases"]),
        (
            "Independent live histories: 50 / 200 / 100; last two phases failed",
            boundary["http_phases"],
        ),
    ]
    for row, (title, phases) in enumerate(plots):
        y = 439 - row * 176
        h = 105
        left = 67
        width = 700
        maximum = 220
        label(d, 25, y + 130, title, 12, anchor="start", bold=True)
        for tick in (0, 50, 100, 150, 200):
            yy = y + h * tick / maximum
            d.add(Line(left, yy, left + width, yy, strokeColor=BORDER, strokeWidth=0.5))
            label(d, left - 10, yy - 3, str(tick), 9, MUTED, anchor="end")
        step = width / len(phases)
        for i, p in enumerate(phases):
            x = left + (i + 0.5) * step
            bw = 24
            target = p["target_rps"]
            actual = p["achieved_rps"]
            d.add(Rect(x - bw - 2, y, bw, h * target / maximum, fillColor=MUTED, strokeColor=None))
            d.add(
                Rect(
                    x + 2,
                    y,
                    bw,
                    h * actual / maximum,
                    fillColor=TEAL if p["passed"] else RED,
                    strokeColor=None,
                )
            )
            threshold_y = y + h * target * 0.95 / maximum
            d.add(
                Line(
                    x - bw - 6,
                    threshold_y,
                    x + bw + 6,
                    threshold_y,
                    strokeColor=ORANGE,
                    strokeWidth=1.2,
                    strokeDashArray=[3, 2],
                )
            )
            label(
                d, x + 15, y + h * max(target, actual) / maximum + 8, f"{actual:.1f}", 10, bold=True
            )
            label(d, x, y - 17, f"Phase {i + 1}: {target}/s", 10)
    label(
        d,
        25,
        35,
        "Original-tool settled phases exclude transition seconds. Independent rates include phase drain and detect omitted offers.",
        9,
        MUTED,
        anchor="start",
    )
    label(
        d,
        25,
        17,
        "Different workloads and query histories; these plots are not a paired comparison. Saved measurements: 5 November 2026.",
        9,
        MUTED,
        anchor="start",
    )
    export_figure(d, "http-phases")


styles = getSampleStyleSheet()
styles.add(
    ParagraphStyle(
        "StudyBody",
        fontName="Study",
        fontSize=9.4,
        leading=13.7,
        textColor=NAVY,
        spaceAfter=7,
        splitLongWords=True,
    )
)
styles.add(
    ParagraphStyle(
        "StudyTitle",
        parent=styles["StudyBody"],
        fontName="StudyBold",
        fontSize=23,
        leading=28,
        spaceAfter=14,
        keepWithNext=True,
    )
)
styles.add(
    ParagraphStyle(
        "StudyH2",
        parent=styles["StudyBody"],
        fontName="StudyBold",
        fontSize=14,
        leading=18,
        spaceBefore=12,
        spaceAfter=7,
        keepWithNext=True,
    )
)
styles.add(
    ParagraphStyle(
        "StudyH3",
        parent=styles["StudyBody"],
        fontName="StudyBold",
        fontSize=10.7,
        leading=14.5,
        spaceBefore=10,
        spaceAfter=5,
        keepWithNext=True,
    )
)
styles.add(ParagraphStyle("StudySmall", parent=styles["StudyBody"], fontSize=8, leading=11))
styles.add(
    ParagraphStyle("StudyCell", parent=styles["StudyBody"], fontSize=7.5, leading=10, spaceAfter=0)
)
styles.add(
    ParagraphStyle(
        "StudyCellHead", parent=styles["StudyCell"], fontName="StudyBold", textColor=colors.white
    )
)
styles.add(
    ParagraphStyle(
        "StudyCode",
        fontName="Courier",
        fontSize=7,
        leading=10,
        spaceBefore=3,
        spaceAfter=7,
        backColor=LIGHT,
        borderPadding=7,
    )
)
styles.add(
    ParagraphStyle(
        "StudyQuote",
        parent=styles["StudyBody"],
        fontName="StudyItalic",
        leftIndent=12,
        borderColor=TEAL,
        borderWidth=0.7,
        borderPadding=8,
        spaceBefore=5,
        spaceAfter=10,
    )
)
md = MarkdownIt("commonmark").enable("table")
PAGE_W, PAGE_H = A4
MARGIN = 43
WIDTH = PAGE_W - 2 * MARGIN
current_source = HERE / "README.md"


def inline(token):
    out = []
    for t in token.children or []:
        if t.type == "text":
            out.append(html.escape(t.content.replace("→", "->")))
        elif t.type in ("softbreak", "hardbreak"):
            out.append("<br/>" if t.type == "hardbreak" else " ")
        elif t.type == "strong_open":
            out.append("<b>")
        elif t.type == "strong_close":
            out.append("</b>")
        elif t.type == "em_open":
            out.append("<i>")
        elif t.type == "em_close":
            out.append("</i>")
        elif t.type == "code_inline":
            out.append('<font name="Courier">' + html.escape(t.content) + "</font>")
        elif t.type == "link_open":
            target = t.attrGet("href")
            # Relative file links resolve from the PDF directory, one level below Markdown.
            if not re.match(r"^[a-zA-Z]+:", target) and not target.startswith("#"):
                target = "../" + target
            if target.startswith("#"):
                out.append('<font color="#087E83">')
            else:
                out.append('<link href="' + html.escape(target, quote=True) + '" color="#087E83">')
        elif t.type == "link_close":
            # All study-pack source links are external files, not local fragment-only anchors.
            out.append("</link>")
        elif t.type == "image":
            out.append(html.escape(t.content.replace("→", "->")))
        elif t.type == "html_inline":
            out.append(html.escape(t.content.replace("→", "->")))
    return "".join(out)


class StudyDoc(BaseDocTemplate):
    def __init__(self, path, title):
        super().__init__(
            str(path),
            pagesize=A4,
            leftMargin=MARGIN,
            rightMargin=MARGIN,
            topMargin=49,
            bottomMargin=44,
            title=title,
            author="Group 15",
            subject="INFOMCEC final assessment preparation",
            pageCompression=1,
        )
        self.short_title = title
        frame = Frame(
            MARGIN,
            44,
            WIDTH,
            PAGE_H - 93,
            id="main",
            leftPadding=0,
            rightPadding=0,
            topPadding=0,
            bottomPadding=0,
        )
        self.addPageTemplates(PageTemplate(id="study", frames=frame, onPage=self.header))
        self.bookmark_number = 0

    def header(self, canvas, doc):
        canvas.saveState()
        canvas.setStrokeColor(BORDER)
        canvas.line(MARGIN, PAGE_H - 32, PAGE_W - MARGIN, PAGE_H - 32)
        canvas.setFont("Study", 7.3)
        canvas.setFillColor(MUTED)
        canvas.drawString(MARGIN, PAGE_H - 23, "GROUP 15  |  INFOMCEC 2026-2027")
        canvas.drawRightString(PAGE_W - MARGIN, PAGE_H - 23, "PREPARATION  •  5 NOVEMBER 2026")
        canvas.drawString(MARGIN, 25, "Evidence-based design and oral exam preparation")
        canvas.drawRightString(PAGE_W - MARGIN, 25, str(doc.page))
        canvas.restoreState()

    def afterFlowable(self, flowable):
        if isinstance(flowable, Paragraph) and flowable.style.name in (
            "StudyTitle",
            "StudyH2",
            "StudyH3",
        ):
            title = flowable.getPlainText()
            key = f"section-{self.bookmark_number}"
            self.bookmark_number += 1
            self.canv.bookmarkPage(key)
            self.canv.addOutlineEntry(
                title,
                key,
                level={"StudyTitle": 0, "StudyH2": 1, "StudyH3": 2}[flowable.style.name],
                closed=False,
            )


def table_from_tokens(tokens, start):
    rows = []
    row = []
    head = True
    i = start + 1
    while i < len(tokens) and tokens[i].type != "table_close":
        t = tokens[i]
        if t.type == "tr_open":
            row = []
        elif t.type == "inline":
            row.append((inline(t), head))
        elif t.type == "tr_close":
            rows.append(row)
        elif t.type == "thead_close":
            head = False
        i += 1
    n = len(rows[0])
    weights = {
        2: [1, 2.1],
        3: [1, 1.45, 1.65],
        4: [1, 1.1, 1.1, 1.3],
        5: [1, 1, 1, 1, 1],
        6: [1, 1, 1, 1, 1, 1],
        7: [0.7, 0.7, 1, 1, 1.1, 1.1, 1.6],
    }.get(n, [1] * n)
    if n == 3 and any(len(re.sub("<[^>]+>", "", r[0][0])) > 70 for r in rows):
        weights = [1.4, 1.3, 1.3]
    widths = [WIDTH * w / sum(weights) for w in weights]
    cells = [
        [Paragraph(s, styles["StudyCellHead" if h else "StudyCell"]) for s, h in r] for r in rows
    ]
    table = Table(cells, colWidths=widths, repeatRows=1, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), NAVY),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, LIGHT]),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ("LINEBELOW", (0, 0), (-1, 0), 0.7, NAVY),
                ("LINEBELOW", (0, 1), (-1, -1), 0.3, BORDER),
            ]
        )
    )
    return table, i


def story_from(path, include_title=True):
    global current_source
    current_source = path
    tokens = md.parse(path.read_text())
    out = []
    i = 0
    list_depth = 0
    ordered = []
    in_item = False
    item_first = False
    in_quote = False
    while i < len(tokens):
        t = tokens[i]
        if t.type == "heading_open":
            level = int(t.tag[1])
            content = inline(tokens[i + 1])
            if level == 1:
                if include_title:
                    out.extend(
                        [
                            Paragraph(content, styles["StudyTitle"]),
                            Paragraph(
                                "Group 15 • Final assessment preparation • 5 November 2026",
                                styles["StudySmall"],
                            ),
                            Spacer(1, 4),
                        ]
                    )
            else:
                out.append(Paragraph(content, styles["StudyH2" if level == 2 else "StudyH3"]))
            i += 2
        elif t.type == "paragraph_open":
            tok = tokens[i + 1]
            images = [c for c in tok.children or [] if c.type == "image"]
            if images:
                for img in images:
                    target = (path.parent / img.attrGet("src")).resolve()
                    pix = pymupdf.Pixmap(str(target))
                    h = WIDTH * pix.height / pix.width
                    out.append(Image(str(target), width=WIDTH, height=h))
                    out.append(Spacer(1, 8))
            else:
                style = styles["StudyQuote" if in_quote else "StudyBody"]
                content = inline(tok)
                bullet = None
                if in_item and item_first:
                    bullet = f"{ordered[-1]}." if ordered and ordered[-1] is not None else "•"
                    if ordered and ordered[-1] is not None:
                        ordered[-1] += 1
                    style = ParagraphStyle(
                        "List",
                        parent=style,
                        leftIndent=13,
                        firstLineIndent=0,
                        bulletIndent=1,
                        spaceAfter=5,
                    )
                    item_first = False
                out.append(Paragraph(content, style, bulletText=bullet))
            i += 2
        elif t.type in ("bullet_list_open", "ordered_list_open"):
            list_depth += 1
            ordered.append(int(t.attrGet("start") or 1) if t.type == "ordered_list_open" else None)
        elif t.type in ("bullet_list_close", "ordered_list_close"):
            list_depth -= 1
            ordered.pop()
            in_item = False
            out.append(Spacer(1, 2))
        elif t.type == "list_item_open":
            in_item = True
            item_first = True
        elif t.type == "list_item_close":
            in_item = False
        elif t.type == "blockquote_open":
            in_quote = True
        elif t.type == "blockquote_close":
            in_quote = False
        elif t.type == "table_open":
            table, i = table_from_tokens(tokens, i)
            out.extend([table, Spacer(1, 9)])
        elif t.type in ("fence", "code_block"):
            out.append(Preformatted(t.content.rstrip(), styles["StudyCode"], maxLineLength=100))
        elif t.type == "hr":
            out.append(Spacer(1, 8))
        i += 1
    return out


def main():
    FIGURES.mkdir(exist_ok=True)
    PDFS.mkdir(exist_ok=True)
    architecture()
    failure_boundaries()
    http_phases()
    sizing()
    sources = sorted(HERE.glob("0[1-9]-*.md"))
    results = []
    for path in sources:
        target = PDFS / (path.stem + ".pdf")
        StudyDoc(target, path.stem).build(story_from(path))
        doc = pymupdf.open(target)
        results.append(
            {
                "source": path.name,
                "pdf": str(target.relative_to(HERE)),
                "pages": len(doc),
                "bytes": target.stat().st_size,
            }
        )
        doc.close()
    book = PDFS / "group-15-assessment.pdf"
    story = story_from(HERE / "README.md")
    for path in sources:
        story.append(PageBreak())
        story.extend(story_from(path))
    StudyDoc(book, "Group 15 assessment and oral exam preparation").build(story)
    doc = pymupdf.open(book)
    results.append(
        {"pdf": str(book.relative_to(HERE)), "pages": len(doc), "bytes": book.stat().st_size}
    )
    doc.close()
    (HERE / "evidence/pdf-build.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    from prepare_sources import prepare

    prepare()
    main()
