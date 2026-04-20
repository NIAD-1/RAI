#!/usr/bin/env python3
"""
Convert research JSON output into a clean manuscript-style PDF.

The layout intentionally follows conservative conventions shared by widely
accepted academic styles such as APA and Turabian: standard paper size,
1-inch margins, 12-point serif type, double spacing, simple pagination,
and restrained section headings.
"""

import json
import re
import sys

from difflib import SequenceMatcher
from xml.sax.saxutils import escape

from reportlab.lib.colors import black
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer

DOMAIN_LABELS = {
    "mdpi.com": "MDPI",
    "nature.com": "Nature",
    "sciencedirect.com": "ScienceDirect",
    "academia.edu": "Academia.edu",
    "pmc.ncbi.nlm.nih.gov": "PMC",
    "ncbi.nlm.nih.gov": "NCBI",
}

META_PREFIX_RE = re.compile(
    r"^\s*(?:"
    r"role|task|constraints|minimum length|minimum citations|output|length|tone|"
    r"check|critique of draft|current draft analysis|current state|refining the prose|"
    r"opening|middle|closing|content|issues to fix|core arguments|design|"
    r"justification|problem|aim|significance|structure|word count check|tone check|"
    r"citation check|drafting thought|drafting the final version|self-correction during drafting|"
    r"strict academic editor|professor ai"
    r")\s*[:.]",
    re.I,
)

LABEL_PREFIX_RE = re.compile(
    r"^\s*\*?(?:"
    r"sentence|paragraph|para|drafting(?:\s+(?:paragraph|para|sentence))?|"
    r"drafting\s+p\d+|final polish|refinement(?:\s+\d+)?|opening|closing|aim|"
    r"problem|significance|structure|findings|nuance|implication|"
    r"recommendation(?:\s+\d+)?|transition|observation|thesis"
    r")"
    r"(?:\s+\d+)?(?:\s*\([^)]{1,80}\))?\s*:\*?\s*",
    re.I,
)

INLINE_LABEL_RE = re.compile(
    r"\*?(?:"
    r"sentence|paragraph|para|drafting(?:\s+(?:paragraph|para|sentence))?|"
    r"drafting\s+p\d+|final polish|refinement(?:\s+\d+)?|opening|closing|aim|"
    r"problem|significance|structure|findings|nuance|implication|"
    r"recommendation(?:\s+\d+)?|transition|observation|thesis"
    r")(?:\s+\d+)?(?:\s*\([^)]{1,80}\))?\s*:\*?",
    re.I,
)

PROMPT_TERMS = (
    "strict academic editor",
    "professor ai",
    "meta-commentary",
    "return only",
    "minimum length",
    "minimum citations",
    "word count check",
    "citation check",
    "tone check",
    "critique of draft",
    "current draft analysis",
    "drafting thought",
    "drafting the final version",
    "self-correction",
    "refining the flow",
    "vocabulary enhancement",
    "world-class academic",
    "placeholder",
    "proper apa",
    "no headings",
    "no bullets",
    "no todos",
    "passes >",
    "meets >",
    "prompt asks",
    "only polished prose",
    "completed section text",
)

INSTRUCTION_OPENERS = (
    "start by",
    "focus on",
    "connect",
    "discuss",
    "mention",
    "integrate",
    "ensure the",
    "use the",
    "move from",
    "transition to",
    "introduce",
    "acknowledge",
    "distinguish between",
    "explain how",
    "bring it all together",
    "address the",
    "return only",
)

EVALUATIVE_SHORT_LINES = {
    "good.",
    "good",
    "strong.",
    "strong",
    "stronger.",
    "stronger",
    "polished.",
    "polished",
    "excellent closing.",
    "excellent closing",
    "clear objective.",
    "clear objective",
    "clear methodology.",
    "clear methodology",
    "clear goal/scope.",
    "clear goal/scope",
}


def clean_markdown(text: str) -> str:
    text = text.replace("$\\rightarrow$", "to")
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"\*(.+?)\*", r"\1", text)
    text = re.sub(r"`(.+?)`", r"\1", text)
    text = re.sub(r"\[(.+?)\]\((.+?)\)", r"\1", text)
    text = re.sub(r"^#+\s+", "", text, flags=re.MULTILINE)
    text = text.replace("•", "")
    return text.strip()


def pretty_source_label(raw: str) -> str:
    lowered = raw.strip().lower()
    for domain, label in DOMAIN_LABELS.items():
        if domain in lowered:
            return label
    return raw.strip()


def source_placeholder_map(data: dict) -> dict[int, str]:
    placeholders = {}
    for idx, source in enumerate(data.get("sources", []), start=1):
        label = pretty_source_label(source.get("author_or_channel") or source.get("url") or f"Source {idx}")
        year = source.get("year_hint") or "n.d."
        placeholders[idx] = f"{label}, {year}"
    return placeholders


def replace_source_placeholders(text: str, placeholders: dict[int, str]) -> str:
    def replace_source(match):
        index = int(match.group(1))
        return placeholders.get(index, match.group(0))

    text = re.sub(r"\bSource\s+(\d+)\b", replace_source, text, flags=re.I)

    for raw, label in DOMAIN_LABELS.items():
        text = re.sub(re.escape(raw), label, text, flags=re.I)

    text = re.sub(r"\bNational Center for Biotechnology Information\s*\[NCBI\]", "NCBI", text, flags=re.I)
    text = re.sub(r"\(Author,\s*(\d{4}|n\.d\.)\)", r"(Source, \1)", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def strip_meta_parentheticals(text: str) -> str:
    def replacer(match):
        inner = match.group(1).strip().lower()
        meta_words = (
            "strong",
            "good",
            "clear",
            "formal",
            "polished",
            "passes",
            "meets",
            "check",
            "tone",
            "citation",
            "length",
            "meta-commentary",
            "world-class",
            "placeholder",
        )
        return "" if any(word in inner for word in meta_words) else match.group(0)

    return re.sub(r"\(([^()]{1,160})\)", replacer, text)


def extract_quoted_passages(text: str) -> list[str]:
    passages = []
    for pattern in (r'"([^"]{50,}?)"', r"“([^”]{50,}?)”"):
        passages.extend(match.strip() for match in re.findall(pattern, text))
    return passages


def strip_inline_artifacts(text: str, placeholders: dict[int, str]) -> str:
    text = replace_source_placeholders(text, placeholders)
    text = clean_markdown(text)
    text = LABEL_PREFIX_RE.sub("", text)
    text = re.sub(r"\b(?:Sentence|Paragraph|Para)\s+\d+(?:\s*\([^)]{1,80}\))?\s*:", "", text, flags=re.I)
    text = re.sub(r"\bDrafting\s+P\d+\s*:", "", text, flags=re.I)
    text = re.sub(r"\bRefining [^:]+:", "", text, flags=re.I)
    text = re.sub(r"\b(?:Check|Length|Tone|Citations?)\b[^.]*\.", "", text, flags=re.I)
    text = re.sub(r"\bSelf-?correction\b[^.]*\.", "", text, flags=re.I)
    text = re.sub(r"\b(?:Wait|Actually|Correction)\b[^.]*\.", "", text, flags=re.I)
    text = re.sub(r"\s*->\s*[^.]+\.?", "", text)
    text = re.sub(r"\.\s*\*?(?:Good|Strong|Stronger|Polished|Excellent)[^.\n]*\.", ".", text, flags=re.I)
    text = strip_meta_parentheticals(text)
    text = text.strip().strip('"“”').lstrip(":-* ").rstrip()
    text = re.sub(r"\s+", " ", text).strip()
    return text


THOUGHT_BLOCK_OPENERS = (
    "check:",
    "check -",
    "opening:",
    "body:",
    "closing:",
    "draft:",
    "constraint check:",
    "refinement:",
    "refining ",
    "refining the",
    "refining paragraph",
    "citation adjustment:",
    "citation fix:",
    "citation issue:",
    "word count",
    "length check",
    "tone check",
    "citation check",
    "let's see",
    "aiming for ~",
    "p1:",
    "p2:",
    "p3:",
    "p4:",
    "total: ~",
    "total:",
    "passes >",
    "meets >",
    "drafting the text",
    "drafting...",
    "mental check",
    "checking buzzwords",
    "checking flow",
    "checking citations",
    "adding detail",
    "adding criticality",
    "adding citation",
    "refining for",
    "initial thought",
    "i initially thought",
    "i need to",
    "i should",
    "i'll",
    "i will ensure",
    "start by",
    "start with",
    "begin with the",
    "now, the design",
    "let's make it",
    "let's try",
    "let me ",
    ":* ",
    ".*:",
)

THOUGHT_BLOCK_PATTERNS = [
    re.compile(r"^\s*\*?\s*(?:check|opening|body|closing|constraint check|draft|citation|refinement|refining|word count|aiming for|total|passes?|meets?)\s*[:\-]", re.I),
    re.compile(r"^\s*P\d+:\s*~?\d+\s*words?", re.I),
    re.compile(r"^\s*Total:\s*~?\d+\s*words?", re.I),
    re.compile(r"(?:did I use|no buzzwords|no \"leverage\"|no \"robust\"|no \"paradigm\")", re.I),
    re.compile(r"(?:human researcher tone\?|mix of sentence lengths\?|no meta-commentary\?|citations =)", re.I),
    re.compile(r"(?:passes? >[=\s]*\d+|meets? >[=\s]*\d+)", re.I),
]


def is_thought_block(text: str) -> bool:
    """Return True if this paragraph is clearly an AI reasoning/planning note."""
    stripped = text.strip()
    lowered = stripped.lower()

    # Short lines that are just labels
    if len(stripped.split()) < 6 and stripped.endswith(":"):
        return True

    # Match known thought-block openers
    for opener in THOUGHT_BLOCK_OPENERS:
        if lowered.lstrip("*- ").startswith(opener):
            return True

    # Match regex patterns for self-evaluation lines
    for pattern in THOUGHT_BLOCK_PATTERNS:
        if pattern.search(stripped):
            return True

    # Paragraphs that look like a checklist of "Yes/No" answers
    yes_no_count = len(re.findall(r"\?\s+(?:Yes|No)\b", stripped, re.I))
    if yes_no_count >= 2:
        return True

    return False


def looks_like_outline_fragment(text: str) -> bool:
    lowered = text.lower()
    words = text.split()
    if len(words) < 8:
        return True
    if text.count(",") >= 2 and len(words) <= 22:
        if not re.search(
            r"\b(is|are|was|were|be|been|being|has|have|had|can|may|might|must|"
            r"should|could|would|will|does|do|did|suggests|reveals|indicates|"
            r"supports|requires|depends|demonstrates|enhances|improves)\b",
            lowered,
        ):
            return True
    segments = [segment.strip() for segment in re.split(r"[.;]", text) if segment.strip()]
    if len(segments) >= 3:
        average_words = sum(len(segment.split()) for segment in segments) / len(segments)
        if average_words < 9:
            return True
    return False


def looks_like_meta(text: str) -> bool:
    lowered = text.lower()
    if META_PREFIX_RE.match(text):
        return True
    if any(term in lowered for term in PROMPT_TERMS):
        return True
    if re.search(r"\bi\s+(?:will|need|must|should|can|treat|refine|use|convert)\b", lowered):
        return True
    return False


def publishable_paragraph(text: str, title: str) -> bool:
    lowered = text.lower().strip()
    words = text.split()

    if not text or len(words) < 12:
        return False
    if lowered in EVALUATIVE_SHORT_LINES:
        return False
    if lowered.rstrip(".") == clean_markdown(title).lower().rstrip("."):
        return False
    if is_thought_block(text):
        return False
    if looks_like_meta(text):
        return False
    if any(lowered.startswith(opener) for opener in INSTRUCTION_OPENERS):
        return False
    if "source " in lowered:
        return False
    if "..." in text:
        return False
    if looks_like_outline_fragment(text):
        return False
    if not text.endswith((".", "?", "!")):
        return False
    return True



def split_candidate_paragraph(paragraph: str, placeholders: dict[int, str]) -> list[str]:
    paragraph = paragraph.strip()
    if not paragraph:
        return []

    quoted = extract_quoted_passages(paragraph)
    if len(quoted) >= 2:
        normalized_quotes = []
        for chunk in quoted:
            cleaned = strip_inline_artifacts(chunk, placeholders)
            if cleaned and cleaned[-1] not in ".?!":
                cleaned = f"{cleaned}."
            if cleaned:
                normalized_quotes.append(cleaned)
        joined = " ".join(normalized_quotes)
        return [joined] if joined else []

    pieces = []
    if INLINE_LABEL_RE.search(paragraph):
        marked = INLINE_LABEL_RE.sub(lambda match: f"\n{match.group(0)}", paragraph).strip()
        for chunk in marked.splitlines():
            cleaned = strip_inline_artifacts(chunk, placeholders)
            if cleaned and cleaned[-1] not in ".?!":
                cleaned = f"{cleaned}."
            if cleaned:
                pieces.append(cleaned)
    else:
        cleaned = strip_inline_artifacts(paragraph, placeholders)
        if cleaned and cleaned[-1] not in ".?!":
            cleaned = f"{cleaned}."
        if cleaned:
            pieces.append(cleaned)

    if len(pieces) > 1 and all(len(piece.split()) < 90 for piece in pieces):
        joined = " ".join(pieces)
        return [joined] if joined else []

    return pieces


def normalize_similarity(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def dedupe_paragraphs(paragraphs: list[str]) -> list[str]:
    kept = []
    normalized = []

    for paragraph in paragraphs:
        norm = normalize_similarity(paragraph)
        if not norm:
            continue

        duplicate = False
        for seen in normalized:
            if norm == seen or norm in seen or seen in norm:
                duplicate = True
                break
            if SequenceMatcher(None, norm, seen).ratio() >= 0.93:
                duplicate = True
                break

        if duplicate:
            continue

        kept.append(paragraph)
        normalized.append(norm)

    return kept


def sanitize_section_body(heading: str, content: str, title: str, placeholders: dict[int, str]) -> list[str]:
    content = re.sub(rf"^\s*##\s*{re.escape(heading)}\s*$", "", content, flags=re.I | re.M).strip()
    raw_paragraphs = [block.strip() for block in re.split(r"\n\s*\n", content) if block.strip()]

    cleaned = []
    for raw in raw_paragraphs:
        if raw.startswith("#"):
            continue
        for candidate in split_candidate_paragraph(raw, placeholders):
            candidate = re.sub(r"\s+", " ", candidate).strip()
            if publishable_paragraph(candidate, title):
                cleaned.append(candidate)

    return dedupe_paragraphs(cleaned)


def fallback_paragraphs(raw_text: str, title: str, placeholders: dict[int, str]) -> list[str]:
    cleaned = []
    for raw in re.split(r"\n\s*\n", raw_text):
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            continue
        for candidate in split_candidate_paragraph(raw, placeholders):
            if publishable_paragraph(candidate, title):
                cleaned.append(candidate)
    return dedupe_paragraphs(cleaned)


def build_sections(data: dict) -> list[tuple[str, list[str]]]:
    title = data.get("title", data.get("topic", "Research Document"))
    placeholders = source_placeholder_map(data)
    sections = []

    for section in data.get("sections", []):
        heading = (section.get("heading") or "").strip()
        content = section.get("content") or ""
        if not heading or not content.strip():
            continue
        paragraphs = sanitize_section_body(heading, content, title, placeholders)
        if paragraphs:
            sections.append((heading, paragraphs))

    if sections:
        return sections

    raw_text = data.get("markdown") or data.get("result") or ""
    paragraphs = fallback_paragraphs(raw_text, title, placeholders)
    if paragraphs:
        return [("Body", paragraphs)]
    return []


def normalize_reference_entry(text: str) -> str:
    text = clean_markdown(text)
    for raw, label in DOMAIN_LABELS.items():
        text = re.sub(rf"^{re.escape(raw)}", label, text, flags=re.I)
        text = re.sub(re.escape(raw), label, text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_references(data: dict) -> list[str]:
    entries = []

    for source in data.get("sources", []):
        hint = source.get("citation_hint")
        if hint:
            entries.append(normalize_reference_entry(hint))

    if not entries:
        raw = data.get("references", "")
        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            entries.append(normalize_reference_entry(line))

    return dedupe_paragraphs(entries)


def reference_heading(reference_style: str) -> str:
    lowered = (reference_style or "").lower()
    if "mla" in lowered:
        return "Works Cited"
    if "chicago" in lowered or "turabian" in lowered:
        return "Bibliography"
    return "References"


def escape_for_pdf(text: str) -> str:
    return escape(text).replace("\n", " ")


def make_styles():
    return {
        "title": ParagraphStyle(
            "title",
            fontName="Times-Bold",
            fontSize=14,
            leading=24,
            textColor=black,
            alignment=TA_CENTER,
            spaceAfter=18,
        ),
        "section": ParagraphStyle(
            "section",
            fontName="Times-Bold",
            fontSize=12,
            leading=24,
            textColor=black,
            alignment=TA_CENTER,
            spaceBefore=12,
            spaceAfter=12,
        ),
        "body": ParagraphStyle(
            "body",
            fontName="Times-Roman",
            fontSize=12,
            leading=24,
            textColor=black,
            alignment=TA_LEFT,
            firstLineIndent=0.5 * inch,
            spaceAfter=0,
            splitLongWords=1,
        ),
        "abstract": ParagraphStyle(
            "abstract",
            fontName="Times-Roman",
            fontSize=12,
            leading=24,
            textColor=black,
            alignment=TA_LEFT,
            firstLineIndent=0,
            spaceAfter=0,
            splitLongWords=1,
        ),
        "reference": ParagraphStyle(
            "reference",
            fontName="Times-Roman",
            fontSize=12,
            leading=24,
            textColor=black,
            alignment=TA_LEFT,
            firstLineIndent=-0.5 * inch,
            leftIndent=0.5 * inch,
            spaceAfter=0,
            splitLongWords=1,
        ),
    }


def draw_page_number(canvas, doc):
    canvas.saveState()
    width, height = LETTER
    canvas.setFont("Times-Roman", 12)
    canvas.drawRightString(width - doc.rightMargin, height - 0.5 * inch, str(doc.page))
    canvas.restoreState()


def build_pdf(data: dict, output_path: str):
    title = clean_markdown(data.get("title", data.get("topic", "Research Document")))
    reference_style = data.get("reference_style", "")
    sections = build_sections(data)
    references = build_references(data)

    doc = SimpleDocTemplate(
        output_path,
        pagesize=LETTER,
        leftMargin=1 * inch,
        rightMargin=1 * inch,
        topMargin=1 * inch,
        bottomMargin=1 * inch,
        title=title,
        author="Research Bot",
        subject=data.get("topic", title),
        creator="research-bot",
    )

    styles = make_styles()
    story = [Spacer(1, 0.25 * inch), Paragraph(escape_for_pdf(title), styles["title"])]

    abstract_section = None
    body_sections = []
    for heading, paragraphs in sections:
        if heading.lower() == "abstract":
            abstract_section = paragraphs
        else:
            body_sections.append((heading, paragraphs))

    if abstract_section:
        story.append(Paragraph("Abstract", styles["section"]))
        for paragraph in abstract_section:
            story.append(Paragraph(escape_for_pdf(paragraph), styles["abstract"]))
        story.append(PageBreak())

    for heading, paragraphs in body_sections:
        story.append(Paragraph(escape_for_pdf(heading), styles["section"]))
        for paragraph in paragraphs:
            story.append(Paragraph(escape_for_pdf(paragraph), styles["body"]))

    if references:
        story.append(PageBreak())
        story.append(Paragraph(escape_for_pdf(reference_heading(reference_style)), styles["section"]))
        for entry in references:
            story.append(Paragraph(escape_for_pdf(entry), styles["reference"]))

    if not sections and not references:
        story.append(Paragraph("No document body was available for PDF rendering.", styles["body"]))

    doc.build(story, onFirstPage=draw_page_number, onLaterPages=draw_page_number)
    print(f"PDF saved: {output_path}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 scripts/generate_pdf.py <json_file>")
        sys.exit(1)

    input_path = sys.argv[1]
    with open(input_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    output_path = input_path.replace(".json", ".pdf")
    build_pdf(payload, output_path)
