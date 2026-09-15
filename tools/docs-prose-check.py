#!/usr/bin/env python3
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# tools/docs-prose-check.py — flag filler phrasing in the user docs.
#
# Usage:
#   python3 tools/docs-prose-check.py [docs-dir]
#
# Scans every Markdown page under docs/content (or the given directory) outside code blocks and
# HTML comments and exits non-zero on a match. The checks cover phrasing that carries no fact:
# filler intensifiers, lead-ins that announce a point instead of making it, questions put to the
# reader, and pages labelling their own kind. Matching runs over whole paragraphs, list items,
# headings and table cells, so a phrase broken across a line wrap is still found. A page whose
# em-dash density exceeds MAX_DASHES_PER_1000_WORDS also fails. A paragraph that legitimately needs
# a flagged phrase can carry `<!-- prose-check: ignore -->` on any of its lines. The body of an
# `!!! example` admonition, which links a runnable example, may open with a question.

import pathlib
import re
import sys

MAX_DASHES_PER_1000_WORDS = 15
MIN_WORDS_FOR_DASH_CHECK = 300
IGNORE_MARKER = "<!-- prose-check: ignore -->"

PHRASE_RULES = [
    (r"\b(genuinely|simply|actually)\b",
     "filler intensifier; delete it"),
    (r"\b(is|are) worth (knowing|noting|naming|mentioning|remembering)\b|\bworth (knowing|noting) (about|that)\b|\bconsequences are worth\b",
     "announces a point; state the point"),
    (r"\b(note that|keep in mind|it is important to|importantly|crucially)\b",
     "announces a point; state the point"),
    (r"\b(the whole vocabulary|hold one idea)\b",
     "slogan; state the fact"),
    (r"\b(in short|in summary|in other words|put simply|let's|here's)\b",
     "signpost; state the fact"),
    (r"\b(look-it-up reference|this page (explains|describes|covers|is the))\b",
     "page labels itself; start with its first fact"),
]
RULES = [(re.compile(pattern, re.IGNORECASE), message) for pattern, message in PHRASE_RULES]
QUESTION_RULE = (re.compile(r"\w[)\"'*_]*\?(\s|$)"), "question to the reader; state what the link or step offers")

LIST_ITEM = re.compile(r"^([-*+]|\d+\.)\s")


def prose_lines(text):
    in_code = False
    in_comment = False
    for number, line in enumerate(text.split("\n"), 1):
        stripped = line.strip()
        if in_comment:
            in_comment = "-->" not in stripped
            continue
        if stripped.startswith("<!--") and "-->" not in stripped:
            in_comment = True
            continue
        if stripped.startswith("```"):
            in_code = not in_code
            continue
        if in_code:
            continue
        yield number, line


def prose_units(text):
    """Yields (first line number, text, whether a question is allowed) for each paragraph, list item,
    heading and table cell."""
    unit_start = None
    unit_lines = []
    in_example = False

    def flush():
        if unit_lines:
            yield unit_start, " ".join(unit_lines), in_example
        unit_lines.clear()

    for number, line in prose_lines(text):
        stripped = line.strip()
        if stripped and not line.startswith(" "):
            in_example = False
        if not stripped or stripped.startswith(("!!!", "???")):
            yield from flush()
            if stripped.startswith("!!! example"):
                in_example = True
        elif stripped.startswith("|"):
            yield from flush()
            for cell in stripped.strip("|").split("|"):
                yield number, cell, False
        elif stripped.startswith("#"):
            yield from flush()
            yield number, stripped.lstrip("#"), False
        else:
            if LIST_ITEM.match(stripped):
                yield from flush()
            if not unit_lines:
                unit_start = number
            unit_lines.append(stripped)
    yield from flush()


def check_unit(unit, question_allowed):
    if IGNORE_MARKER in unit:
        return []
    without_code = re.sub(r"`[^`]*`", "CODE", unit)
    without_links = re.sub(r"\]\([^)]*\)", "]()", without_code)
    rules = RULES if question_allowed else RULES + [QUESTION_RULE]
    return [message for pattern, message in rules if pattern.search(without_links)]


def dash_density(text):
    words = 0
    dashes = 0
    for _, line in prose_lines(text):
        if line.strip().startswith("|"):
            continue
        words += len(line.split())
        dashes += re.sub(r"`[^`]*`", "", line).count("—")
    return words, dashes


def main():
    root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "docs/content")
    failures = 0
    for page in sorted(root.rglob("*.md")):
        text = page.read_text(encoding="utf-8")
        for number, unit, question_allowed in prose_units(text):
            for message in check_unit(unit, question_allowed):
                print(f"{page}:{number}: {message}: {unit.strip()[:120]}")
                failures += 1
        words, dashes = dash_density(text)
        if words >= MIN_WORDS_FOR_DASH_CHECK:
            density = dashes * 1000 / words
            if density > MAX_DASHES_PER_1000_WORDS:
                print(f"{page}: {dashes} em-dashes in {words} words ({density:.1f} per 1,000, "
                      f"limit {MAX_DASHES_PER_1000_WORDS}); rewrite asides as sentences, colons or parentheses")
                failures += 1
    if failures:
        print(f"{failures} prose finding(s)")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
