#!/usr/bin/env python3
"""Checks an area's consolidation before it is committed. Run from the worktree root.

1. No tracked file references a design doc that exists in neither _designs/ nor
   _designs-legacy/ (i.e. a removed source); a planned target not written yet is pending.
2. Relative Markdown links in _designs/, _designs-legacy/, _plans/ and the root *.md files
   resolve. A link to a target of a later area (a file named in the plan's target table
   that does not exist yet) is reported as pending, not as a failure.
3. The area's changes to *.java (committed, staged or not, since the branch left
   origin/main) touch comments only.
Exits non-zero on a failure.
"""
import glob
import os
import re
import subprocess
import sys
from collections import Counter

PLAN = '_plans/DESIGN_DOC_CONSOLIDATION.md'


def existing_docs():
    return {os.path.basename(p) for p in glob.glob('_designs/*.md') + glob.glob('_designs-legacy/*.md')}


def planned_targets():
    with open(PLAN, encoding='utf-8') as f:
        return set(re.findall(r'^\| \d+ \| `([A-Z0-9_]+\.md)`', f.read(), re.M))


def check_references(existing, planned):
    failures, pending = [], []
    out = subprocess.run(['git', 'grep', '--untracked', '-nE', r'_designs(-legacy)?/[A-Z0-9_]+\.md'],
                         capture_output=True, text=True).stdout
    for line in out.splitlines():
        path = line.split(':', 1)[0]
        if path == PLAN or path.startswith('.claude/skills/hardwood-design-consolidation/'):
            continue
        for name in re.findall(r'_designs(?:-legacy)?/([A-Z0-9_]+\.md)', line):
            if name in existing:
                continue
            if name in planned:
                pending.append(f'{line[:160]}')
            else:
                failures.append(f'removed doc referenced: {line[:160]}')
    return failures, pending


def check_links(planned):
    failures, pending = [], []
    files = (glob.glob('_designs/*.md') + glob.glob('_designs-legacy/*.md')
             + glob.glob('_plans/*.md') + glob.glob('*.md'))
    for path in files:
        base = os.path.dirname(path)
        with open(path, encoding='utf-8') as f:
            text = f.read()
        for m in re.finditer(r'\]\(([^)#\s]+)(?:#[^)]*)?\)', text):
            target = m.group(1)
            if re.match(r'[a-z]+:', target):
                continue
            if os.path.exists(os.path.normpath(os.path.join(base, target))):
                continue
            entry = f'{path} -> {target}'
            (pending if os.path.basename(target) in planned else failures).append(entry)
    return failures, pending


def check_java_comment_only():
    base = subprocess.run(['git', 'merge-base', 'origin/main', 'HEAD'],
                          capture_output=True, text=True, check=True).stdout.strip()
    diff = subprocess.run(['git', 'diff', '-U0', base, '--', '*.java'],
                          capture_output=True, text=True, check=True).stdout
    removed, added = Counter(), Counter()
    for line in diff.splitlines():
        if not re.match(r'^[-+](?![-+])', line):
            continue
        code = re.sub(r'\s*//.*$', '', line[1:]).strip()  # a trailing comment is not code
        if code and not re.match(r'(\*|/\*)', code):
            (removed if line[0] == '-' else added)[code] += 1
    changed = (added - removed) + (removed - added)
    return [f'non-comment Java change: {c[:160]}' for c in changed]


def main():
    existing = existing_docs()
    planned = planned_targets()
    failures, pending = check_references(existing, planned)
    link_failures, link_pending = check_links(planned)
    pending += link_pending
    failures += [f'broken link: {e}' for e in link_failures]
    failures += check_java_comment_only()
    for p in pending:
        print(f'pending (later area): {p}')
    for f in failures:
        print(f'FAIL {f}')
    print(f'{len(failures)} failure(s), {len(pending)} pending link(s)')
    sys.exit(1 if failures else 0)


if __name__ == '__main__':
    main()
