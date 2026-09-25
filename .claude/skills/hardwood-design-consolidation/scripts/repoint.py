#!/usr/bin/env python3
"""Remove consolidated legacy design docs and repoint every reference to them.

Usage: repoint.py SOURCE=TARGET [SOURCE=TARGET ...]   (names without .md)

For each SOURCE: `git rm _designs-legacy/SOURCE.md`, then rewrite references in every
tracked file:
- `_designs-legacy/SOURCE.md` anywhere            -> `_designs/TARGET.md`
- link text `[SOURCE.md]` / `[SOURCE]` anywhere    -> `[TARGET.md]` / `[TARGET]`
- inside _designs-legacy/: `](SOURCE.md`           -> `](../_designs/TARGET.md`
                           a bare `SOURCE.md`      -> `_designs/TARGET.md`
Run from the repository root of the area worktree, after the targets are written: every
SOURCE must exist in _designs-legacy/ and every TARGET in _designs/, or nothing is changed.
Prints the files changed, every line where one target is now named twice (e.g. two
sources that merged into one target were listed side by side), and every link whose
`#anchor` pointed into a source and now points into its target; fix those by hand.
"""
import os
import re
import subprocess
import sys


def main():
    mapping = {}
    for arg in sys.argv[1:]:
        source, _, target = arg.partition('=')
        if not source or not target:
            sys.exit(f'bad mapping: {arg!r}, expected SOURCE=TARGET')
        mapping[source.removesuffix('.md')] = target.removesuffix('.md')
    if not mapping:
        sys.exit(__doc__)

    missing = [f'_designs-legacy/{s}.md' for s in mapping if not os.path.isfile(f'_designs-legacy/{s}.md')]
    missing += sorted({f'_designs/{t}.md' for t in mapping.values() if not os.path.isfile(f'_designs/{t}.md')})
    if missing:
        sys.exit('missing, nothing changed: ' + ', '.join(missing))

    for source in mapping:
        subprocess.check_call(['git', 'rm', '-q', f'_designs-legacy/{source}.md'])

    alt = '|'.join(re.escape(s) for s in mapping)
    listed = subprocess.run(['git', 'grep', '--untracked', '-lE', rf'({alt})\.md'],
                            capture_output=True, text=True).stdout.split()
    for path in listed:
        if path == '_plans/DESIGN_DOC_CONSOLIDATION.md':
            continue  # the plan names every legacy document on purpose
        with open(path, encoding='utf-8') as f:
            text = f.read()
        report_anchors(path, text, alt)
        new = re.sub(rf'_designs-legacy/({alt})\.md',
                     lambda m: f'_designs/{mapping[m.group(1)]}.md', text)
        new = re.sub(rf'\[({alt})(\.md)?\]', lambda m: f'[{mapping[m.group(1)]}{m.group(2) or ""}]', new)
        if path.startswith('_designs-legacy/'):
            new = re.sub(rf'\]\(({alt})\.md', lambda m: f'](../_designs/{mapping[m.group(1)]}.md', new)
            new = re.sub(rf'(?<![/\w])({alt})\.md', lambda m: f'_designs/{mapping[m.group(1)]}.md', new)
        if new != text:
            with open(path, 'w', encoding='utf-8') as f:
                f.write(new)
            print('repointed', path)
            report_duplicates(path, new, set(mapping.values()))


def report_anchors(path, text, alt):
    """Report a reference into a section of a source; the target has sections of its own."""
    for i, line in enumerate(text.split('\n')):
        for m in re.finditer(rf'(?<![\w])({alt})\.md#[\w-]+', line):
            print(f'  check {path}:{i + 1}: anchor {m.group(0)} now points into the target')


def report_duplicates(path, text, targets):
    """Report a line (joined with the next) that mentions one target twice, not counting
    the target half of a Markdown link, which repeats the link text by construction."""
    lines = text.split('\n')
    for i, line in enumerate(lines):
        window = re.sub(r'\]\([^)]*\)', ']', line + ' ' + (lines[i + 1] if i + 1 < len(lines) else ''))
        for target in targets:
            if window.count(f'{target}.md') > 1:
                print(f'  check {path}:{i + 1}: {target}.md named twice')


if __name__ == '__main__':
    main()
