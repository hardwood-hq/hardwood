#!/usr/bin/env python3
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# tools/release/contributors.py — emit the alphabetical contributor list for a release range.
#
# Usage:
#   tools/release/contributors.py <since-tag> [<until-tag>]
#
# With two tags, lists contributors whose commits are in (<since>, <until>].
# With one tag, lists everyone up to and including <since> (initial release).
# Output is a Markdown line ready to paste into docs/content/release-notes.md.
#
# Resolution order for each unique author/co-author email in the range:
#   1. GitHub noreply pattern — login is encoded in the address.
#   2. A commit authored by that email — ask GitHub which login it mapped to.
#   3. search/users?q=<email>+in:email — only works if the user made their
#      email public on GitHub.
# Unresolved emails are reported on stderr.

import json
import re
import subprocess
import sys
from collections import OrderedDict

NOREPLY = re.compile(r'^(?:\d+\+)?(?P<login>[^@]+)@users\.noreply\.github\.com$')
COAUTHOR = re.compile(r'(?im)^co-authored-by:\s*[^<]+<([^>]+)>')


def run(cmd):
    return subprocess.check_output(cmd, text=True).strip()


def gh(path):
    return json.loads(run(['gh', 'api', path]))


def is_bot_login(login):
    return login.endswith('[bot]') or login in {'dependabot', 'github-actions'}


def emails_in_range(rev_range):
    authors = run(['git', 'log', rev_range, '--format=%ae']).splitlines()
    bodies = run(['git', 'log', rev_range, '--format=%b'])
    coauthors = COAUTHOR.findall(bodies)
    seen = set()
    for e in authors + coauthors:
        e = e.strip().lower()
        if not e or e in seen or e.endswith('@github.com'):
            continue
        seen.add(e)
        yield e


def login_for_email(rev_range, email, repo):
    m = NOREPLY.match(email)
    if m:
        return m.group('login')
    try:
        sha = run(['git', 'log', rev_range, f'--author={email}', '--format=%H', '-1'])
        if sha:
            data = gh(f'repos/{repo}/commits/{sha}')
            login = (data.get('author') or {}).get('login')
            if login:
                return login
    except subprocess.CalledProcessError:
        pass
    try:
        data = gh(f'search/users?q={email}+in:email')
        items = data.get('items') or []
        if items:
            return items[0]['login']
    except subprocess.CalledProcessError:
        pass
    return None


def main():
    if len(sys.argv) not in (2, 3):
        sys.exit('usage: tools/release/contributors.py <since-tag> [<until-tag>]')
    rev_range = sys.argv[1] if len(sys.argv) == 2 else f'{sys.argv[1]}..{sys.argv[2]}'

    repo = run(['gh', 'repo', 'view', '--json', 'nameWithOwner', '--jq', '.nameWithOwner'])

    contributors = OrderedDict()
    unresolved = []
    for email in emails_in_range(rev_range):
        login = login_for_email(rev_range, email, repo)
        if not login or is_bot_login(login):
            if not login:
                unresolved.append(email)
            continue
        if login in contributors:
            continue
        try:
            profile = gh(f'users/{login}')
        except subprocess.CalledProcessError:
            unresolved.append(email)
            continue
        contributors[login] = (profile.get('name') or login, profile['html_url'])

    items = sorted(contributors.values(), key=lambda nu: nu[0].lower())
    rendered = ', '.join(f'[{name}]({url})' for name, url in items)
    print(f'Thank you to all contributors to this release: {rendered}.')

    if unresolved:
        sys.stderr.write('warning: could not resolve emails: '
                         + ', '.join(unresolved) + '\n')


if __name__ == '__main__':
    main()
