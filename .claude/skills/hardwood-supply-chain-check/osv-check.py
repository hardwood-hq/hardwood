#!/usr/bin/env python3
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# osv-check.py — query OSV for the runtime dependencies of the published modules.
#
# Usage, from the repository root:
#   ./mvnw -q -pl core,s3,aws-auth,avro,cli dependency:list -DincludeScope=runtime \
#       -DoutputFile=target/runtime-deps.txt
#   python3 -I .claude/skills/hardwood-supply-chain-check/osv-check.py */target/runtime-deps.txt
#
# Prints one line per vulnerable coordinate: the modules that ship it, then each advisory
# with its summary.

import json
import re
import sys
import urllib.request
from pathlib import Path

LINE = re.compile(r"\s+([^:\s]+):([^:\s]+):[^:\s]+(?::[^:\s]+)?:([^:\s]+):(compile|runtime)\b")


def post(url, body):
    request = urllib.request.Request(url, json.dumps(body).encode(), {"Content-Type": "application/json"})
    with urllib.request.urlopen(request) as response:
        return json.load(response)


def get(url):
    with urllib.request.urlopen(url) as response:
        return json.load(response)


def main(files):
    modules = {}
    for f in files:
        module = Path(f).parts[0]
        for line in Path(f).read_text().splitlines():
            m = LINE.match(line)
            if m:
                modules.setdefault((f"{m[1]}:{m[2]}", m[3]), set()).add(module)
    if not modules:
        sys.exit("no dependencies found; was dependency:list run with a relative -DoutputFile?")
    coordinates = sorted(modules)
    queries = [{"package": {"ecosystem": "Maven", "name": n}, "version": v} for n, v in coordinates]
    results = post("https://api.osv.dev/v1/querybatch", {"queries": queries})["results"]
    print(f"{len(coordinates)} coordinates checked")
    for (name, version), result in zip(coordinates, results):
        ids = [v["id"] for v in result.get("vulns", [])]
        if not ids:
            continue
        print(f"{name} {version} (in {', '.join(sorted(modules[(name, version)]))})")
        for vuln_id in ids:
            summary = get(f"https://api.osv.dev/v1/vulns/{vuln_id}").get("summary", "")
            print(f"  {vuln_id}  {summary}")


if __name__ == "__main__":
    main(sys.argv[1:])
