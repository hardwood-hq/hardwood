#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Runs the predicate rule audit end to end; see README.md.
#
#   tools/predicate-audit/run.sh [report-dir]
#
# Builds Hardwood and the tool, writes the fixtures, derives their variants and runs every step.
# The report defaults to tools/predicate-audit/target/report.
set -euo pipefail

tool_dir="$(cd "$(dirname "$0")" && pwd)"
repo_dir="$(cd "$tool_dir/../.." && pwd)"
target="$tool_dir/target"
report="${1:-$target/report}"
fixtures="$target/fixtures"

cd "$repo_dir"
echo "== Building Hardwood and the audit tool"
# The classpath is resolved in the same reactor as the build, so Hardwood's modules resolve to
# the jars just packaged rather than to whatever the local repository last had installed. The
# relative output file resolves against each module's own directory.
timeout 180 ./mvnw -q -Ppredicate-audit -pl tools/predicate-audit -am package dependency:build-classpath \
    -DskipTests -Dquick -Dmdep.outputFile=target/classpath.txt -Dmdep.includeScope=runtime
classpath="$target/classes:$(cat "$target/classpath.txt")"
java_opts=(--enable-native-access=ALL-UNNAMED -cp "$classpath")

# Runs a step with its stderr in a log, which is named if the step fails.
run_logged() {
    local log="$1"
    shift
    if ! "$@" 2> "$log"; then
        echo "failed; stderr is in $log" >&2
        exit 1
    fi
}

echo "== Writing fixtures with parquet-java"
rm -rf "$fixtures"
run_logged "$target/fixtures-stderr.log" java "${java_opts[@]}" dev.hardwood.tools.predicateaudit.PredicateAudit fixtures "$fixtures"

echo "== Deriving footer-rewritten variants"
# The venv lives in the main checkout; a worktree of the repository has none of its own.
venv="$repo_dir/.docker-venv"
if [[ ! -d "$venv" ]]; then
    venv="$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")/.docker-venv"
fi
# shellcheck disable=SC1091
source "$venv/bin/activate"
python "$tool_dir/derive_fixtures.py" "$fixtures"

echo "== Auditing"
run_logged "$target/audit-stderr.log" java "${java_opts[@]}" dev.hardwood.tools.predicateaudit.PredicateAudit audit "$fixtures" "$report"
