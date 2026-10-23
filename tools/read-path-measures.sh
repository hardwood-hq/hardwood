#!/usr/bin/env bash
#
#  SPDX-License-Identifier: Apache-2.0
#
#  Copyright The original authors
#
#  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# read-path-measures.sh — the measures of _plans/READ_PATH_CONSOLIDATION.md for the read path's
# fetch and planning classes: lines of code (neither blank nor comments, imports included) and
# concurrency constructs (lines containing ReentrantLock, synchronized, Atomic*, CompletableFuture,
# volatile or tryLock), per class and in total.
#
# Usage: tools/read-path-measures.sh [REF]
#   REF   a commit to measure (default: the working tree)
#
# A class the plan's stages move code into or add goes into CLASSES in the PR that creates it, so
# every stage is measured over the same code. A class that no longer exists counts as zero.

set -euo pipefail
cd "$(dirname "$0")/.."

DIR=core/src/main/java/dev/hardwood/internal
CLASSES=(
  reader/RowGroupIterator reader/SequentialFetchPlan reader/IndexedFetchPlan reader/ChunkHandle
  reader/SharedRegion reader/RangeRead reader/PrefetchTasks reader/FileMetadataCache
  reader/FileOpenAhead reader/FetchedAhead reader/IndexWindow reader/CoalescedRanges
  reader/OnceCache reader/WorkItemPlanning reader/PageSource reader/ParquetMetadataReader
  reader/DictionaryParser predicate/RowGroupBloomFilterSource reader/PageFormatProbe
  reader/RowGroupIndexBuffers reader/ReadAheadBuffer reader/ReadExecutor DecodeExecutor
  reader/HardwoodContextImpl reader/SchemaCompatibility reader/RowGroupPruning reader/FetchPlanBuilder
  reader/ReadTasks reader/FetchSequence reader/EntryCursor reader/TailedInputFile reader/InternalProperty
)

REF=${1:-}

source_of() {
  if [[ -n "$REF" ]]; then
    git show "$REF:$1" 2>/dev/null || true
  elif [[ -f "$1" ]]; then
    cat "$1"
  fi
}

printf 'class\tcode\tconstructs\n'
total_code=0
total_constructs=0
for c in "${CLASSES[@]}"; do
  text=$(source_of "$DIR/$c.java")
  code=$(grep -cvE '^\s*(///|//|\*|/\*|$)' <<< "$text" || true)
  constructs=$(grep -cE 'ReentrantLock|synchronized|Atomic[A-Z]|CompletableFuture|volatile|tryLock' <<< "$text" || true)
  [[ -z "$text" ]] && code=0
  printf '%s\t%s\t%s\n' "${c##*/}" "$code" "$constructs"
  total_code=$((total_code + code))
  total_constructs=$((total_constructs + constructs))
done
printf 'total\t%s\t%s\n' "$total_code" "$total_constructs"
