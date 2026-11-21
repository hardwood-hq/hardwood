#!/usr/bin/env bash
#
# Runs on the N300 as `build`, in a login shell (SDKMan's JDK): installs Hardwood at REF as a
# snapshot, times the quick regression profile (or the named scripts) on the pinned clock, and
# prints the run against the latest nightly results of the same machine.
#
# Usage: quick-vs-nightly.sh REF [METRIC] [SCRIPTS]
#   REF      a commit of ~/projects/hardwood (the skill pushes it there first)
#   METRIC   wall (default), cpu, alloc or live
#   SCRIPTS  comma-separated regression scripts (e.g. nested-columns,nested) to time with the
#            nightly preset instead of the whole quick profile: all of their contenders, as
#            the nightly job times them
#
# Its log ends in a line starting DONE, REFUSED or FAILED. Everything it keeps lives under
# ~/scratch/quick-vs-nightly; ~/nightly belongs to the nightly job and is only locked, never
# written.
set -uo pipefail

# Started in the background of an SSH command, the job runs at shell level 1 with SSH_CLIENT
# set, so every `bash -c` below it takes itself for a shell sshd started and sources ~/.bashrc,
# whose `cd /home/build` moves it out of its directory: run-regression.sh then records no heap
# flags in the manifest, and the run matches no nightly machine epoch.
unset SSH_CLIENT SSH_CONNECTION SSH_TTY

REF="${1:?usage: quick-vs-nightly.sh REF [METRIC]}"
METRIC="${2:-wall}"
SCRIPTS="${3:-}"
if [ -n "$SCRIPTS" ]; then PROFILE=(--profile nightly --only "$SCRIPTS"); else PROFILE=(--profile quick); fi
HOME_DIR="$HOME/scratch/quick-vs-nightly"
HW="$HOME/projects/hardwood"
BENCH="$HOME_DIR/hardwood-benchmarks"
RESULTS="$HOME_DIR/hardwood-benchmark-results"
CPU=/sys/devices/system/cpu/cpu0/cpufreq
mkdir -p "$HOME_DIR/runs"

log() { echo "$(date +%T) $*"; }
refuse() { echo "REFUSED: $*"; exit 3; }
fail() { echo "FAILED: $*"; exit 1; }

# The nightly job starts at 03:00 and may run three hours: stay out of its way.
now=$((10#$(date +%H%M)))
(( now >= 230 && now < 630 )) && refuse "inside the nightly job's window (02:30-06:30 box time)"
pgrep -x java > /dev/null && refuse "a JVM runs on the box: $(ps -eo args= -q "$(pgrep -d, -x java)" | cut -c1-100 | head -2 | tr '\n' ';')"
[ "$(cat $CPU/scaling_max_freq)" = "$(cat $CPU/cpuinfo_max_freq)" ] || refuse "the clock is pinned by someone else"
mkdir -p "$HOME/nightly"
exec 9> "$HOME/nightly/lock"
flock -n 9 || refuse "the nightly job holds its lock"

# Hardwood at REF, installed as <version>-q<sha>-SNAPSHOT unless it is already.
sha="$(git -C "$HW" rev-parse --short=7 "$REF^{commit}" 2>/dev/null)" || fail "no commit $REF in $HW"
base="$(git -C "$HW" show "$sha:pom.xml" | sed -n 's|^  <version>\(.*\)</version>|\1|p' | head -1)"
VERSION="${base%-SNAPSHOT}-q$sha-SNAPSHOT"
if [ ! -f "$HOME/.m2/repository/dev/hardwood/hardwood-s3/$VERSION/hardwood-s3-$VERSION.jar" ]; then
  log "building $VERSION"
  git -C "$HW" reset -q --hard && git -C "$HW" checkout -q --detach "$sha" || fail "cannot check out $sha"
  (
    cd "$HW" \
      && timeout 600 ./mvnw -q -B versions:set -DnewVersion="$VERSION" -DgenerateBackupPoms=false \
      && timeout 900 ./mvnw -q -B -pl core,s3 -am install -DskipTests -Dquick
  ) > "$HOME_DIR/build-$sha.log" 2>&1
  rc=$?
  git -C "$HW" reset -q --hard
  (( rc == 0 )) || fail "the build of $sha failed, see $HOME_DIR/build-$sha.log"
fi
log "Hardwood $REF is $VERSION"

# The benchmarks as the nightly job runs them, and the latest results.
if [ -d "$BENCH/.git" ]; then git -C "$BENCH" fetch -q origin; else git clone -q https://github.com/hardwood-hq/hardwood-benchmarks.git "$BENCH"; fi
git -C "$BENCH" checkout -q --detach "${BENCH_REF:-origin/main}" && git -C "$BENCH" clean -q -fd -e target \
  || fail "cannot check out the benchmarks"
[ -f "$BENCH/charts/against-history.py" ] || fail "the benchmarks at ${BENCH_REF:-origin/main} have no charts/against-history.py"

# The Overture places file of the nested scripts: the nightly checkout's, linked rather than
# downloaded a second time (740 MB), so a run reads the release the nightly reads. The benchmarks
# download only a missing file, so nothing writes through the links.
DATA=target/overture-maps-data
if [ -s "$HOME/nightly/hardwood-benchmarks/$DATA/overture_places.zstd.parquet" ]; then
  mkdir -p "$BENCH/$DATA"
  for f in overture_places.zstd.parquet overture_places.zstd.parquet.release; do
    rm -f "$BENCH/$DATA/$f"
    [ -e "$HOME/nightly/hardwood-benchmarks/$DATA/$f" ] && ln -s "$HOME/nightly/hardwood-benchmarks/$DATA/$f" "$BENCH/$DATA/$f"
  done
fi

# Measure on the pinned clock, and restore it whatever happens.
pgrep -x java > /dev/null && refuse "a JVM started on the box meanwhile"
sudo -n profiling-setup pin > /dev/null || refuse "the clock could not be pinned"
restore() {
  if pgrep -x java > /dev/null; then log "a JVM still runs: the clock stays pinned"; else sudo -n profiling-setup restore > /dev/null && log "clock restored"; fi
}
trap restore EXIT
log "clock pinned at $(cat $CPU/scaling_max_freq) kHz; timing ${SCRIPTS:-the quick profile}"
OUT="$HOME_DIR/runs/$sha-$(date +%Y%m%d-%H%M)"
( cd "$BENCH" && bash ./run-regression.sh "${PROFILE[@]}" --out "$OUT" "$VERSION" ) > "$OUT.log" 2>&1
log "timed: $(tail -1 "$OUT/progress.log" 2>/dev/null)"
[ -s "$OUT/manifest.json" ] || fail "the run recorded nothing, see $OUT.log"

if [ -d "$RESULTS/.git" ]; then
  git -C "$RESULTS" fetch -q origin && git -C "$RESULTS" reset -q --hard origin/main
else
  git clone -q https://github.com/hardwood-hq/hardwood-benchmark-results.git "$RESULTS"
fi || fail "cannot fetch the results"

echo
( cd "$BENCH" && python3 charts/against-history.py "$OUT" "$VERSION" --results "$RESULTS" --metric "$METRIC" )
if [ -s "$OUT/not-run.txt" ]; then
  echo
  echo "Not run:"
  cat "$OUT/not-run.txt"
fi
echo
echo "DONE $OUT"
