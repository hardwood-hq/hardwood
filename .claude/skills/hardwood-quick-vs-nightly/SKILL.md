---
name: hardwood-quick-vs-nightly
description: Time a Hardwood branch on the N300, with the quick regression profile or with one or more named regression benchmarks, and compare it with the latest nightly results of the same machine, as a table of latest nightly value (with its commit), this run and the delta per contender. Use whenever the user asks to "run the quick profile", "check my branch for regressions", "compare against nightly", "quick vs nightly", "how does this branch compare with main's nightly numbers", "run the nested (or filter, s3, …) benchmark against nightly", or wants a regression check of committed work before opening or merging a PR.
---

# Quick profile against the nightly history

Times the committed `HEAD` of the current Hardwood checkout on the N300, then shows each
contender against the latest nightly value of the same contender, definition and machine from
`hardwood-benchmark-results`. A snapshot build of a new commit takes about 3 minutes; then:

- **the `quick` profile** (default): the contenders covering the core read and write paths,
  about 9 minutes;
- **named benchmarks**, for work on one area: every contender of the named regression scripts,
  with the preset the nightly job uses, so they compare exactly with their nightly series. A
  script takes from about 1 to 6 minutes on main; pick it from the change:

  | Area of the change | Script |
  |---|---|
  | nested columns, nested levels, dictionary-backed binary values | `nested-columns` |
  | nested records through the row reader | `nested` |
  | flat full scans, column and row readers | `flat`, `narrow` |
  | plain byte arrays | `binary` |
  | filters and pruning; dictionary filtering; time windows; Bloom filters | `filter`; `dictfilter`; `window`; `bloom` |
  | 1BRC-style aggregation by dictionary id | `1brc` |
  | fixed-size lists | `fixedlist` |
  | writing; float encodings | `write`; `encodings` |
  | S3 fetching | `s3` |

  The contenders of each script and what each covers are listed in hardwood-benchmarks'
  `_designs/BENCHMARK_WORKLOAD_COVERAGE.md` ("Regression contenders"). When unsure, ask
  which area the user wants, or run the quick profile.

How the regression suite works, and what a contender, epoch and machine epoch are, is in
hardwood-benchmarks' `_designs/REGRESSION_SUITE.md`. The box's access, layout and sharing
rules are the `n300-profiling` skill's; follow them.

## What it compares, and what it does not

- **Committed work only.** The box builds what is pushed to it. If `git status --porcelain`
  lists modified tracked files, say that the uncommitted changes would not be measured and
  ask whether to commit first or measure `HEAD` as it is.
- **The delta is a hint.** The latest nightly value is a single row, so a delta is called
  worse or better only beyond a fixed band (5 %), not from both sides' spread. A worse call
  is a reason to look closer: rerun, or compare the two commits side by side with
  `./run-regression.sh --profile nightly --only <script> --rounds 2 <before> <after>` on the
  box. Never present it as a confirmed regression.
- **Same machine only.** The table compares within one machine epoch (CPU, JDK, heap flags,
  clock). If the box's JDK changed since the last night, the script says there is no history
  on this machine.
- **A contender whose definition differs** (the branch changes a benchmark's preset, method or
  fixture) has no nightly series to compare with, and the table says so.

## Steps

1. **Check the checkout.** In the Hardwood worktree the user works in:
   `git status --porcelain` (see above) and `git rev-parse --short HEAD`. Note the branch and
   commit for the summary.

2. **Push the commit to the box's Hardwood checkout**, to a ref of its own (not the checked-out
   branch):

   ```sh
   git remote get-url box >/dev/null 2>&1 || git remote add box "build@<n300_host>:projects/hardwood"
   GIT_SSH_COMMAND="ssh -i /workspace/_tmp/n300-claude" git push -f box HEAD:refs/heads/quick-vs-nightly
   ```

3. **Copy the script and start it detached**, then poll its log. It refuses rather than waits
   when the box is busy, inside the nightly window (02:30–06:30 box time) or held by the
   nightly job, so report a refusal and stop.

   ```sh
   K=/workspace/_tmp/n300-claude
   ssh -i $K build@<n300_host> 'mkdir -p ~/scratch/quick-vs-nightly'
   scp -i $K .claude/skills/hardwood-quick-vs-nightly/scripts/quick-vs-nightly.sh build@<n300_host>:scratch/quick-vs-nightly/
   ssh -i $K build@<n300_host> 'setsid nohup bash -lc "bash ~/scratch/quick-vs-nightly/quick-vs-nightly.sh <sha> wall" > ~/scratch/quick-vs-nightly/last.out 2>&1 < /dev/null & disown; sleep 5; cat ~/scratch/quick-vs-nightly/last.out'
   ```

   `wall` is the metric; pass `cpu`, `alloc` or `live` when the user asks about those (an
   allocation change, say, shows best as `alloc`). To time named benchmarks instead of the
   quick profile, add them as a third argument: `… quick-vs-nightly.sh <sha> alloc nested-columns`.
   Say in chat that the script pins the clock for the run and restores it.

4. **Wait for the result** with a background wait on the log's last line, not by polling in
   the foreground; tell the user the expected time and report progress if asked:

   ```sh
   until ssh -i $K build@<n300_host> 'grep -qE "^(DONE|REFUSED|FAILED)" ~/scratch/quick-vs-nightly/last.out'; do sleep 30; done
   ssh -i $K build@<n300_host> 'cat ~/scratch/quick-vs-nightly/last.out'
   ```

5. **Report.** Put the table into the reply itself, as the script printed it: it is Markdown,
   and the session renders a reply's Markdown as a table, while a command's output, or a table
   inside a code block, shows as its source. Then, in a few lines: how many
   contenders are worse beyond the band, which ones, with their latest nightly commit, and any
   "not settled", "bimodal" or "no nightly history" rows with what they mean. Mention what was
   not run, and that the clock was restored (the log's `clock restored` line); if the log says
   the clock stays pinned, say so.

## Failure modes

| Log ends in | Meaning | Do |
|---|---|---|
| `REFUSED: inside the nightly job's window` | 02:30–06:30 box time | Offer to run after 06:30. |
| `REFUSED: a JVM runs on the box` / `the clock is pinned by someone else` | another session measures | Report what runs; don't touch it. |
| `REFUSED: the nightly job holds its lock` | a night or backfill is running | Report it; `ssh … 'tail -3 ~/nightly/backfill.out'` shows a backfill's progress. |
| `FAILED: the build of <sha> failed` | the commit does not build | Show the tail of the build log it names. |
| `FAILED: the benchmarks … have no charts/against-history.py` | hardwood-benchmarks' `main` predates the comparison script | Rerun with `BENCH_REF=origin/<branch>` in the `bash -lc` environment. |
