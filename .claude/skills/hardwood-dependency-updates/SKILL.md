---
name: hardwood-dependency-updates
description: Triage Hardwood's open Dependabot pull requests into one pull request of grouped commits (library, CLI, build, test dependencies), hold back updates that break something with the reason recorded, verify the result locally and in CI, and close the Dependabot PRs it covers. Use whenever the user says "look at the Dependabot PRs", "update dependencies", "bump the dependencies", "group the Dependabot changes", or asks why a Dependabot PR fails.
---

# Hardwood dependency updates

Dependabot (`.github/dependabot.yml`) opens weekly PRs once a release is seven days old (`cooldown`; security updates come at once): one grouped PR for minor and patch Maven updates, one per major Maven update, one grouped PR for actions, and Docker base-image updates. Merging them one by one leaves `main` broken between merges and hides why an update was skipped. This skill folds them into one PR of a few commits, each a group a reviewer can judge on its own.

## 1. Collect

```bash
gh pr list --author app/dependabot --state open --json number,title,headRefName,files
gh pr diff <N>
```

Classify every updated artifact by where it ends up, checked in the POMs (scope, module, whether the module is deployed: `maven.deploy.skip`), not by its name:

| Commit | Contents |
|---|---|
| Library dependencies | Runtime dependencies of the published modules (`bom/pom.xml`, `aws-auth`, `avro`, …): what reaches users' classpaths |
| CLI dependencies | `cli/pom.xml`: what ships in the native binary and the image |
| Build | Maven (wrapper), Maven plug-ins, Error Prone |
| Test dependencies | `test-bom/pom.xml`, test-scoped dependencies, modules that are not deployed |

The actions PR touches `.github/workflows/`, which cannot be pushed from the container. Review its majors' release notes against the workflows (`hardwood-supply-chain-check`, area 1) and let the user merge it as is.

## 2. Issue and branch

Agree the issue with the user first (CLAUDE.md). Frame it as a routine dependency update; list the versions and, once known, what was held back and why. Work in a worktree named after the branch (`<issue>-dependency-updates`).

## 3. Apply, one group per commit

Edit the versions by hand (exact-match replacements), not by merging Dependabot branches: their grouped diffs also carry unrelated churn, such as a rewritten `mvnw.cmd`. For a Maven update, take the wrapper's SHA-256 from a distribution verified against Apache's signature and SHA-512 (`hardwood-supply-chain-check`, area 3).

Record what the published modules put on users' classpaths before the first change:

```bash
./mvnw -q -pl core,s3,aws-auth,avro,cli dependency:list -DincludeScope=runtime -DoutputFile=target/runtime-deps.txt
mkdir -p /tmp/deps-before && for f in */target/runtime-deps.txt; do cp "$f" "/tmp/deps-before/${f%%/*}.txt"; done
```

`-DoutputFile` must be relative, so each module writes its own file. After each group, rerun the first command and `diff` each module's file against the copy. A test-group change must leave them identical. A library or CLI change shows its own bump; any other new or changed artifact is a dependency Hardwood starts shipping, which the user reviews before it goes in.

Before taking an update, check what it pulls in and what pins it:

- **Majors.** Check the dependents' own POMs: a library built against the old major rules the new one out.
- **Comments in the POMs.** Versions held for a reason say so where they are declared: the SLF4J binding in `cli/pom.xml`, the Avro and Hadoop of the parquet-java reference reader and the Jackson alignment in `test-bom/pom.xml`. Read them before changing one of these versions, and keep each comment true.
- **Native code.** A dependency that adds native code, reflection or an FFM path can break the native CLI while the JVM tests pass.
- **Surefire and Failsafe.** After updating either, check that `-Dquick` and `-DskipTests` still skip the integration tests and `-DskipITs` still runs the unit tests.

After each bump, `git grep -nF '<old-version>' -- ':!*pom.xml'` finds prose and comments that name the old version; update the ones that describe the current state.

## 4. Verify

Locally, after each group and once on the tip:

- `./mvnw verify`, the full build with integration tests (180 s timeout).
- `tools/predicate-audit/run.sh`: a DuckDB or parquet-java update can change the oracles' answers. When the diff is an oracle answering differently, update `tools/predicate-audit/baseline.tsv` from the run's output, never by retyping rows, following `tools/predicate-audit/README.md`: each entry keeps a `#` comment naming its reason, and a changed `engine` row changes the differences table in `_designs/PREDICATE_MODEL.md` in the same commit. Re-run until it passes.

In CI, on the pushed PR, wait for every job before calling it done. Only CI shows:

- **The native CLI build check**, for the native-code changes above.
- **The Testcontainers path of the S3 integration tests.** Locally the dev container's `s3proxy` serves them, so a classpath conflict on the Testcontainers side surfaces only in CI.

A CI run whose jobs stay "in progress" long after they should have ended may be a GitHub incident (githubstatus.com); a run GitHub refuses to retry needs a new push.

## 5. Hold back, record, close

An update that breaks something and has no small fix stays out, with the reason in three places: the commit message of its group, the PR description, and a comment on the Dependabot PR ("Not taken: …"). Closing a single-update Dependabot PR makes Dependabot skip that version, so the next one comes back as a new PR; for an update that needs work, file an issue so that PR has context.

Fold fixes found later into the commit of their group and force-push with lease:

```bash
git commit --fixup=<commit>
GIT_SEQUENCE_EDITOR=true git -c core.commentChar=';' rebase -i --autosquash origin/main
git push --force-with-lease
```

See `hardwood-pr-git` for why the comment character matters. After the merge, close each Dependabot PR the merged PR covers with "Taken over by #<PR>", and update the issue to the actual scope.
