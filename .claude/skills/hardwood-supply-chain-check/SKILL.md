---
name: hardwood-supply-chain-check
description: Periodic supply-chain and security check of the Hardwood build and release pipeline — action pins, advisories and new majors, workflow injection and cache risks, permissions and runners, the Maven distribution, the GraalVM/JDK line and native libraries behind the CLI, known-vulnerable dependencies, release checksums and attestations, repository and organisation settings, the dev container, and the open security follow-ups. Use before each release, after each quarterly JDK security update (third Tuesday of January, April, July, October), or whenever the user asks for a "supply-chain check", "security check", "audit the build", "is the toolchain current", or "check the release pipeline".
---

# Hardwood supply-chain check

Reports the state of the build and release pipeline against the pins and rules in the **Supply chain** section of `_designs/BUILD_INFRASTRUCTURE.md`, which is the source of truth for what is pinned, how, and by whom it is updated. This skill holds the procedure and the traps; it does not repeat the current versions. Dependency *updates* are the `hardwood-dependency-updates` skill's job; this check only says which are due.

Read that design section first. Then work through the areas below and report one table per area: **item | state | assessment**, with `ok`, `due` (an update or a decision is needed) or `gap` (something the design or the guidance below asks for does not hold). End with the proposed actions, ordered by value per effort, and wait for the user's go before changing anything. Report divergences listed under [Known divergences](#known-divergences) only if their reason no longer holds.

The checklist follows the OpenSSF Scorecard checks, GitHub's secure-use reference for Actions, SLSA's build track and the OSPS Baseline; cite the check by name when reporting a gap.

## Access from the container

| Readable with the container's token | Not readable (HTTP 403): the user checks in the UI |
|---|---|
| Private vulnerability reporting (`repos/{repo}/private-vulnerability-reporting`), rulesets (`repos/{repo}/rules/branches/main`, `repos/{repo}/rulesets`), GitHub advisories (`/advisories`), workflow runs and logs, releases | Dependabot alerts, code scanning, branch protection, Actions settings, immutable releases, organisation settings (2FA, secret scanning), repository secrets |

The token also cannot push changes under `.github/workflows/` (no `workflow` scope): prepare the commit and give the user the push command.

## Areas

### 1. GitHub Actions

- Every `uses:` is pinned by a full SHA with the release tag in a trailing comment, and the SHA is the tag's in the action's own repository: `gh api repos/<owner>/<action>/git/ref/tags/<tag>` (dereference an annotated tag through `git/tags/<sha>`).
- Advisories: Dependabot alerts do not cover SHA-pinned actions. For each action, `gh api '/advisories?ecosystem=actions&affects=<owner>/<repo>' --jq '.[] | [.ghsa_id, .summary, (.vulnerabilities[].vulnerable_version_range)]'` and compare the ranges with the pinned tag.
- New **major** versions, which Dependabot proposes in its grouped actions PR: read each new major's release notes against the workflows (inputs we use, triggers such as `pull_request_target`/`workflow_run`, runtime). A pin to the newest release of an old major is not current; say so when a newer major exists.
- Deprecation warnings: for each workflow, `gh run list -w <file> -L 1 --json databaseId,createdAt`, then the run's annotations (`gh api repos/hardwood-hq/hardwood/check-runs/<job-id>/annotations`). Dispatch-only workflows may have no recent run; report the run's age.
- `actionlint .github/workflows/*.yml` (installed by the `Dockerfile`; if missing, the container predates it: ask the user to rebuild it, don't skip the check).
- `zizmor .github/workflows` covers what actionlint does not: template injection, cache poisoning, credential persistence (`artipacked`), impostor commits, known-vulnerable actions, dangerous triggers, excessive permissions. If it is not installed, propose adding it to the `Dockerfile` (CLAUDE.md).

### 2. Workflow permissions, inputs, caches and runners

- Every workflow has a top-level `permissions: contents: read`; each job that writes declares only what it uses (see "Token permissions" in the design). A reusable workflow cannot exceed its caller's grant.
- Dispatch inputs and other `${{ … }}` expressions inside `run:` scripts: they belong in `env:` and are referenced as shell variables.
- Caches in the release path: list the steps that restore a cache (`setup-java` `cache:`, `actions/cache`) in `release.yml`, `release-cli.yml` and `cli-early-access.yml`. `--strict-checksums` checks downloads, not what a cache restores, so a cache entry written by another workflow on `main` reaches the release build unchecked. Report them; dropping the cache there or recording why its writers are trusted is the maintainer's call.
- Runner labels are versioned; no `-latest`. Check GitHub's runner-images announcements for a label's deprecation date.

### 3. Maven distribution

- `--strict-checksums` is in `.mvn/maven.config`; `--lax-checksums` on the command line does not override it.
- Wrapper: `distributionUrl` and `distributionSha256Sum` in `.mvn/wrapper/maven-wrapper.properties`. To check or update them, download the distribution and verify it against Apache's signature and SHA-512, taken from `downloads.apache.org` (or `archive.apache.org` for older releases), not from the mirror the zip came from:

  ```bash
  curl -fsSLO https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/<v>/apache-maven-<v>-bin.zip
  curl -fsSLO https://downloads.apache.org/maven/maven-3/<v>/binaries/apache-maven-<v>-bin.zip.asc
  curl -fsSL https://downloads.apache.org/maven/maven-3/<v>/binaries/apache-maven-<v>-bin.zip.sha512 | awk '{print $1"  apache-maven-<v>-bin.zip"}' | sha512sum -c
  curl -fsSL https://downloads.apache.org/maven/KEYS | gpg --import
  gpg --verify apache-maven-<v>-bin.zip.asc apache-maven-<v>-bin.zip
  sha256sum apache-maven-<v>-bin.zip
  ```

### 4. Native toolchain, native libraries and the CLI image

- GraalVM CE Innovation (`version: '25i-latest'` in setup-graalvm): compare the JDK version of the newest GraalVM CE release with the latest OpenJDK security update. A lag of days after a quarterly update is expected; flag it once a newer GraalVM release exists that CI is not picking up, or when the lag passes a few weeks. `README.md` and `NATIVE_BUILD.md` name the GraalVM to install with SDKMAN; keep them on the version CI uses.
- Blocked certificates: `cli/src/main/trust/blocked.certs` against the current distrust lists. A new GraalVM install needs `tools/block-certificates.sh <java-home>` once; the native build fails until then. Never name a distrusted CA in public text beyond what is already public.
- glibc floor of the Linux binary: the highest `GLIBC_` version in `objdump -T` of the binary from the last early-access build. It comes from GraalVM, not the runner; report a change.
- Native libraries: the codec jars bundle C libraries (zstd in zstd-jni, whose version encodes it; snappy in snappy-java; lz4 in lz4-java; brotli in brotli4j). OSV by Maven coordinate reports the Java artifact's advisories, not CVEs filed against the C library: look up each bundled library version in OSV or its upstream security advisories.
- CLI image: `cli/docker/Dockerfile` is on a current Fedora release and runs as a non-root user (`USER 1000`). Report the age of the image tags users pull (`latest`, `1.0-early-access`): its OS packages are frozen at that build.

### 5. Known-vulnerable dependencies

Dependabot alerts are on; the user reads them. Cross-check independently:

```bash
./mvnw -q -pl core,s3,aws-auth,avro,cli dependency:list -DincludeScope=runtime -DoutputFile=target/runtime-deps.txt
python3 -I .claude/skills/hardwood-supply-chain-check/osv-check.py */target/runtime-deps.txt
```

`-DoutputFile` must be relative, so each module writes its own file. Separate what Hardwood ships (a library's runtime classpath, the CLI binary and image) from what reaches users only through a dependency they choose (e.g. Jackson via Avro); report reachability, not counts.

### 6. Release outputs

For the latest release and the latest early-access build (`gh release download <tag>`; the early-access release and image tag are `1.0-early-access`):

- `sha256sum --ignore-missing -c checksums_sha256.txt`.
- `gh attestation verify <archive> -R hardwood-hq/hardwood --signer-workflow hardwood-hq/hardwood/.github/workflows/release-cli.yml` (early access: `cli-early-access.yml`), and the image with `oci://ghcr.io/hardwood-hq/hardwood:<tag>`. Check that the wrong `--signer-workflow` is rejected.
- `cli-early-access.yml` runs only on `workflow_dispatch`: changes to actions or the toolchain are not exercised in the native jobs until it runs. Start it (with the user's go) after such changes and before a release.
- The release's milestone was closed by JReleaser.
- Optional cross-check: run the OpenSSF Scorecard CLI against the repository. Its *Signed-Releases* check reads only release assets (`*.sigstore.json`, `*.intoto.jsonl`, …), so it scores GitHub-stored attestations as absent.

### 7. Repository and organisation settings

Read from the container:

- Private vulnerability reporting enabled; `SECURITY.md` current.
- Rulesets: `gh api repos/hardwood-hq/hardwood/rules/branches/main --jq '[.[].type]'`. Present today: `deletion`, `non_fast_forward`, `required_linear_history`, and the `v*` tag ruleset. OSPS Baseline (AC-03.01, QA-03.01) and Scorecard *Branch-Protection* also ask for a `pull_request` rule (0 approvals works for a single maintainer) and `required_status_checks` on the PR build; the release job pushes to `main` directly, so these need a bypass for it. Report their absence as a decision for the maintainer.

The user checks in the UI:

- Dependabot alerts and Dependabot security updates on; alerts triaged.
- CodeQL default setup on.
- Actions: the allowed-actions policy requires SHA pinning; default workflow permissions read-only; "Allow GitHub Actions to create and approve pull requests" off.
- Organisation: 2FA required; secret scanning with push protection on.
- Secrets: each PAT (`DOCS_DISPATCH_TOKEN`, `SKILLS_DISPATCH_TOKEN`) fine-grained, scoped to its one target repository, with an expiry date (report it); the release secrets (GPG key, Central token) are scoped as narrowly as the workflows allow (#1631).

### 8. Dev container

The dev container runs agent sessions that hold a GitHub token.

- Hand-pinned tools in the `Dockerfile` (`*_VERSION` build args) are current; Dependabot only updates its `FROM` line.
- Every download is verified: a checksum per architecture (as for actionlint) or a package manager. Flag piped installers (`curl … | bash`) and downloads without a checksum.
- It runs as root (no `USER`); the compose file grants no extra privileges, mounts no Docker socket and nothing from the host beyond the repository. Report a change to any of these.

### 9. Open security follow-ups

`gh issue list --label security --state open --json number,title,milestone`. Report which have slipped past their milestone, and label new security issues as you file them.

## Known divergences

Recorded decisions that differ from the guidance above; report one only if its reason no longer holds:

- Container base images are pinned by release tag, not digest, so each build picks up the release's package updates.
- Build JDKs and GraalVM float within a major release line.
- No SBOM is published.
- `SECURITY.md` names no response time.
- No second reviewer: a single maintainer.
- The CLI attestations reach SLSA Build L2; L3 would need the attestation in a shared reusable workflow.
- Immutable releases are off: `cli-early-access.yml` deletes and recreates the rolling `1.0-early-access` release.

## Traps

- **A green PR build is not the whole pipeline.** The release and early-access workflows use the same actions but run only on dispatch; the PR build checks the native binary on Linux only.
- **The dev container hides the Testcontainers path.** S3 integration tests use the compose `s3proxy` locally and Testcontainers in CI, so classpath conflicts there show only in CI.
- **Claims about versions are measured, not inferred.** Download and check (glibc via `objdump`, the JDK via the binary or the setup log, SHA-256 from the artifact); release notes have led to wrong conclusions before.
