# Creating a Release

A release is a tag on a commit that carries the release version. Pushing the tag builds the release, stages it on the Maven Central portal and smoke-tests it; a maintainer then publishes it in the portal and releases the CLI. [BUILD_INFRASTRUCTURE.md](_designs/BUILD_INFRASTRUCTURE.md#release-jobs) describes the jobs and what each may do.

## One-time setup

**Signing key**: Maven Central checks signatures against the public key servers.

```shell
gpg --quick-generate-key "<Name> (Hardwood release signing) <<email>>" rsa4096 sign 2y
gpg --keyserver keyserver.ubuntu.com --send-keys <FINGERPRINT>
gpg --export <FINGERPRINT> | curl -T - https://keys.openpgp.org
```

**Environment** `release` (Settings → Environments), with the deployment rule "tags matching `v*`" and these secrets:

| Secret | Value |
|--------|-------|
| `GPG_PUBLIC_KEY` | `gpg --armor --export <FINGERPRINT>` |
| `GPG_SECRET_KEY` | `gpg --armor --export-secret-keys <FINGERPRINT>` |
| `GPG_PASSPHRASE` | the key's passphrase |
| `CENTRAL_USERNAME`, `CENTRAL_TOKEN` | a Central Portal user token |

**Tag ruleset** for `v*`: restrict creations, updates and deletions, with those who may release as bypass actors. The environment hands the secrets to any run on a `v*` tag, so this ruleset is what keeps them from anyone else.

## Preparing the release notes

Land the version's section of `docs/content/release-notes.md` on the branch to release from, with the milestone link and the contributor line, which `tools/release/contributors.py <since-tag> [<until-tag>]` produces from the commit authors and `Co-authored-by` trailers (it needs an authenticated `gh`).

## Running the Release

| Step | Command |
|---|---|
| 1. Tag the release: makes the release commit on top of `origin/<BRANCH>` (default `main`), pushes its tag, which starts the `Release` workflow, and opens a pull request that brings the version into `main` (with `--next`, required after a `Final` from `main`: also the next snapshot version) | `tools/release/release.sh <VERSION> [--base <BRANCH>] [--next <NEXT>-SNAPSHOT]` |
| 2. Follow the `Release` run; once it has passed, this prints the commands for the next steps and for running the smoke test locally | `tools/release/await.sh <VERSION> --latest <true\|false>` (`release.sh` prints it) |
| 3. Publish the deployment, or drop it | https://central.sonatype.com/publishing/deployments |
| 4. Release the CLI: GitHub release with the archives and the release notes, the milestone, the image | `gh workflow run release-cli.yml --ref v<VERSION> -f latest=<true\|false>` |
| 5. Merge the pull request against `main` | |

A failed job can be re-run, except `stage` once it has uploaded: drop that deployment first, since a re-run uploads again. Before the deployment is published nothing is public; to redo a release, drop the deployment, delete the tag (`git push origin :refs/tags/v<VERSION>`), close the pull request and delete its branch `release-<VERSION>`, fix the cause and start again. Once published, the libraries are final; `release-cli.yml` can be re-run, and a GitHub release or image tag it created can be deleted by hand.

To resolve the staged deployment yourself rather than use the run's artifact, set `MAVEN_CENTRAL_BEARER_TOKEN` to `base64("<token username>:<token password>")` and run `tools/release/smoke-test/resolve.sh <VERSION> target/staged-classpath`.

## Releasing a fix from an earlier release

A fix release, such as `1.0.1.Final` after `1.0.0.Final`, ships a fix without the rest of `main`. Land the fix on `main` first, then:

1. Branch from the previous release's tag, named after the release or the line: `git switch -c 1.0.x v1.0.0.Final`. A tag runs the workflows of its own commit, so a line released before 1.1.0.CR2 first needs the release tooling: cherry-pick the commit that introduced it (`git log --format=%h -1 -- tools/release/jreleaser.yml` on `main`).
2. Cherry-pick the fix and give the release its own section in `docs/content/release-notes.md`.
3. Build and test the branch locally, including `./mvnw -Dnative verify` in `cli`: it gets no CI.
4. Push the branch and release as above with `--base <BRANCH>`. A release below the latest one is not marked latest and opens no pull request against `main`.
5. Add the release's section to `docs/content/release-notes.md` on `main`, and publish the documentation (below) with **update-latest** unchecked if the release belongs to an older line.

## Publishing the documentation

Versioned documentation is published after the release, so that the announcement link and release-notes errata can be added without moving the release tag:

1. Add the blog post link and any errata to `docs/content/release-notes.md` on `main`.
2. On top of the release tag, point the CLI links at the release and take over those release notes, then tag the commit `v<VERSION>-docs`:
   ```shell
   git fetch origin main
   git switch --detach v<VERSION>
   perl -pi -e 's|^  cli_release_tag: .*|  cli_release_tag: v<VERSION>|; s|^  cli_docker_tag: .*|  cli_docker_tag: <VERSION>|' docs/mkdocs.yml
   git checkout origin/main -- docs/content/release-notes.md
   git commit -am "[docs] <VERSION> documentation"
   git tag v<VERSION>-docs && git push origin v<VERSION>-docs
   ```
3. Run the **Publish versioned documentation** workflow with **ref** `v<VERSION>-docs`, **version** `<VERSION>`, and **update-latest** unchecked for a release of an older line.

The workflow has `hardwood-hq/hardwood-hq.github.io` build the site at that commit. The `dev` documentation is refreshed after every successful Main Build of `main`.
