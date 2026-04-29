# Creating a Release

Releases are published to Maven Central via the `Release` GitHub Actions workflow.

## GPG Signing Key Setup

Maven Central requires artifacts to be GPG-signed. Generate a key pair (one-time setup):

```shell
# Generate a new GPG key pair (choose RSA 4096-bit)
gpg --full-generate-key

# List keys to find your key ID (shown after "sec rsa4096/")
gpg --list-secret-keys --keyid-format long

# Publish the public key to a keyserver (Maven Central validates against these)
gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID>

# Export the public key (armored)
gpg --armor --export <KEY_ID>

# Export the secret key (armored)
gpg --armor --export-secret-keys <KEY_ID>
```

## GitHub Actions Secrets

Configure the following secrets in the repository settings:

| Secret | Description |
|--------|-------------|
| `JRELEASER_GPG_PUBLIC_KEY` | Armored public key from `gpg --armor --export` |
| `JRELEASER_GPG_SECRET_KEY` | Armored secret key from `gpg --armor --export-secret-keys` |
| `JRELEASER_GPG_PASSPHRASE` | Passphrase chosen during key generation |
| `JRELEASER_MAVENCENTRAL_USERNAME` | Maven Central (Sonatype) username |
| `JRELEASER_MAVENCENTRAL_TOKEN` | Maven Central (Sonatype) token |

## Preparing the release notes

Before triggering the release, land an `[release] <VERSION> release notes` commit on `main` that updates `docs/content/release-notes.md` with the new section, milestone link, and contributor list.

`tools/contributors.py` produces the contributor line, alphabetised and with display names resolved from each GitHub profile:

```shell
# Two tags: contributors with commits in (<since>, <until>]
tools/contributors.py v1.0.0.Beta1 HEAD

# One tag: contributors up to and including <since> (initial release)
tools/contributors.py v1.0.0.Alpha1
```

It reads commit authors and `Co-authored-by` trailers, skips bots, and resolves emails via `gh api` (so authentication needs to be set up — `gh auth status`). Emails on accounts with email privacy enabled may not resolve; the script reports them on stderr.

## Running the Release

Trigger the `Release` workflow from the GitHub Actions UI with the following inputs:

- **Release version**: The version to release (e.g., `1.0.0`)
- **Development version**: The next snapshot version (e.g., `1.1.0-SNAPSHOT`)
- **Stage**: `UPLOAD` to upload the release to the deploy to the Maven Central portal (you can examine it at https://central.sonatype.com/publishing/deployments then), or `FULL` to publish directly to Maven Central

To test a staged release, check out the release tag and run:

```shell
git checkout v<VERSION>
MAVEN_CENTRAL_BEARER_TOKEN=`printf "${MAVENCENTRAL_USERNAME}:${MAVENCENTRAL_TOKEN}" | base64` ./mvnw clean verify -pl :hardwood-integration-test -Pcentral.manual.testing -s release-test-settings.xml
```

## Publishing the documentation

Versioned documentation is published as a manual follow-up to the release, so post-release touch-ups (announcement blog post link, release-notes errata) can be folded in without mutating the release tag itself. The convention is:

1. After the release, write the announcement and add the blog post link plus any errata to `docs/content/release-notes.md` on `main`.
2. Tag that commit `v<VERSION>-docs` and push the tag:
   ```shell
   git tag v<VERSION>-docs
   git push origin v<VERSION>-docs
   ```
3. Run the **Publish versioned documentation** workflow from the GitHub Actions UI with:
   - **ref**: `v<VERSION>-docs`
   - **version**: `<VERSION>` (e.g. `1.0.0.Beta2`)
   - **update-latest**: leave checked for forward releases; uncheck for back-ports of an earlier line that should not displace the newer one.

The workflow fires a `repository_dispatch` at `hardwood-hq/hardwood-hq.github.io`, which builds the site at the resolved commit. The `dev` label is refreshed automatically on every successful Main Build of `main` and never claims `latest`.
