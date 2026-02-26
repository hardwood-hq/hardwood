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

## Running the Release

Trigger the `Release` workflow from the GitHub Actions UI with the following inputs:

- **Release version**: The version to release (e.g., `1.0.0`)
- **Development version**: The next snapshot version (e.g., `1.1.0-SNAPSHOT`)
- **Stage**: `UPLOAD` to upload the release to the deploy to the Maven Central portal (you can examine it at https://central.sonatype.com/publishing/deployments then), or `PUBLISH` to publish directly to Maven Central

To test a staged release, run the following:

```shell
MAVEN_CENTRAL_BEARER_TOKEN=`printf "${MAVENCENTRAL_USERNAME}:${MAVENCENTRAL_TOKEN}" | base64` ./mvnw clean verify -pl :hardwood-integration-test -Pcentral.manual.testing -s release-test-settings.xml -Dhardwood.version=<VERSION>
```
