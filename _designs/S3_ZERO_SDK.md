# Design: Zero-SDK S3 Source with Credentials Delegation (#144)

**Status: Implemented**

## Context

`hardwood-s3` needs two HTTP operations against S3: a suffix-range GET (for `open()`) and byte-range GETs (for `readRange()`). This does not warrant an 8 MB / 31-JAR dependency on the AWS SDK S3 service client.

S3 support is built on three layers:

1. **Credentials delegation** — `S3Credentials` / `S3CredentialsProvider` types in `hardwood-s3`. Callers supply credentials; Hardwood never resolves them.
2. **SigV4 signing** — ~200 lines, JDK crypto only (`MessageDigest`, `Mac`).
3. **HTTP transport** — `S3Api` issues signed requests via JDK `java.net.http.HttpClient`.

`hardwood-s3` has zero external dependencies beyond `hardwood-core`. An optional `hardwood-aws-auth` module bridges `software.amazon.awssdk:auth` for users who want the full AWS credential chain.

| Module | Compile JARs | Size | AWS SDK? |
|---|---|---|---|
| `hardwood-s3` | 0 | 0 | No |
| `hardwood-aws-auth` (optional) | 11 | ~3.1 MB | Yes (auth only) |

---

## Module structure

```
hardwood-core          # S3-agnostic. InputFile abstraction, Parquet reading.
hardwood-s3            # S3InputFile, S3Source, S3Credentials/Provider, Aws4Signer, S3Api (internal).
hardwood-aws-auth      # Optional bridge: software.amazon.awssdk:auth → S3CredentialsProvider.
hardwood-cli           # Depends on hardwood-s3 + hardwood-aws-auth.
```

---

## 1. Credentials delegation

### Credential types (in `hardwood-s3`)

```java
package dev.hardwood.s3;

public record S3Credentials(
    String accessKeyId,
    String secretAccessKey,
    String sessionToken       // null for long-term/static credentials
) {
    public static S3Credentials of(String accessKeyId, String secretAccessKey) {
        return new S3Credentials(accessKeyId, secretAccessKey, null);
    }
}

@FunctionalInterface
public interface S3CredentialsProvider {
    S3Credentials credentials();
}
```

Plain types, no SDK dependency. Hardwood calls `provider.credentials()` per signing operation and does not cache — the provider handles caching and refresh.

### `hardwood-aws-auth`

Bridges `software.amazon.awssdk:auth` → `S3CredentialsProvider`:

```java
package dev.hardwood.aws.auth;

public final class SdkCredentialsProviders {
    /// Full AWS credential chain (env vars, ~/.aws/credentials, IMDS, SSO, web identity, etc.).
    public static S3CredentialsProvider defaultChain() { ... }
    /// Named profile from ~/.aws/credentials.
    public static S3CredentialsProvider fromProfile(String profileName) { ... }
}
```

Implementation bridges SDK types to Hardwood types, handling `AwsSessionCredentials` for STS/IRSA.

#### Dependencies

`hardwood-aws-auth` depends on `hardwood-s3` (for credential types) and `software.amazon.awssdk:auth` with aggressive exclusions (signing, checksums, eventstream modules excluded; `http-client-spi` retained — required by `ContainerCredentialsRetryPolicy` at GraalVM build time). Resulting tree: ~3.1 MB.

### CLI region resolution

The CLI resolves region without the SDK's region chain (avoiding slow IMDS timeouts on local dev machines):

1. `aws.region` system property
2. `AWS_REGION` env var
3. `AWS_DEFAULT_REGION` env var
4. `~/.aws/config` profile (respects `AWS_PROFILE`)

If none found and a custom endpoint is set, `S3Source` defaults to `"auto"` (S3-compatible services don't use the region for routing). If none found and no custom endpoint, an error is raised.

### Manual bridge (without `hardwood-aws-auth`)

Users with the AWS SDK on their classpath can write the bridge themselves:

```java
S3CredentialsProvider provider = () -> {
    AwsCredentials c = sdkProvider.resolveCredentials();
    return c instanceof AwsSessionCredentials s
        ? new S3Credentials(s.accessKeyId(), s.secretAccessKey(), s.sessionToken())
        : S3Credentials.of(c.accessKeyId(), c.secretAccessKey());
};
```

---

## 2. SigV4 signing

Package-private `dev.hardwood.s3.internal.Aws4Signer`. JDK crypto only, no AWS SDK types.

**Input:** method, URI, headers, payload hash, credentials (accessKeyId, secretAccessKey, sessionToken), region, service, timestamp.
**Output:** `SignResult` containing the `Authorization` header and all signed headers.

### Algorithm

**Step 1 — Canonical request:** `Method\nURI\nQueryString\nHeaders\nSignedHeaders\nPayloadHash`. S3-specific: single URL encoding (not double), no path normalization.

**Step 2 — String to sign:** `AWS4-HMAC-SHA256\n<timestamp>\n<date>/<region>/s3/aws4_request\nSHA256(canonical request)`.

**Step 3 — Signing key** (HMAC-SHA256 chain):

```java
dateKey    = hmacSha256("AWS4" + secretKey, date)
regionKey  = hmacSha256(dateKey,            region)
serviceKey = hmacSha256(regionKey,          service)
signingKey = hmacSha256(serviceKey,         "aws4_request")
```

**Step 4 — Signature:** `hexEncode(hmacSha256(signingKey, stringToSign))`, placed in the `Authorization` header as `AWS4-HMAC-SHA256 Credential=<accessKey>/<scope>, SignedHeaders=<signedHeaders>, Signature=<sig>`.

### Key details

- **Payload hash**: `SHA-256("")` for GET (no body); actual `SHA-256(body)` for PUT. Set as the `x-amz-content-sha256` header and the canonical request's `HashedPayload`.
- **Session tokens**: when `sessionToken` is non-null, `x-amz-security-token` is added to headers **before** signing. Omitted entirely when null.
- **URI encoding**: encode all bytes except `A-Za-z0-9-._~`, space → `%20`, preserve `/` in paths, uppercase hex (`%2F`). Key path segments are URI-encoded when constructing request URIs.
- **Path normalization**: controlled by a `normalize` parameter. S3 uses `false`; the test suite validates both modes.

---

## 3. HTTP transport

### `S3Api`

`dev.hardwood.s3.internal.S3Api` encapsulates all REST interactions: credential resolution, SigV4 signing, URI construction (including key encoding), and HTTP dispatch. Used by `S3InputFile` for reads and directly by test code for uploads.

Key methods:
- `getBytes(bucket, key, rangeHeader)` → `HttpResponse<byte[]>`
- `getStream(bucket, key, rangeHeader)` → `HttpResponse<InputStream>`
- `putObject(bucket, key, body)` — for test infrastructure
- `createBucket(bucket)` — for test infrastructure
- `objectUri(bucket, key)` — builds virtual-hosted or path-style URI with encoded key

### `S3Source`

Configured connection to an S3-compatible service. Owns the `HttpClient` and `S3Api`, creates `S3InputFile` instances.

```java
public final class S3Source implements Closeable {

    public S3InputFile inputFile(String bucket, String key) { ... }
    public S3InputFile inputFile(String uri) { ... }              // s3://bucket/key

    public List<InputFile> inputFilesInBucket(String bucket, String... keys) { ... }
    public List<InputFile> inputFiles(String... uris) { ... }     // s3:// URIs, may span buckets

    public static Builder builder() { ... }

    public static final class Builder {
        public Builder region(String region) { ... }
        public Builder endpoint(String endpoint) { ... }
        public Builder pathStyle(boolean pathStyle) { ... }
        public Builder credentials(S3CredentialsProvider provider) { ... }
        public Builder credentials(S3Credentials credentials) { ... }
        public S3Source build() { ... }
    }
}
```

`S3Source.api()` is package-private — accessible to `S3InputFile` (same package) but not part of the public API.

**URI parsing**: `s3://<bucket>/<key>`.

**URL style rules**:
- **Virtual-hosted** (default): `https://{bucket}.s3.{region}.amazonaws.com/{key}`
- **Path-style** (opt-in via `pathStyle(true)`): `{endpoint}/{bucket}/{key}`, or `https://s3.{region}.amazonaws.com/{bucket}/{key}` when no custom endpoint is set

Setting an endpoint does not imply path-style — callers must opt in explicitly.

**Region defaulting**: region is required when no custom endpoint is set. When a custom endpoint is set and no region is specified, defaults to `"auto"`.

### `S3InputFile`

Implements `InputFile`. Created via `S3Source.inputFile()` / `inputFilesInBucket()` / `inputFiles()`. Delegates HTTP operations to `S3Api`.

- `open()`: suffix-range GET (`bytes=-65536`), extracts file length from `Content-Range`, caches tail bytes
- `readRange()`: serves from tail cache if in range, otherwise byte-range GET into `ByteBuffer`
- `close()`: no-op — `S3Source` owns the `HttpClient`
- Errors wrapped in `IOException` with status code and response body

---

## API usage examples

```java
// Static credentials
S3Source source = S3Source.builder()
        .region("us-east-1")
        .credentials(S3Credentials.of("AKIA...", "secret"))
        .build();

// AWS credential chain (via hardwood-aws-auth)
S3Source source = S3Source.builder()
        .region("us-east-1")
        .credentials(SdkCredentialsProviders.defaultChain())
        .build();

// Single file
ParquetFileReader.open(source.inputFile("s3://my-bucket/events.parquet"));

// Multiple files (same bucket)
hardwood.openAll(source.inputFilesInBucket("my-bucket",
        "events-2024-01.parquet", "events-2024-02.parquet", "events-2024-03.parquet"));

// Multiple files (across buckets)
hardwood.openAll(source.inputFiles(
        "s3://bucket-a/events.parquet", "s3://bucket-b/events.parquet"));

// Cloudflare R2 (no region needed)
S3Source.builder()
        .endpoint("https://<account-id>.r2.cloudflarestorage.com")
        .credentials(S3Credentials.of(accessKeyId, secretKey))
        .build();

// GCP Cloud Storage (HMAC keys, no region needed)
S3Source.builder()
        .endpoint("https://storage.googleapis.com")
        .credentials(S3Credentials.of(hmacAccessId, hmacSecret))
        .build();
```

| Provider | Endpoint | Region | Auth |
|---|---|---|---|
| AWS S3 | (default) | Required | SigV4 + credential chain or static keys |
| Cloudflare R2 | `<account>.r2.cloudflarestorage.com` | Optional | SigV4 + static API token |
| GCP Cloud Storage | `storage.googleapis.com` | Optional | SigV4 + HMAC keys |
| MinIO | custom | Optional | SigV4 + static keys |

---

## `HadoopInputFile`

`parquet-java-compat`'s `HadoopInputFile` uses reflection to construct `S3Source` and `S3Credentials` from Hadoop configuration properties (`fs.s3a.*`). Reflects only on Hardwood types, no AWS SDK types.

Since `HadoopInputFile` creates a one-off `S3Source` per file, the resulting `S3InputFile` is wrapped in an `OwnedSourceInputFile` that closes the `S3Source` (and its `HttpClient`) when the file is closed. This prevents resource leaks without affecting the shared-source pattern used by `S3Source.inputFile()`.

---

## Testing

### SigV4 test suite

38 test vectors from [awslabs/aws-c-auth](https://github.com/awslabs/aws-c-auth) (`tests/aws-signing-test-suite/v4/`), cloned at test time via JGit (shallow clone into `target/aws-c-auth`, reused across runs). Each case provides `context.json` (credentials, region, service, timestamp, flags), `request.txt`, and expected outputs for canonical request, string-to-sign, and signature.

Key `context.json` flags:
- **`normalize`**: path normalization on/off. S3 requires `false` (keys can contain `//`, `..`). 7 cases test each mode.
- **`sign_body`**: payload hashing on/off. 3 cases include session tokens.

Parameterized `Aws4SignerTest` runs all 38 cases, asserting each intermediate result. `Aws4Signer` exposes canonical request and string-to-sign via package-private methods for this purpose.

### s3proxy integration tests

`S3InputFileTest`, `S3MultiFileTest`, `S3SelectiveReadJfrTest` run against s3proxy via Testcontainers. Tests construct `S3Api` directly for upload setup and use `S3Source` for reading.

### `hardwood-aws-auth` tests

`SdkCredentialsProvidersTest` validates the SDK → Hardwood credential bridging via system properties (`aws.accessKeyId`, `aws.secretAccessKey`, `aws.sessionToken`). Verifies both long-term and session credentials.

---

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Signing bug | 38 AWS test vectors + s3proxy integration tests |
| Future SigV4 changes | Stable since 2012; SigV4a only needed for multi-region access points |
| SDK exclusions break credential resolution | Excluded modules are signing infrastructure, not credential providers; `http-client-spi` retained for GraalVM compatibility |
| Session token handling | Explicit signer check + test vectors with session tokens |

---

## Out of scope

- **SigV4a** (multi-region signing), **presigned URLs**, **streaming signatures** — not needed for read-only byte-range GETs.
- **Azure Blob Storage** — different signing scheme.
- **GCP ADC / OAuth2** — different auth; GCP users should use the S3-compatible XML API with HMAC keys.
- **CLI credential flags** (`--access-key-id`, `--profile`) — the CLI relies on `hardwood-aws-auth`. Can be added later.
- **Pure JDK credential resolution** — feasible but significantly more work (IMDS, ECS, SSO, web identity). Can be revisited.
