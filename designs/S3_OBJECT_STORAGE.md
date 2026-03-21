# Plan: Support Reading Files from S3 Object Storage (#31)

## Context

It's a common requirement to read Parquet files stored on object storage (S3). The `InputFile` abstraction (PR #99) decouples the read pipeline from memory-mapped local files, enabling custom backends. This plan adds an `S3InputFile` implementation backed by the AWS SDK for Java v2, along with Testcontainers-based integration tests using LocalStack.

### Design decisions

1. **Separate module** (`hardwood-s3`): The AWS SDK v2 has a large dependency tree (multiple JAR files, Netty-based HTTP client, etc.). Putting it in core would bloat the minimal-dependency promise. A separate module keeps core clean and follows the pattern used by other projects (e.g., parquet-hadoop separate from parquet-column).

2. **Range requests, not full download**: The `InputFile.readRange()` contract maps directly to S3 `GetObject` with a byte range. This lets the reader fetch only the footer, specific column chunks, and specific pages — avoiding downloading entire files.

3. **Suffix-range open**: `open()` uses a suffix-range GET (`bytes=-64KB`) instead of a HEAD request. This discovers the file length from the `Content-Range` response header and pre-fetches the Parquet footer (which sits at the end of the file) in the same round-trip. Subsequent `readRange()` calls for the footer are served from this tail cache. For multi-file workloads this halves the per-file metadata overhead (1 request instead of 2).

4. **AWS credential chain**: The standard `DefaultCredentialsProvider` handles EC2 instance roles, environment variables, ~/.aws/credentials, etc. No custom auth plumbing needed.

5. **Exception translation**: All AWS SDK exceptions (`S3Exception`, `NoSuchKeyException`, etc.) are caught and wrapped in `IOException` so that callers see a consistent error model regardless of the `InputFile` backend.

---

## Step 1: New Module `hardwood-s3`

### New file: `s3/pom.xml`

```xml
<artifactId>hardwood-s3</artifactId>
<name>Hardwood S3</name>
<description>S3 object storage support for Hardwood</description>

<dependencies>
    <dependency>
        <groupId>dev.hardwood</groupId>
        <artifactId>hardwood-core</artifactId>
    </dependency>
    <dependency>
        <groupId>software.amazon.awssdk</groupId>
        <artifactId>s3</artifactId>
    </dependency>
</dependencies>
```

Add the module to the parent POM `<modules>` and add the AWS SDK BOM to `<dependencyManagement>`.
Also add `hardwood-s3` to the BOM (`bom/pom.xml`) so consumers can import it.
Add Testcontainers BOM and `log4j-slf4j2-impl` to the test BOM.

**Files:**
- `s3/pom.xml` (new)
- `pom.xml` (modify — add module, `aws-sdk.version` property, AWS SDK BOM)
- `bom/pom.xml` (modify — add hardwood-s3)
- `test-bom/pom.xml` (modify — add Testcontainers BOM, log4j-slf4j2-impl)

---

## Step 2: `S3InputFile` Implementation

### New file: `s3/src/main/java/dev/hardwood/s3/S3InputFile.java`

Package: `dev.hardwood.s3` — public API for S3 support.

Key properties:
- `open()` uses a suffix-range GET to discover file length and pre-fetch the
  footer in a single round-trip. The tail cache (64 KB) serves subsequent
  `readRange()` calls that fall within it.
- `readRange()` returns a heap-allocated `ByteBuffer` (unavoidable for network
  I/O), or a slice from the tail cache if the range overlaps.
- All SDK exceptions are caught and wrapped in `IOException` with context (file
  name, offset/range) for debuggability.
- Caller can share a single `S3Client` across multiple files (connection pooling,
  credential reuse) or let the factory create one per file.
- Thread-safe once opened: `S3Client` is thread-safe, `fileLength` and `tailCache`
  are set once in `open()`.

**Files:**
- `s3/src/main/java/dev/hardwood/s3/S3InputFile.java` (new)

---

## Step 3: Integration Tests with Testcontainers + LocalStack

`S3InputFileTest` — basic read operations: metadata, rows, row values, nulls,
file-not-found error, name formatting.

`S3MultiFileTest` — multi-file reading over S3 via `Hardwood.openAll()`.

`S3SelectiveReadJfrTest` — verifies that column projection and row group filtering
reduce S3 I/O using JFR events:
- `dev.hardwood.RowGroupScanned` — only projected columns are scanned
- `dev.hardwood.RowGroupFilter` — row groups are skipped by predicate push-down
- `jdk.SocketRead` — fewer bytes transferred with a projection

**Files:**
- `s3/src/test/java/dev/hardwood/s3/S3InputFileTest.java` (new)
- `s3/src/test/java/dev/hardwood/s3/S3MultiFileTest.java` (new)
- `s3/src/test/java/dev/hardwood/s3/S3SelectiveReadJfrTest.java` (new)
- `s3/src/test/resources/log4j2.xml` (new)

---

## Step 4: Docker Compose

Mount the Docker socket and set Testcontainers environment variables for
Docker-in-Docker networking:

```yaml
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
environment:
  - TESTCONTAINERS_RYUK_DISABLED=true
  - TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal
```

**Files:**
- `docker-compose.yaml` (modify)

---

## Module Layout Summary

```
hardwood/
├── core/                       (existing, no changes)
├── s3/                         (new module)
│   ├── pom.xml
│   └── src/
│       ├── main/java/dev/hardwood/s3/
│       │   └── S3InputFile.java
│       └── test/java/dev/hardwood/s3/
│           ├── S3InputFileTest.java
│           ├── S3MultiFileTest.java
│           └── S3SelectiveReadJfrTest.java
├── pom.xml                     (add s3 module + AWS SDK BOM)
├── bom/pom.xml                 (add hardwood-s3)
├── test-bom/pom.xml            (add Testcontainers BOM, log4j-slf4j2-impl)
└── docker-compose.yaml         (Docker socket + Testcontainers env vars)
```

---

## Implementation Notes

- All new source files must include the Apache-2.0 license header (matching the pattern in existing source files).
- `S3InputFile` wraps all `S3Exception` subtypes (including `NoSuchKeyException`) in `IOException` with contextual messages.

## Open Questions / Future Work

1. **Async S3 client**: The AWS SDK v2 also offers `S3AsyncClient` for non-blocking I/O. The current `InputFile.readRange()` is synchronous, so we use the sync client. If async becomes important, a future `InputFile` evolution could add async methods.

2. **Large files (>2 GB)**: `readRange()` takes `int length`, limiting individual reads to 2 GB. This matches the existing constraint from `MappedInputFile`. S3 files can be larger, but individual column chunks should be well under this limit. The `long offset` parameter already supports files of any size.

3. **Retry and resilience**: The AWS SDK v2 has built-in retry logic for transient S3 errors. No custom retry needed in `S3InputFile`.

4. **Connection/request timeouts**: The AWS SDK v2 default timeouts may be too generous for interactive use. Consider exposing `S3Client` builder configuration or documenting recommended timeout settings for latency-sensitive workloads.

5. **Multi-file convenience factory**: A future addition could provide `S3InputFile.ofAll(S3Client, String bucket, List<String> keys)` to match the `InputFile.ofPaths()` pattern.

---

## Verification

1. `./mvnw verify -pl core` — existing tests still pass
2. `./mvnw verify -pl s3` — S3 integration tests pass against LocalStack
3. `./mvnw verify` — full build succeeds
4. All with 180s timeout to detect deadlocks
