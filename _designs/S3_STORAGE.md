# S3 Storage

Covers the `hardwood-s3` module: the HTTP client it builds on the JDK alone, SigV4 request signing, credential delegation and anonymous access, the optional `hardwood-aws-auth` bridge and the CLI's credential fallback, `S3Source` and its configuration, `S3InputFile` with its suffix-range open and its `ETag` identity, `S3OutputFile` and its multipart upload, retries and error mapping, and the optional range cache (`RangeBacking`, `RangeBackedInputFile`) that remote reads can sit behind.

Related documents:

- [INPUT_FILES.md](INPUT_FILES.md): the `InputFile` contract every backend implements, including ownership and the local mapped and in-memory backends
- [FETCH_PLANNING.md](FETCH_PLANNING.md): which byte ranges a read asks for, how they are coalesced and prefetched
- [FILE_METADATA.md](FILE_METADATA.md): how the footer bytes are parsed and cached
- [WRITER.md](WRITER.md#outputfile): the `OutputFile` contract `S3OutputFile` implements
- [docs/content/how-to/s3.md](../docs/content/how-to/s3.md), [docs/content/how-to/write-to-s3.md](../docs/content/how-to/write-to-s3.md) and [docs/content/reference/s3.md](../docs/content/reference/s3.md): user-facing configuration

## Module boundary

`hardwood-s3` depends on `hardwood-core` and nothing else at compile time. Reading needs two operations against an object store, a suffix-range `GET` and a byte-range `GET`; writing needs a `PUT` and the four multipart-upload requests. It implements them on `java.net.http.HttpClient` with JDK crypto for signing. The AWS SDK is not a dependency of the module; a design change that adds one to `hardwood-s3` gives up the dependency-free footprint the module exists to keep.

| Module | Depends on | Role |
|---|---|---|
| `hardwood-core` | nothing S3-specific | `InputFile`, the read pipeline, `RangeBackedInputFile` and `RangeSet` (internal) |
| `hardwood-s3` | `hardwood-core` | `S3Source`, `S3InputFile`, `S3OutputFile`, `S3Credentials`, `S3CredentialsProvider`, `RangeBacking`; internal `S3Api`, `Aws4Signer`, `S3Xml`; package-private `S3Fetcher` |
| `hardwood-aws-auth` (optional) | `hardwood-s3`, `software.amazon.awssdk:auth` | `SdkCredentialsProviders`: the AWS credential chain as an `S3CredentialsProvider` |
| `hardwood-cli` | `hardwood-s3`, `hardwood-aws-auth` | `s3://` URIs on every file-taking command |

`hardwood-aws-auth` pulls in `software.amazon.awssdk:auth` with the signing, checksum, eventstream, endpoint, metrics and retry modules excluded. `http-client-spi` stays on the classpath because the container-credentials provider references it during GraalVM native-image analysis. The exclusions are signing and transport infrastructure, not credential sources, so the chain resolves the same sources as an unexcluded `auth` module.

Every protocol request goes through `S3Api` (`dev.hardwood.s3.internal`): credential lookup, signing, URI construction and dispatch. `S3Api` also exposes `createBucket` and a whole-array `putObject`, which exist for test fixtures; production writes go through `S3OutputFile` only.

## S3Source and configuration

`S3Source` is a configured connection to one S3-compatible service. It owns the `HttpClient` and the `S3Api` built on it, and creates `S3InputFile` instances for `(bucket, key)` pairs or `s3://bucket/key` URIs, singly or in lists (`inputFilesInBucket`, `inputFiles`; the latter may span buckets). One source is meant to serve many files, which share its connection pool and credential provider.

| Builder option | Default | Contract |
|---|---|---|
| `region` | none | Required when no endpoint is set; `build()` throws otherwise. With a custom endpoint and no region, the source signs with `"auto"`. |
| `endpoint` | AWS | Base URI of an S3-compatible service. Setting it does not imply path-style. |
| `pathStyle` | `false` | Selects path-style addressing (below). |
| `credentials` | none | An `S3CredentialsProvider`, or static `S3Credentials` wrapped as one. Required unless `anonymous` is set; `build()` throws otherwise. |
| `anonymous` | off | Sends every request unsigned. Mutually exclusive with `credentials`; `build()` throws when both are set. |
| `connectTimeout` | 10 s | Applied to the `HttpClient` the source builds; ignored with a caller-supplied client. |
| `requestTimeout` | 30 s | Per HTTP request, applied by `S3Api` to every request. |
| `maxRetries` | 3 | Retries per `GET` (see [Errors and retries](#errors-and-retries)); negative values are rejected. |
| `httpClient` | built by the source | A caller-supplied client is never closed by the source. |
| `rangeBacking` | `NONE` | Range cache mode for every file from this source (see [Range backing](#range-backing)). |
| `tempDir` | `java.io.tmpdir` | Backing-file directory under `SPARSE_TEMPFILE`; ignored under `NONE`. |

`S3Source.close()` closes the `HttpClient` only when the source built it. `S3InputFile.close()` never touches the client, so closing a file leaves the source usable for the next one; closing the source ends every file created from it.

URI construction lives in `S3Api.objectUri`:

| Addressing | No endpoint | Custom endpoint |
|---|---|---|
| Virtual-hosted (default) | `https://{bucket}.s3.{region}.amazonaws.com/{key}` | `{scheme}://{bucket}.{host}[:{port}]/{key}` |
| Path-style | `https://s3.{region}.amazonaws.com/{bucket}/{key}` | `{endpoint}/{bucket}/{key}` |

The key is URI-encoded per path segment with the SigV4 rules, keeping `/` as the separator. `s3://` URIs are split at the first `/` after the bucket; a URI without a key is rejected.

Tests: `S3SourceNullValidationTest` (s3), `S3SourceTempDirValidationTest` (s3), `S3SourceCredentialsValidationTest` (s3). Region defaulting and virtual-hosted URI construction are untested; the ITs run path-style against s3proxy ([TESTING.md](../TESTING.md#s3-tests)).

## Credentials

Hardwood never resolves credentials itself. The caller supplies an `S3CredentialsProvider`, a functional interface returning `S3Credentials` (access key id, secret key, and a session token that is `null` for long-term keys). `S3Api` calls the provider once per signed request and caches nothing, so a provider that fetches or refreshes credentials is responsible for its own caching and expiry.

`SdkCredentialsProviders` in `hardwood-aws-auth` adapts the AWS SDK: `defaultChain()` wraps `DefaultCredentialsProvider` (environment, profile files, container and instance metadata; its SSO and web-identity sources need the SDK's `sso`, `ssooidc` and `sts` modules, which `hardwood-aws-auth` does not bring, so the CLI does not resolve them), and `fromProfile(name)` wraps one named profile. Session credentials from the SDK map to `S3Credentials` with their token; other credentials map without one. Callers who already have the SDK can write the same few-line adapter themselves.

An anonymous source has no provider. `S3Api` then sends each request with its `Range` header alone: no `Authorization`, no `x-amz-*` headers, on retries as on first attempts. S3 serves such requests for objects that allow public read and denies the rest with `401` or `403`; since the server's error does not say the request was unsigned, `S3Fetcher` appends that to the message of a `401` or `403` on an anonymous source.

The CLI decides between signing and unsigned requests once per command, in `FileMixin`, and shares the resulting source across every open of the file. It signs with `defaultChain()` when the chain resolves credentials. When the chain fails, it sends unsigned requests only if no local credential source is configured, so that a broken profile, an expired token or an `AWS_PROFILE` naming no profile fails with the chain's error instead of a denied unsigned request; otherwise it reports the chain's error. The SDK's chain reports every failure as the same `SdkClientException`, whether no source is configured or a configured one failed, so `CredentialSources` (`hardwood-aws-auth`, internal) tells the two apart by inspecting configuration before the chain runs. It counts a source as configured when any of the access-key, secret-key, session-token, web-identity token file or container credentials settings is set, so a half-configured source also reports the chain's error, when `AWS_PROFILE` is set, or when the `default` profile carries a property the SDK derives credentials from. Instance metadata has no local configuration; it is consulted through the chain, so on EC2 an instance role is used and an instance without one reads unsigned. A credential source that a later SDK version adds to the chain is not seen by `CredentialSources` until it is added there, and until then a failure of that source leads to unsigned requests.

Region is not part of the credential contract. The CLI resolves it without the SDK's region chain, which may block on the instance metadata endpoint: `aws.region` system property, then `AWS_REGION`, then `AWS_DEFAULT_REGION`, then the `region` of the `AWS_PROFILE` (or `default`) profile in `~/.aws/config`. No region and no endpoint is an error; no region with an endpoint falls through to the source's `"auto"`. The CLI takes the endpoint from `aws.endpointUrl` or `AWS_ENDPOINT_URL` and path-style from `aws.pathStyle` or `AWS_PATH_STYLE` (`FileMixin`); the user-facing list is in the [CLI reference](../docs/content/reference/cli.md).

Tests: `SdkCredentialsProvidersTest` (aws-auth), `CredentialSourcesTest` (aws-auth), `S3ApiAnonymousTest` (s3), `S3AnonymousIT` (s3), `S3CredentialResolutionCommandIT` (cli), which covers the unsigned fallback, a broken profile, an instance role served by a local metadata endpoint, the no-region error, and configuration through environment variables, and the `*S3CommandIT` classes (cli), which set the `aws.region`, `aws.endpointUrl` and `aws.pathStyle` system properties. The `~/.aws/config` region fallback is untested.

## Request signing

`Aws4Signer` implements AWS Signature Version 4 with `MessageDigest` and `Mac` and no AWS types. It is a pure function of method, URI, headers, payload hash, credentials, region, service and timestamp, returning the `Authorization` value and the full set of headers to send.

- Canonical request: method, canonical URI, canonical query string, lower-cased sorted headers, signed header names, payload hash. S3 signing uses single URI encoding and no path normalization, because object keys may contain `//` and `..` that normalization would rewrite into a different key. `S3Api` always signs with normalization off; the `normalize` variant exists for the conformance suite.
- String to sign: `AWS4-HMAC-SHA256`, timestamp, scope `{date}/{region}/s3/aws4_request`, and the SHA-256 of the canonical request.
- Signing key: the HMAC-SHA256 chain over date, region, service and `aws4_request`, seeded with `"AWS4" + secret`.
- Payload hash: `SHA-256("")` for body-less `GET`; the body's hash for `PUT`. It is sent as `x-amz-content-sha256` and is part of the canonical request.
- Session token: when present, `x-amz-security-token` is added before signing, so it is covered by the signature; when absent the header is omitted entirely.
- Host: the signed `host` must equal the host the server receives. `HttpClient` leaves the scheme's default port (80 for `http`, 443 for `https`) out of the HTTP/1.1 `Host` header but keeps it in the HTTP/2 `:authority`, so `S3Api` drops a default port from the endpoint, and no request URI carries one. The signed `host` is the request URI's host and any explicit port.
- URI encoding: every byte except `A-Za-z0-9-._~` is percent-encoded with upper-case hex; a space is `%20`.

The signing region comes from the source; for S3-compatible services that ignore it, `"auto"` is an arbitrary but valid value. SigV4a (multi-region access points), presigned URLs and streaming (chunked) signatures are not implemented; none of them is needed for body-less range `GET`s.

Tests: `Aws4SignerTest` (s3), which runs the SigV4 conformance vectors of `awslabs/aws-c-auth` (cloned at test time by `SigningTestSuite`) and asserts canonical request, string to sign and signature for each, in both normalization modes and with session tokens; `S3ApiHostSigningTest` (s3), which verifies the signature of requests to endpoints with no port, a default port and a non-default port against the headers received, and that request URIs carry no default port.

## S3InputFile

`S3InputFile` is the public `InputFile` for one object. It is a facade over a single delegate chosen in its constructor from the source's `RangeBacking`: the package-private `S3Fetcher` under `NONE`, or a `RangeBackedInputFile` wrapping that fetcher under `SPARSE_TEMPFILE`. Every `InputFile` method delegates unconditionally, so the read path carries no per-call branch on the mode. The facade keeps a direct reference to the fetcher for the network counters (below). `S3Source` returns the concrete `S3InputFile` type so callers reach the counters without a cast and the internal `RangeBackedInputFile` never appears in public signatures. `name()` is `s3://{bucket}/{key}` in either mode.

### Open: suffix-range tail fetch

`S3Fetcher.open()` issues one `GET` with `Range: bytes=-65536` instead of a `HEAD`. The response carries the object size in `Content-Range` (`bytes a-b/size`), falling back to `Content-Length` when the object is smaller than the window and the server answers `200` with the whole body. The same round-trip returns the last 64 KB of the object, which usually contains the Parquet footer and its length field, so opening a file costs one request instead of two. The window size is a cost heuristic, not an assumption about file layout: a footer larger than the window is read by a second range request, because the tail serves only reads that lie entirely inside it. Opening is idempotent.

The tail bytes are held in a direct buffer for as long as the file is open, so slices handed to FFM-based decompressors are native memory. Any `readRange` wholly inside the tail window is answered from it without a request, in both backing modes.

### Object identity

`S3InputFile.identity()` is the `ETag` header of the response to the suffix-range `GET`, taken as the server sends it, quotes included, and empty when the server sends none. It costs no request beyond the open, is fixed when the file opens, and names the object version whose tail and length the open observed. S3 changes the `ETag` whenever an object's content changes, which is what a `MetadataSource` footer check relies on ([INPUT_FILES.md](INPUT_FILES.md#identity)). Range reads after the open do not send `If-Match`, so the identity names the content at open time and not what a later range read returns. The s3proxy file-system backend the integration tests run against sends no `ETag`; `S3InputFileIdentityTest` covers the identity against a local endpoint that does.

### Range reads

Outside the tail, every `readRange(offset, length)` is one signed `GET` with `Range: bytes={offset}-{offset+length-1}`, streamed into a freshly allocated direct buffer of exactly `length` bytes. A body shorter than `length` raises `IOException` ("Short read"). A zero-length read issues no request. The fetcher does not split, merge or cache ranges; request shape is decided above it by fetch planning ([FETCH_PLANNING.md](FETCH_PLANNING.md)).

Under `RangeBacking.NONE` an S3 object of any size is readable within the limits of [INPUT_FILES.md](INPUT_FILES.md#size-and-offset-limits).

### Network counters

`networkRequestCount()` and `networkBytesFetched()` count the fetcher's requests: the open-time tail fetch with its actual response size, and every `readRange` that goes to the network with its requested length. A range read is counted before it is sent, so a failed one counts; the retries inside `S3Api` do not. Tail hits are served before the counters move, and range-cache hits never reach the fetcher, so both counters mean network traffic in both modes. `hardwood dive` shows them through `ParquetModel` ([DIVE_ARCHITECTURE.md](DIVE_ARCHITECTURE.md)).

### Thread safety

An opened `S3InputFile` is safe for concurrent `readRange` calls. The fetcher's length and tail are written once in `open()` and read-only afterwards, published as the [`InputFile` contract](INPUT_FILES.md#contract) describes; the counters are atomic; `HttpClient` is thread-safe.

Tests: `S3InputFileIT` (s3), `S3InputFileLargeFileIT` (s3), `S3MultiFileIT` (s3), `S3SelectiveReadJfrIT` (s3).

## S3OutputFile

`S3Source.outputFile(...)` returns an `S3OutputFile`, which implements the [output contract](WRITER.md#outputfile). It fills buffers of the source's `uploadPartSize` (default 8 MiB, at least the 5 MiB S3 requires of every part but the last), the first allocated on `create()`, and uploads up to four parts at once, each on a virtual thread of its own.

| Event | Requests |
|---|---|
| Buffer fills | `CreateMultipartUpload` the first time, on the writing thread. The buffer is handed to an `UploadPart` of the next part on a virtual thread, and writing continues in a spare buffer. With four parts in flight, the writing thread first waits for the oldest |
| `close()` without a full part ever | One `PUT` of the buffered bytes |
| `close()` after a part | `UploadPart` of the remainder, if any, a wait for every part, then `CompleteMultipartUpload` listing each part's `ETag` in part order |
| `discard()`, or a failed `close()` | Waits for the part uploads in flight to end, then `AbortMultipartUpload`, if an upload was started |

Two invariants follow. Nothing is visible at the key until the closing `PUT` or completion succeeds, which is what lets the writer's `discard()` leave the destination as it was. And an output holds at most five parts in memory however large the file grows, four uploading and one filling, so uploading overlaps the writer's encoding by four parts and no further. A part that fails is raised by the next part handed off or by `close()`, so the writer stops within one part of a failure. The abort waits for the uploads in flight to end, without interrupting them, because S3 keeps a part whose upload completes after the abort; the wait and the abort run with the thread's interrupt status cleared, so an interrupted writer still releases its parts. A part upload has to complete within `requestTimeout`, which with four uploads sharing the connection's bandwidth bounds the part size a slow link can carry. Ten thousand parts is S3's limit, so a file is capped at 10,000 × `uploadPartSize`; a write that would cross it is rejected before any of it is buffered.

S3 sends a completion's `200` status before the completion is done, and reports a failure that follows as an `Error` document in that body; `S3Xml` reads the body and raises it as a failed completion, which is retried like a `500` when its code is `InternalError`, `SlowDown` or `ServiceUnavailable`. Every request is retried as in [Errors and retries](#errors-and-retries). A retried `CreateMultipartUpload` whose first response was lost leaves an upload the output does not know of, and an output whose process ends leaves its started upload behind; both are removed only by a bucket lifecycle rule. A completion that succeeded but whose response was lost through every retry makes `close()` throw although the object exists, the one case where a failed `close()` does not leave the destination unchanged.

A source built with `anonymous()` refuses `outputFile(...)` with an `IllegalStateException`, since S3 does not accept unsigned writes to a bucket that is not public-write.

Tests: `S3OutputFileTest`, `S3XmlTest`, `S3SourceOutputTest`, `S3OutputFileIT` (s3). The lost-completion case is untested.

## Range backing

`RangeBacking` selects whether an `S3InputFile` keeps the bytes it fetches beyond the tail. Both modes return identical bytes; they differ in how many requests go out and in side effects on the host.

| Mode | Behaviour |
|---|---|
| `NONE` (default) | Every read outside the tail is a network `GET`, including a repeat of an earlier range. No temp files, no growth of the resident set beyond the buffers the reader holds. |
| `SPARSE_TEMPFILE` | A whole-file cache: fetched ranges are written into a sparse temp file mapped `READ_WRITE`; any read covered by already-fetched bytes is a zero-copy slice of the mapping. |

Under `SPARSE_TEMPFILE` every fetched range is also written through the mapping to the temp file: it stays in memory when the temp directory is on tmpfs, and otherwise costs disk writes proportional to the bytes fetched.

The mode is set on `S3Source`, not per file, because it follows the workload: interactive re-reading or one-pass streaming. `NONE` is the default so that a caller who did not ask for caching gets no temp-file writes, no temp-file failure at `open()`, and a resident set bounded by active row groups rather than by the bytes touched. In the CLI, `FileMixin.toInputFile(RangeBacking)` takes the mode from the command: `DiveCommand` passes `SPARSE_TEMPFILE`, every other command uses `NONE`. `HadoopInputFile` in `parquet-java-compat` uses the default.

### RangeBackedInputFile

`RangeBackedInputFile` (`dev.hardwood.internal.reader`, in `core`) is an `InputFile` decorator that knows nothing about S3; any remote `InputFile` can sit beneath it. Its structure is the remote analogue of `MappedInputFile`: a buffer of the file's size that the rest of the pipeline slices into, without knowing whether the pages behind it came from the kernel's page cache or from the network.

- `open()` opens the delegate (for S3, the tail fetch), creates a temp file in `tempDir`, truncates it to the file length and maps it whole, read-write. A failure after the temp file exists deletes it. On filesystems with sparse files the untouched holes take neither disk nor memory, so the real footprint is the bytes fetched, while the virtual reservation is the file length.
- `readRange()` asks a `RangeSet` (a sorted map of disjoint, non-touching intervals, merged on insert) for the gaps in the requested range, fetches each gap from the delegate, writes it at its absolute offset, marks it populated, and returns a slice of the mapping. A read spanning a hole fetches only the hole. Reads outside `[0, length)` are rejected.
- `close()` drops the mapping, closes the channel, deletes the temp file and closes the delegate. The unmap is left to the garbage collector, which offers the only portable release, so the address space outlives `close()`. Where the platform refuses to delete a mapped file (Windows), the delete is deferred to JVM exit and logged rather than failing the close.

`open`, `readRange` and `close` hold the instance monitor, across the refill included. A reader therefore never sees a range that is marked populated but only partly written, and two threads missing the same range serialize so that the second finds it populated and issues no request. The price is that one miss blocks every other read of the file, cache hits included, for the duration of the delegate fetch.

Composition with the layers around it:

- The tail sits below the cache, in the fetcher. Under `SPARSE_TEMPFILE` a footer read fills the mapping from the tail buffer, and neither layer counts it as network traffic.
- `SharedRegion` and per-work-item buffers sit above it ([FETCH_PLANNING.md](FETCH_PLANNING.md)). A region's bytes are released with its work item; the same bytes stay in the file-level cache, so a later region covering the same range is a cache hit.
- Local files do not use it: `MappedInputFile` already is a whole-file mapping ([INPUT_FILES.md](INPUT_FILES.md)).

Constraints of the mode:

| Constraint | Consequence |
|---|---|
| The mapping and its slices are `int`-addressed | `open()` of a file longer than `Integer.MAX_VALUE` bytes throws `UnsupportedOperationException` naming `RangeBacking.NONE` as the way to read it. |
| One mapping and one temp file per open file | Many open files reserve the sum of their lengths in address space; there is no eviction and no global cap. |
| A usable temp directory is required | `S3Source.Builder.build()` rejects a `tempDir` that is missing, not a directory, or not writeable when the mode is `SPARSE_TEMPFILE`, so the mistake surfaces at configuration rather than at the first `open()`. `build()` validates every option before it builds the `HttpClient`, so a rejected configuration leaves no client behind. |
| The object is assumed immutable while open | No conditional `GET`s or revalidation: an object overwritten during a session can yield a file assembled from two versions. The same holds under `NONE` for any two reads of one open file. |
| Cache is per JVM and per open file | Nothing is shared across processes or across reopenings of the same object. |

Tests: `RangeBackedInputFileTest`, `RangeSetTest`, `S3RangeBackingIT` (s3), `S3SourceTempDirValidationTest` (s3). The over-2 GB rejection and the single-flight behaviour under concurrent misses are untested.

## Errors and retries

Every failure from the network path is an `IOException`, the checked type the `InputFile` contract declares. An HTTP error's message carries the object's `s3://` name and the status; a final network failure is the `HttpClient`'s own `IOException`. The status is not mapped to a finer type: a missing object (`404`), a denied request (`403`) and an exhausted retry budget reach the caller as the same exception type with different messages. How `IOException` sits among the reader's exception categories, and why a file too large for the range cache is `UnsupportedOperationException` instead, is in [EXCEPTION_MODEL.md](EXCEPTION_MODEL.md).

| Where | Failure | Result |
|---|---|---|
| `open()` | status other than `200`/`206` | `IOException` "Failed to open {name}: HTTP {status} {body}", plus the unsigned-request note for a `401`/`403` on an anonymous source |
| `open()` | neither `Content-Range` nor `Content-Length`, or an unparseable `Content-Range` | `IOException` |
| `readRange()` | before `open()`; a range outside `[0, length)` | `IllegalStateException`; `IndexOutOfBoundsException`, before any request |
| `readRange()` | `200`, from a server that ignored `Range` | `IOException` naming the range, the object and the status, saying the endpoint ignored the `Range` header |
| `readRange()` | any other status except `206` | `IOException` naming the range, the object, the status and the body, plus the unsigned-request note for a `401`/`403` on an anonymous source |
| `readRange()` | body ends before `length` bytes | `IOException` "Short read" |
| any request | interrupt | `IOException` with the interrupt flag restored |
| `S3Source.Builder.build()` | neither credentials nor anonymous, both, missing region without endpoint, unusable `tempDir` | `IllegalStateException` |

`S3Api.sendWithRetry` retries a request up to `maxRetries` times when the response is `500` or `503` or when `HttpClient.send` throws an `IOException`; the body of a response it retries past is closed, so a retry does not hold a connection. Each attempt is signed afresh (unless the source is anonymous), so a retry after a credential refresh carries the new credentials and a current timestamp. Backoff between attempts is exponential with a cap and random jitter. When the budget is spent, a final `500`/`503` response is returned to the fetcher, which raises it as an HTTP error; a final network failure is rethrown as is. Other statuses, including `429` and every `4xx`, are not retried.

The retry boundary is `send()`. For the open-time tail the body is read inside `send()`, so a failure mid-body is retried; for `readRange` the body is streamed after `send()` returns, so a connection lost mid-body surfaces as an `IOException` from the stream without a retry. A write sends its body from a byte array, so each attempt sends the same bytes.

Tests: `S3InputFileIT` (s3), `S3FetcherStatusTest` (s3), `S3ApiRetryTest` (s3). Backoff, retry exhaustion and the non-retried statuses are untested.

## Hadoop configuration bridge

`HadoopInputFile` in `parquet-java-compat` opens `s3://`, `s3a://` and `s3n://` paths through `S3Source`, configured from `fs.s3a.access.key`, `fs.s3a.secret.key`, `fs.s3a.aws.credentials.provider` (S3A's anonymous provider, or the AWS SDK v1 name S3A maps to it, selects an anonymous source and the keys are not used; a list naming the anonymous provider among others is rejected, since S3A's in-order fallback has no counterpart in `S3Source`; any other value is ignored), `fs.s3a.endpoint`, `fs.s3a.endpoint.region` (default `us-east-1`) and `fs.s3a.path.style.access`. It declares `hardwood-s3` as an optional dependency and reaches it by reflection, so it need not be on the runtime classpath; when it is absent, opening an `s3://` path fails with an `UnsupportedOperationException` naming the missing artifact. A failure to build the source, such as missing credentials, is raised as `UnsupportedOperationException` too. Because each such file has a source of its own, the returned file closes that source, and with it the `HttpClient`, when the file is closed.

Tests: `ParquetReaderS3CompatIT` (parquet-java-compat).

## Boundaries

- **Parallel range requests (#260).** A wide column chunk is fetched in sequential range requests, never split into parallel ones.
- **Latency-aware coalescing (#763).** The gap policy does not adapt to the latency of the remote backend ([FETCH_PLANNING.md](FETCH_PLANNING.md#gap-policy)).
- **Other remote backends (#519).** Only S3 is a remote backend; there is no plain HTTP(S) `InputFile`.
- **Range-backed files over 2 GB (#501).** See [INPUT_FILES.md](INPUT_FILES.md#boundaries).
- **Not tracked.** Requester-pays buckets, SigV4a, eviction or a global size cap for the range cache, and revalidation of an object that changes while open.
