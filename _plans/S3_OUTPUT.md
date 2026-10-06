# #1454 S3 Output: Analysis and Delivery

Implementation follows the reviewed phases below. The proposed end state is
[_designs/S3_OUTPUT.md](../_designs/S3_OUTPUT.md). Contract decisions below remain
unresolved until discussed with the maintainer.

## Accepted design direction

The tentative architecture includes a fresh random UUID per output, stored as
`x-amz-meta-hardwood-write-id` on small-object PUT or multipart initialization.
The UUID stays stable for that output's retries and metadata verification. A lost
final response triggers bounded HEAD checks: matching UUID and final byte length
confirm publication; unresolved results remain explicitly unknown.

The caller's chosen key remains unchanged. Independent writers generate IDs without
a registry. UUIDs identify writes but do not prevent same-key replacement, and
automatic filename changes or version-history recovery are not included. This
direction is accepted for the draft; the maintainer may recommend changes in review.

## Research baseline and contribution rules

Reviewed upstream `main` at `e4be4824ad08080e91c2ff3f544b2b925ef2fd5c`, fetched on
2026-10-05. Implementation uses `1454-s3-output`, based on upstream `main` at
`292654e5`, separately from `1101-jump-to-row-or-row-group`.
Existing untracked `AGENTS.md` and `_designs/DIVE_COLUMN_SHAPE.md` are unrelated.

`CONTRIBUTING.md` requires issue-linked changes, behavior tests, small public API,
usage documentation with API changes, and a full `./mvnw clean verify` before
pushing. `*IT` names select tests requiring s3proxy; local HTTP protocol tests are
`*Test`. IOException represents destination/protocol failures; API misuse retains
its own exception type. Imports follow repository conventions, `var` is forbidden,
and JavaDoc uses `///` Markdown. Build plugins remain version-managed in the parent.
Every Maven invocation must have the local instructions' 180-second timeout.

The supplied `AGENTS.md` asks for system-design work under `_designs/` before
implementation. The contribution guide separates current subsystem documentation
from delivery plans under `_plans/`. This pair keeps a proposed end-state design
in the requested location and planning/research material outside the subsystem
design. Existing subsystem docs and roadmap completion boxes stay unchanged until
implementation lands. The design and plan were committed with phase 1; no
external PR is part of this local implementation workflow.

## Issue and linked context

### Direct requirements and failure-contract history

| Reference | Finding and relevance |
|---|---|
| [#1454](https://github.com/hardwood-hq/hardwood/issues/1454) | Open enhancement: lazy multipart initialization, single PUT for sub-part output, bounded payloads, completion on close, abort on discard, s3proxy failure/round-trip tests, API docs and design updates |
| [#1291](https://github.com/hardwood-hq/hardwood/issues/1291) | Parent writer epic; stage 22 is this backend. Other stages are separate work |
| [#1047](https://github.com/hardwood-hq/hardwood/issues/1047) | Closed: a write failure must poison the writer so try-with-resources cannot publish a good-looking truncated prefix |
| [PR #1075](https://github.com/hardwood-hq/hardwood/pull/1075) | Closed predecessor with `COMMIT_PREFIX`; Gunnar closed it in favor of #1251. Do not reinstate that policy |
| [PR #1251](https://github.com/hardwood-hq/hardwood/pull/1251) | Merged: failed writes discard; public `abort()` covers producer failures invisible to the writer; cleanup errors are reported. Gunnar explicitly justified producer/database/Kafka failures and job cancellation |
| [#1035](https://github.com/hardwood-hq/hardwood/issues/1035), [PR #1036](https://github.com/hardwood-hq/hardwood/pull/1036) | Closed/merged: schema/configuration validation before destination acquisition, cleanup after failures during writer creation |
| [#1253](https://github.com/hardwood-hq/hardwood/issues/1253), [PR #1256](https://github.com/hardwood-hq/hardwood/pull/1256), [PR #1367](https://github.com/hardwood-hq/hardwood/pull/1367) | Rejected records may be returned from `tryWriteRow` without poisoning the writer; #1256 is closed and #1367 is merged. Strict throwing writes still discard |
| [#1147](https://github.com/hardwood-hq/hardwood/issues/1147) | Existing in-memory sink; clarifies that whole-file buffering costs the file size and is unsuitable as the large-file S3 implementation |
| [#9](https://github.com/hardwood-hq/hardwood/issues/9) | Original writer milestone, now closed; the core writer is already available |
| [#1290](https://github.com/hardwood-hq/hardwood/issues/1290), [PR #1292](https://github.com/hardwood-hq/hardwood/pull/1292) | Completed documentation split; intended end-state designs and delivery planning have separate homes |

The #1454 timeline cross-reference points to #1291; it contains no linked
implementation PR at this baseline. The issue's observed comment expresses the
contributor's interest, with no additional maintainer requirements yet. A PR search
returned unrelated #723; it is not treated as a linked implementation.

### Other work linked by the parent epic

These issues were checked for overlap. They do not need implementation for #1454.
Historical issue descriptions can predate the current code; the upstream code and
merged contract changes are the behavioral baseline.

| Reference | Relationship |
|---|---|
| [#1426](https://github.com/hardwood-hq/hardwood/issues/1426) | Page indexes shipped; include their footer-adjacent bytes in multipart boundary tests |
| [#1449](https://github.com/hardwood-hq/hardwood/issues/1449) | Bloom filters shipped; they use the same sequential output and must pass unchanged |
| [#1083](https://github.com/hardwood-hq/hardwood/issues/1083) | Encoding statistics shipped; no S3-specific encoding change |
| [#982](https://github.com/hardwood-hq/hardwood/issues/982) | Open exact distinct-count refinement; independent |
| [#985](https://github.com/hardwood-hq/hardwood/issues/985) | Caller row-group boundaries shipped; they are not S3 part boundaries |
| [#957](https://github.com/hardwood-hq/hardwood/issues/957) | Open field-ID preservation; avoid claiming S3 output adds table-format rewrite fidelity |
| [#791](https://github.com/hardwood-hq/hardwood/issues/791) | Open rewrite/compaction capability; S3 output provides a destination but does not implement rewriting |
| [#1424](https://github.com/hardwood-hq/hardwood/issues/1424) | Input-form proposal dropped by the epic after benchmarks; no new ColumnBatch input API |
| [#1455](https://github.com/hardwood-hq/hardwood/issues/1455) | Parallel column encoding, not parallel S3 uploads; preserves ordered OutputFile writes and single-caller confinement |
| [#1045](https://github.com/hardwood-hq/hardwood/issues/1045), [#989](https://github.com/hardwood-hq/hardwood/issues/989) | Row-layer staging/performance and benchmark work; useful background, not this feature's acceptance |

Further links inside these broader epics concern other writer features or project
history. They are not treated as additional dependencies of the storage backend.

## Existing code and integration points

| File or area | Observed behavior |
|---|---|
| `ARCHITECTURE.md` | Core implements Parquet directly; storage backends sit below its reader/writer |
| `core/.../OutputFile.java` | Five-method sequential sink; public close/discard wording promises unchanged destination after failed publication |
| `core/.../writer/ParquetFileWriter.java` | Validates before `out.create()`, cleans up failed construction, latches throwing writes, handles caller abort, flushes footer before calling `out.close()` |
| `core/.../internal/writer/ChannelOutputFile.java` | Temporary sibling and atomic replacement; existing object preserved until close |
| `core/.../InMemoryOutputFile.java` | Growable full-file heap storage; unrelated to bounded multipart buffering |
| `core/.../internal/writer/ColumnChunkBuffer.java` | Emits headers and valid prefixes of encoded page arrays as ByteBuffers; no seek or known total size required |
| `s3/.../S3Source.java` | Shared client/config, public input factories, private URI parser, source/client ownership; output factory can pass internal S3Api to the sink |
| `s3/.../internal/S3Api.java` | Signed GETs retry 500/503 and I/O; fixture PUT currently has no retry, payload hash covers the entire byte array; no multipart methods, XML parser, or metadata HEAD helper |
| `s3/.../internal/Aws4Signer.java` | POST/DELETE signing can reuse it; canonicalizes query parameters; add only a slice-hash helper needed for valid prefixes |
| `s3/.../S3InputFile.java`, `S3Fetcher.java` | Direct range reads; last 64 KiB retained at open; optional sparse-file caching is read-side behavior |
| `s3/.../internal/S3ApiRetryTest.java` | Local HttpServer, scripted failures, explicit client/server teardown; model for deterministic write protocol tests |
| `s3/.../internal/S3ApiHostSigningTest.java` | HTTP proxy permits endpoint/Host signing tests for virtual-hosted and path-style requests without DNS tricks |
| `core/.../writer/WriterFailureTest.java` | Existing poisoning, creation/close cleanup, abort, codec, and filler failure coverage; reuse the behavior through the S3 sink |
| `test-support/.../ContainerS3Proxy.java` | Filesystem storage bind is READ_ONLY; upload ITs cannot work through this configuration unchanged |
| `test-support/.../TestBucket.java` | Creates fixtures by local filesystem copy/hard-link, not by upload; useful bucket isolation but not proof of a writer upload. Writable tests must not overwrite hard-linked source fixtures |
| `docker-compose.yaml` | Shared proxy also mounts `/data:ro`; it needs writable storage for the same upload ITs |
| `s3/pom.xml` | No production AWS SDK dependency; existing JUnit/AssertJ/test-support are sufficient |

The build uses Java 25 but targets a Java 21 baseline. Internal XML/HTTP helpers
must use baseline APIs. A native-image smoke check is relevant if the new JDK XML
factory is reachable from CLI production paths; no native build is needed merely
to draft this proposal.

## Architecture recommendation and tradeoffs

Use one reusable part buffer and synchronous ordered upload. It naturally applies
backpressure, has no worker lifecycle, and makes payload ownership and cancellation
easier to reason about. Reuse the transport/signing stack without changing the core
writer's byte layout.

Whole-file memory buffering grows with the file and does not satisfy the bounded
memory requirement. Local-file staging retains the workflow #1454 removes.
Background part upload can improve throughput but brings buffer pools, delayed
failures, cancellation races, and executor ownership into scope. It is unnecessary
for the explicitly sequential first backend.

Return `OutputFile` rather than expose an S3-specific public type. A source-level
`uploadPartSize(int)` is the one proposed tuning option: it is needed because
10,000 fixed parts cap total output size, and their size also governs memory and
per-request throughput. No public uploader, receipt, retry-policy enum, or parallel
executor option is proposed.

The backend assigns one random UUID to each output and places it in S3 metadata.
Only uncertain publication responses trigger a HEAD check; normal successful
publication needs no extra request. Recovery requires both matching ID and length.
The reserved metadata key is internal and needs no public write-ID configuration.

Part retries repeat identical bytes at the same identified part number. Final
publication is not automatically replayed: HEAD verification gathers evidence
without another write. Replaying a small-file PUT could overwrite another client's
object or create another version. Multipart completion has its own server state
and may return NoSuchUpload after a successful first completion; treating its
retry behavior as a generic PUT or GET would still not establish the outcome.
The conservative completion replay policy remains a maintainer-review tradeoff.

## Contract decisions requiring maintainer review

### 1. Lost responses and the absolute failure guarantee

Two concrete scenarios conflict with a literal reading of #1454 and OutputFile:

```text
POST ?uploads -> S3 allocates upload U -> response lost
Hardwood has no U, so AbortMultipartUpload(U) is impossible.

POST ?uploadId=U -> S3 completes the object -> response lost
Hardwood throws, but the object exists and U may no longer exist.
```

The same publication ambiguity exists for the small-file PUT. Aborting U cannot
delete a completed object. Blind deletion could destroy the previous or another
writer's object. HEAD existence alone does not identify ownership. A matching
write UUID and length can positively identify this output; a non-match still
cannot establish whether it failed or was subsequently replaced. This is a
distributed-operation limitation, not fixed by a larger retry count.

Recommendation: clarify the public contract for remote outputs. Before any
publication request, failed writes never publish a prefix and known uploads are
aborted. An uncertain final response first triggers bounded metadata verification.
An exact UUID/length match lets close return successfully. If verification is
denied, interrupted, unavailable, or exhausts its budget without a match, publication
remains unknown and the IOException retains the original publication cause and
verification details. Cleanup comes after verification, so it does not abort an
in-progress completion before giving it a chance to become visible.

An initialization without a recoverable upload ID remains an explicitly reported
unconfirmed cleanup outcome; object HEAD cannot discover unfinished multipart
uploads. Generic IOException remains the public exception category. Verification
does not recover a failed row/batch write, codec, incomplete footer, or cancellation.

This recommendation is a proposed contract clarification, not an already-approved
interpretation of #1454. If the requirement instead insists on strict rollback
after any exception, the direct-to-key generic backend cannot meet it as stated.
Write-ID metadata verification is included in this proposal, but still cannot
prove rollback during every partition. Object versioning, exclusive keys, or a
staging-key publication protocol offer other recovery guarantees and require
additional assumptions/APIs. They are outside this proposal. A separate typed
public outcome exception is also a possible API decision if callers need
machine-readable uncertainty; it is not part of the minimal proposal.

### 2. Replacement and concurrent writers

Recommendation: successful close replaces the target, matching local output;
known failures leave its old contents unchanged. Abort addresses only this
output's upload ID. No unconditional object deletion. Concurrent publication to
the same key receives normal S3 semantics, without compare-and-swap.

Random UUIDs are generated independently for every output, so writers do not need
a coordination service. That identifies each write but does not preserve its ID
after another writer replaces the same object. A HEAD response with another UUID
does not prove our write failed. Distinct keys are an application-level option;
version-history recovery would add listing/version-read APIs and permissions.
The backend does not silently change filenames or enable bucket versioning.

If the maintainer wants create-only output or conditional replacement instead,
settle it before implementation and apply the same policy to PUT and completion.
Conditional writes do not remove lost-response uncertainty.

### 3. Cleanup verification and permissions

Recommendation: sequential acknowledged uploads use a normal abort. After an
uncertain part request, bounded abort/ListParts verification handles AWS's documented
race with server-side parts still running. This requires ListMultipartUploadParts
permission in addition to PutObject and AbortMultipartUpload.

Metadata verification requires `s3:GetObject` on the destination for HEAD. Existing
readers normally already have that permission; upload-only users may not.
`s3:ListBucket` is optional for reading existing metadata but affects whether
a missing key produces 404 or 403. Neither 403 nor 404 proves publication failed.
403 stops verification as inconclusive, while 404 can consume another pending
check within the budget. IAM permission is granted by the application's/bucket's
administrator, not by the library. The repo has caller-supplied credentials and
local s3proxy test credentials, not a project-wide AWS permission policy; live
permissions remain unverified without a specific bucket and credential context.

Use at most `maxRetries + 1` HEAD attempts, sharing the budget across non-matches
and transient failures, with existing per-request timeout/backoff. Successful
publications make no HEAD call. Slow-completion tests must verify this bounded
recovery behavior; a short budget can legitimately end with an unknown outcome.
No new public verification timing option is proposed until API review requires one.

Agree whether that additional permission is acceptable. A narrower implementation
with abort only must document best-effort cleanup after timeout/cancellation and
must not promise verification. Never use a broad list-and-abort sweep as recovery.

### 4. Part size and API stability

Recommendation: the two factory overloads and an 8 MiB default with an int-sized
`uploadPartSize` option. That default caps outputs at 78.125 GiB; larger outputs
require a larger configured buffer. Source builder placement, default size, and
whether these factories should be marked `@Experimental` need normal API review.
The core OutputFile sink remains unchanged except agreed contract documentation.

## Delivery checklist

### Implementation phases

Each phase is completed and verified before contributor review. Commit it only
after approval, then begin the next phase. Commits reference #1454.

| Phase | Deliverable | Verification |
|---|---|---|
| 1 | Internal multipart XML parsing, ordered completion serialization, and response validation | JVM tests for namespaces, opaque IDs/ETags, malformed or duplicate fields, embedded errors, and unsafe XML |
| 2 | Signed write/HEAD transport, payload-prefix hashing, bounded responses, whole-response deadlines, and operation-specific retries | Local HTTP tests for signing, encoding, body limits, timeouts, retries, and list-parts pagination |
| 3 | Sequential output buffer, multipart lifecycle, capacity guards, and discard cleanup | Boundary, ByteBuffer, lifecycle, limits, interruption, and cleanup tests |
| 4 | Per-output UUID metadata and bounded publication verification | Lost-response, delayed completion, mismatch, concurrent replacement, and permission-denial tests |
| 5 | Public S3Source output factories and part-size configuration, with usage/failure documentation | Public input-validation and writer-contract tests; documentation checks |
| 6 | Writable s3proxy round trips and failure coverage; subsystem designs and roadmap updates | S3 integration tests and full repository verification |

Phase 1 begins on `1454-s3-output`, based on upstream `main` at
`292654e5`. Phases 1–4 do not expose a user-facing output factory. The final
documentation and roadmap mark the feature implemented only after phase 6.

Phase 1 was approved and committed as `9d9e70f7`. Its 45 new XML cases and
full `./mvnw verify` passed.

Phase 2 was reviewed, approved, and committed as `25382d17`. Its
50 new tests pass, along with the 45 phase 1 XML cases, and full `./mvnw verify`
passed. The transport signs multipart, small-PUT, and HEAD requests, hashes only
the payload prefix, bounds response bodies, and applies request deadlines through
body completion. Bounded retries apply to parts and normal aborts. HEAD, paginated
listing, and the single-attempt abort helper leave retries to the later shared
recovery/cleanup budgets. No public output factory is exposed yet.

Phase 3 was reviewed, approved, and committed as `d629a89d`, based
on `25382d17`. The internal `S3OutputFile` owns one reusable part buffer, validates
whole-write capacity before consuming input, uploads full parts sequentially, and
publishes only on close. Failed writes latch a terminal failure and release local
payload storage. Cleanup preserves the original exception and interrupt flag;
uncertain part requests use a shared, bounded abort/list-parts budget and require
parsed `NoSuchUpload` to confirm removal. Explicit discard can retry unresolved
cleanup without replaying publication. Its 45 tests and the 95 existing protocol
tests pass, and full `./mvnw verify` passed. Public factories and documentation
remain phase 5.

Phase 4 is implemented and awaiting contributor review before committing, based
on `d629a89d`. Uncertain publication responses enter bounded HEAD verification
before multipart cleanup. A single UUID value and valid long Content-Length must
match this output before close can succeed. Pending/mismatching metadata,
HTTP 404, HTTP 500/503, and transport failures share one `maxRetries + 1` budget.
Permission errors and other non-transient HEAD failures stop verification.
Explicit validation/permission service errors remain failures even with a 5xx
status or matching metadata. Cancellation stops verification, attempts cleanup,
and preserves the interrupt flag. Unconfirmed publication retains the original
exception and adds a suppressed diagnostic with UUID, expected length, result,
and HEAD-attempt count. All 48 new recovery tests and 140 earlier sink/protocol
tests pass; full `./mvnw verify` passed with 17,164 tests, zero failures/errors,
and 75 skipped tests. No public factory is exposed yet.

- [x] Record the accepted per-output UUID and metadata-verification architecture in the design and analysis.
- [ ] Resolve lost-response contract wording, replacement semantics, cleanup verification, and minimal public API.
- [ ] Finalize the end-state design and submit the human-reviewed planning material if desired under CONTRIBUTING.md.
- [x] Create a feature branch from current upstream main; keep the existing navigation branch separate.
- [x] Add failing tests for new output behavior using local HTTP servers and existing S3 signing-test patterns.
- [x] Add signed multipart request/response handling, write-ID metadata, HEAD verification, secure XML, valid-prefix hashing, bounded response reception, and whole-response deadlines.
- [ ] Add the sequential internal sink, factory overloads, part-size validation, capacity guards, and lifecycle/cleanup rules.
- [ ] Make s3proxy filesystem mounts writable in both Testcontainers and compose; retain per-test isolated buckets and pinned images.
- [ ] Add actual small/multipart writer/read-back ITs and failed-write/caller-abort ITs; query pending uploads instead of assuming an absent object proves cleanup.
- [ ] Add fault-injection tests for protocol errors, lost responses, recovered publication, delayed/mismatching metadata, permission denial, interruption, and cleanup failure; use the matrix in the design.
- [ ] Update the how-to/reference pages, writer failure docs and Markdown JavaDoc; reconcile S3_STORAGE.md, WRITER.md, ARCHITECTURE.md, and package docs with implemented behavior.
- [ ] Update stage 22 in WRITER_SUPPORT.md and add/complete the S3 output capability in ROADMAP.md without rewriting unrelated stale inventory.
- [ ] Run the focused unit and s3proxy integration coverage, then full ./mvnw clean verify under a 180-second timeout.
- [ ] Fold the implemented design into the S3/writer subsystem docs; retire this plan and the temporary proposal document when their content is incorporated.

No core encoding algorithm or new dependency is required. Changing public contract
documentation does require cross-checking custom OutputFile behavior, but does not
justify a base-class refactor or changes to unrelated writer APIs.

## Acceptance mapping

| #1454 acceptance/scope | Verification |
|---|---|
| Direct sequential output | Recording protocol tests plus a real multi-part Parquet written by the sink |
| Deferred initialization and sub-part PUT | Request counts at create, small writes, first full part, and close |
| Bounded in-flight payloads | Blocking server/recording transport checks one outstanding part and a reused immutable-until-ack buffer; no whole-file staging |
| Close publishes | Validated PUT or completion response, or positive UUID/length verification after a lost response; completed file read through S3InputFile |
| Verification after uncertain publication | Bounded HEAD checks; matching ID and length recover success; old/new IDs, delayed completion, denied reads, or unreachable service remain inconclusive; no object-data download |
| Failed write leaves no object and no pending upload | Deterministic known-ID failure after uploaded parts; assert missing object and no matching incomplete upload |
| Discard aborts | Healthy caller abort, failed writer close, abort failure surfaced/suppressed |
| Reuse read stack | Signed requests including HEAD under existing region, endpoint, credentials, client ownership, and shared bounded retry settings |
| User and design docs | New factory/config usage docs; agreed failure wording; existing subsystem docs updated in implementation |

Lost initialization/publication responses cannot be covered by an assertion that
the client always undoes the service operation. Publication tests assert recovered
success when UUID and length match, and explicit uncertainty when verification
cannot confirm the result. Initialization tests retain the missing-upload-ID
limitation. All cases prohibit destructive rollback and hidden initialization or
publication retries. Residual unknown outcomes remain the principal contract
clarification for the maintainer.

## Verification and practical effort

Initial analysis inspected source and primary protocol documentation before
implementation. Each phase records its test coverage and verification result
before contributor review; no external issue or PR is modified by this workflow.

The feature is medium-sized after the contract decisions are settled. The original
2-4-day estimate assumed the happy-path upload and existing harness would suffice.
Allow roughly 4-6 focused development days for protocol/lifecycle implementation,
UUID/metadata recovery, fault injection, writable test infrastructure, documentation,
and final verification for a contributor familiar with the code. Allow additional
ramp-up and maintainer review time when returning to the project. Version-history
or staging-key recovery,
or parallel upload, would require re-estimating rather than fitting them into this
scope.

## Primary protocol references

- [Multipart upload limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html): 10,000 parts, 5 MiB-5 GiB parts, final-part exception, 48.8 TiB maximum.
- [CreateMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html): ID ownership and incomplete-upload lifecycle.
- [UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html): same-number replacement, ETag receipts, SigV4 payload integrity.
- [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html): HTTP 200 can carry an error, ordered receipt XML, long processing, conditional-write behavior.
- [AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html): server-side part races and ListParts verification.
- [ListParts](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html): part-number markers, pagination, and incomplete-upload inspection.
- [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html): whole-object publication, concurrent writes, replacement/versioning.
- [Object metadata](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html): custom write-ID metadata on creation and retrieval.
- [HeadObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html): metadata-only verification, Content-Length, GetObject permission, and missing-key status behavior.
- [S3 consistency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html): strongly consistent AWS object metadata after completed writes; does not mean an outstanding completion request has finished.
- [S3 Versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html): retains replaced objects but version-history recovery is outside the selected backend design.

These official pages were downloaded and read on 2026-10-05. Provider-specific
multipart limits and behavior need compatibility checks before claiming support.
