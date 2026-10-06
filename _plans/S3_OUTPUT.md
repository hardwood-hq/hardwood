# #1454 S3 output delivery

Status: completed.

Issue: [#1454](https://github.com/hardwood-hq/hardwood/issues/1454).

The completed design is documented in [S3 output](../_designs/S3_OUTPUT.md),
[S3 storage](../_designs/S3_STORAGE.md), and [Parquet writing](../_designs/WRITER.md).
Usage is documented in [Write to S3](../docs/content/how-to/write-to-s3.md).
The roadmap and writer delivery plan mark S3 output as implemented.

## Delivery record

| Phase | Delivered scope | Commit |
|---|---|---|
| 1 | Secure multipart XML parsing and response validation | `9d9e70f7` |
| 2 | Signed write/HEAD transport, deadlines, response bounds, and operation-specific retries | `25382d17` |
| 3 | Sequential output buffering, multipart lifecycle, capacity limits, and discard | `d629a89d` |
| 4 | UUID metadata verification and uncertain publication recovery | `c9c3518a` |
| 5 | Public output factories, part-size configuration, and usage documentation | `e690d206` |
| 6 | Real upload/read-back and fault integration coverage, writable test infrastructure, subsystem documentation, roadmap, and final review | Complete |

## Acceptance and verification

- Sequential output uses bounded buffering, small-object PUT, and ordered multipart uploads.
- Successful close confirms publication through the response or matching write UUID and final object length.
- Failed writes and caller abort discard only their owned upload. Cleanup failures and unknown publication outcomes remain explicit; completed objects are never deleted for rollback.
- Public factories preserve literal object keys and perform no network access until writing requires it.
- All 22 real s3proxy output integration cases passed, including both writer APIs, empty output, multipart and footer boundaries, replacement visibility, lost responses, failed metadata verification, abort retry, and independent uploads.
- Two observer tests verify pagination marker encoding, exact-key filtering, and repeated-marker rejection.
- Final `./mvnw clean verify` passed across all 13 modules on Java 25 under a 180-second timeout: 17,199 tests, zero failures or errors, 75 skipped.
- Strict MkDocs, core/S3 JavaDoc, Docker Compose configuration, and whitespace validation passed.
- A fresh whole-feature review found no actionable issues.

## Test endpoint limits

The writable filesystem fixture validates upload/read-back. Metadata-sensitive tests use a second s3proxy service with its transient backend because Docker Desktop filesystem mounts do not retain the metadata attributes needed by UUID verification.

The pinned emulator does not support explicit multipart-upload pagination parameters. The observer follows returned pagination markers; scripted HTTP tests cover pagination behavior. Its incomplete ListParts responses remain subject to strict production validation: cleanup can be reported as unconfirmed even when independent upload-list observations confirm removal.

These emulator limitations do not weaken the publication or cleanup contract. Remote outcomes that cannot be confirmed remain explicit failures.
