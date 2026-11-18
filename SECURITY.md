# Security Policy

Please report vulnerabilities privately through [GitHub's private vulnerability reporting](https://github.com/hardwood-hq/hardwood/security/advisories/new), not as a public issue. Include:

- the affected version,
- where the problem is: which module (e.g. `hardwood-core`, `hardwood-s3`), or the CLI binary or Docker image,
- how to reproduce it, ideally with a Parquet file that triggers it,
- what an attacker gains.

Security fixes generally go into the next release from `main`. There is no commitment to fix earlier releases.

## Threat model

- Parquet files, their data and their metadata, may come from untrusted sources. Hardwood checks a size, count or length a file declares against the file's bytes before allocating anything from it, and fails a read whose decoding would need more than the maximum heap with `ParquetReadException` before allocating.
- Hardwood makes no guarantee on the memory a read of a given file needs. Each read is checked on its own, so a crafted file, or several reads running at once, can still exhaust the heap. Applications reading untrusted files are responsible for the limits around the library: a heap sized for the reads they run at once, a process or container of their own, and `-XX:+ExitOnOutOfMemoryError`, since after an `OutOfMemoryError` Hardwood makes no promise about its readers or their context. Improvements to how Hardwood bounds the memory of a read are welcome.
- The `hardwood.internal.*` system properties are set by the application. An application that lets untrusted parties set them takes on that risk itself.
