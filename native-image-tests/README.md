# Native Image Tests

Scripts for building and testing the Hardwood native binary against various Parquet compression codecs.

## Usage

```bash
# Linux binary (via Quarkus container-build), tested in Docker:
./native-image-tests/test.sh

# macOS binary, tested directly (no Docker):
./native-image-tests/test.sh --local

# Skip the Maven build in either mode:
./native-image-tests/test.sh --skip-build
./native-image-tests/test.sh --local --skip-build

# Extra parquet files (mounted at /testdata in Docker, or read directly in local mode):
TESTDATA_DIR=/path/to/parquets ./native-image-tests/test.sh
```

## Files

- `test.sh` — Main entry point. Builds the binary and orchestrates the test run.
- `run-tests.sh` — Platform-agnostic test runner (used by both modes).
- `Dockerfile.test` — Test image. Extracts the dist archive inside the container (Linux, case-sensitive FS) to preserve `lib/Linux/` vs `lib/linux/` directory casing required by snappy-java.
