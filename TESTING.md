# Testing

This document collects manual testing recipes for scenarios that aren't covered by the automated test suite. Automated coverage is run via `./mvnw verify`; the recipes below are for ad-hoc verification against external services or environments.

## Manual S3 testing

The steps below run a local S3-compatible endpoint via [s3proxy](https://github.com/gaul/s3proxy) and exercise the CLI against it. Useful for verifying S3 behaviour without an AWS account, and for testing the native binary's S3 path (see also [NATIVE_BUILD.md](NATIVE_BUILD.md)).

The image is pulled from `ghcr.io/hardwood-hq/s3proxy`, a mirror of `andrewgaul/s3proxy` we maintain to avoid Docker Hub rate limits. The mirrored tag is kept in sync with `S3ProxyContainers.IMAGE` and the `S3PROXY_IMAGE` env var in the CI workflows under `.github/workflows/`.

1. Start s3proxy and set environment

```bash
docker run -d --name s3proxy -p 9090:80 \
    -e S3PROXY_AUTHORIZATION=none \
    -e S3PROXY_ENDPOINT=http://0.0.0.0:80 \
    -e JCLOUDS_PROVIDER=transient \
    ghcr.io/hardwood-hq/s3proxy:sha-6597ca59cd5c5fa8ee313e13d349d507cc6090c3

export AWS_ENDPOINT_URL=http://localhost:9090
export AWS_ACCESS_KEY_ID=foo
export AWS_SECRET_ACCESS_KEY=bar
export AWS_REGION=us-east-1
export AWS_PATH_STYLE=true
```

2. Create bucket and upload with curl

```bash
curl -X PUT http://localhost:9090/test-bucket

curl -T performance-testing/test-data-setup/target/tlc-trip-record-data/yellow_tripdata_2025-01.parquet \
    http://localhost:9090/test-bucket/yellow_tripdata_2025-01.parquet

curl -T performance-testing/test-data-setup/target/overture-maps-data/overture_places.zstd.parquet \
    http://localhost:9090/test-bucket/overture_places.zstd.parquet
```

3. Run hardwood CLI

```bash
cli/target/hardwood-cli-early-access-macos-aarch64/bin/hardwood info -f s3://test-bucket/yellow_tripdata_2025-01.parquet
```
