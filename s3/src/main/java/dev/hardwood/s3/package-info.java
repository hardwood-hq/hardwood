/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/// Amazon S3 backend for reading and writing Parquet files in object storage.
///
/// [S3Source] is the main entry point, providing [dev.hardwood.InputFile]
/// instances backed by S3 range requests and sequential [dev.hardwood.OutputFile]
/// instances for [dev.hardwood.writer.ParquetFileWriter]. Output buffers are bounded
/// by [S3Source.Builder#uploadPartSize(int)], and close publishes the finished object.
/// An uncertain publication response is verified by write UUID and object length;
/// discard cleans up known uploads without deleting completed objects.
/// Uses a lightweight HTTP client
/// with no AWS SDK dependency; for AWS SDK credential integration, see
/// the `hardwood-aws-auth` module.
package dev.hardwood.s3;
