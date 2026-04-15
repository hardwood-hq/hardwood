/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

/// S3 path constants shared by all S3-backed integration test classes.
abstract class AbstractS3CommandIT {

    protected static final String S3_FILE = "s3://test-bucket/plain_uncompressed.parquet";
    protected static final String S3_DICT_FILE = "s3://test-bucket/dictionary_uncompressed.parquet";
    protected static final String S3_BYTE_ARRAY_FILE = "s3://test-bucket/delta_byte_array_test.parquet";
    protected static final String S3_DEEP_NESTED_FILE = "s3://test-bucket/deep_nested_struct_test.parquet";
    protected static final String S3_LIST_FILE = "s3://test-bucket/list_basic_test.parquet";
    protected static final String S3_NONEXISTENT_FILE = "s3://test-bucket/nonexistent.parquet";
}
