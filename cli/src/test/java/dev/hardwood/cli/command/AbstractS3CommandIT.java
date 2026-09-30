/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package dev.hardwood.cli.command;

import java.nio.file.Files;
import java.nio.file.Path;

import dev.hardwood.s3.S3Proxy;
import dev.hardwood.s3.TestBucket;

/// Singleton s3proxy bucket shared across all S3 command tests. The bucket
/// is created once when this class is loaded and is deleted by a shutdown hook
/// when the JVM exits.
///
/// Test parquet fixtures from `core/src/test/resources/` are written to the
/// bucket at startup, so no upload step is needed in tests.
abstract class AbstractS3CommandIT {

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    static final TestBucket bucket = S3Proxy.get().bucketFor(AbstractS3CommandIT.class)
            .withObject("convert_fidelity_test.parquet", TEST_RESOURCES.resolve("convert_fidelity_test.parquet"))
            .withObject("plain_uncompressed.parquet", TEST_RESOURCES.resolve("plain_uncompressed.parquet"))
            .withObject("dictionary_uncompressed.parquet", TEST_RESOURCES.resolve("dictionary_uncompressed.parquet"))
            .withObject("delta_byte_array_test.parquet", TEST_RESOURCES.resolve("delta_byte_array_test.parquet"))
            .withObject("deep_nested_struct_test.parquet", TEST_RESOURCES.resolve("deep_nested_struct_test.parquet"))
            .withObject("list_basic_test.parquet", TEST_RESOURCES.resolve("list_basic_test.parquet"))
            .withObject("unsigned_int_test.parquet", TEST_RESOURCES.resolve("unsigned_int_test.parquet"))
            .withObject("column_index_pushdown.parquet", TEST_RESOURCES.resolve("column_index_pushdown.parquet"))
            .withObject("cli_long_value_test.parquet", TEST_RESOURCES.resolve("cli_long_value_test.parquet"))
            .withObject("filter_pushdown_int.parquet", TEST_RESOURCES.resolve("filter_pushdown_int.parquet"))
            .withObject("cli_info_kv_metadata_test.parquet", TEST_RESOURCES.resolve("cli_info_kv_metadata_test.parquet"));

    protected static final String S3_FILE = bucket.uri("plain_uncompressed.parquet");
    protected static final String S3_DICT_FILE = bucket.uri("dictionary_uncompressed.parquet");
    protected static final String S3_BYTE_ARRAY_FILE = bucket.uri("delta_byte_array_test.parquet");
    protected static final String S3_DEEP_NESTED_FILE = bucket.uri("deep_nested_struct_test.parquet");
    protected static final String S3_LIST_FILE = bucket.uri("list_basic_test.parquet");
    protected static final String S3_NONEXISTENT_FILE = bucket.uri("nonexistent.parquet");
    protected static final String S3_UNSIGNED_INT_FILE = bucket.uri("unsigned_int_test.parquet");
    protected static final String S3_PAGE_INDEX_FILE = bucket.uri("column_index_pushdown.parquet");
    protected static final String S3_LONG_VALUE_FILE = bucket.uri("cli_long_value_test.parquet");
    protected static final String S3_MULTI_ROW_GROUP_INT_FILE = bucket.uri("filter_pushdown_int.parquet");
    protected static final String S3_KV_METADATA_FILE = bucket.uri("cli_info_kv_metadata_test.parquet");
    protected static final String S3_FIDELITY_FILE = bucket.uri("convert_fidelity_test.parquet");

    static {
        bucket.create();
        Runtime.getRuntime().addShutdownHook(new Thread(bucket::delete));

        try {
            // Redirect AWS profile files to an empty temp file so the SDK does not parse
            // the developer's ~/.aws/config (which may contain non-standard profiles that
            // trigger parse warnings and interfere with the test credential provider chain).
            String emptyFile = Files.createTempFile("hardwood-test-aws", "").toString();
            System.setProperty("aws.configFile", emptyFile);
            System.setProperty("aws.sharedCredentialsFile", emptyFile);

            System.setProperty("aws.accessKeyId", S3Proxy.ACCESS_KEY);
            System.setProperty("aws.secretAccessKey", S3Proxy.SECRET_KEY);
            System.setProperty("aws.region", "us-east-1");
            System.setProperty("aws.endpointUrl", bucket.endpoint());
            System.setProperty("aws.pathStyle", "true");
        }
        catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
