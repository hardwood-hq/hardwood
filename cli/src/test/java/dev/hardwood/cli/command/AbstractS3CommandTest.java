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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import dev.hardwood.s3.S3ProxyContainers;

/// Singleton s3proxy container shared across all S3 command tests. The
/// container starts once when this class is loaded and is stopped automatically
/// by Testcontainers' shutdown hook when the JVM exits.
///
/// Test parquet fixtures from `core/src/test/resources/` are copied into the
/// container at startup as objects in bucket "test-bucket", so no upload step
/// is needed in tests.
abstract class AbstractS3CommandTest {
    protected static final String S3_FILE = "s3://test-bucket/plain_uncompressed.parquet";
    protected static final String S3_DICT_FILE = "s3://test-bucket/dictionary_uncompressed.parquet";
    protected static final String S3_BYTE_ARRAY_FILE = "s3://test-bucket/delta_byte_array_test.parquet";
    protected static final String S3_DEEP_NESTED_FILE = "s3://test-bucket/deep_nested_struct_test.parquet";
    protected static final String S3_LIST_FILE = "s3://test-bucket/list_basic_test.parquet";
    protected static final String S3_NONEXISTENT_FILE = "s3://test-bucket/nonexistent.parquet";
    protected static final String S3_UNSIGNED_INT_FILE = "s3://test-bucket/unsigned_int_test.parquet";
    protected static final String S3_PAGE_INDEX_FILE = "s3://test-bucket/column_index_pushdown.parquet";
    protected static final String S3_MULTI_ROW_GROUP_INT_FILE = "s3://test-bucket/filter_pushdown_int.parquet";

    private static final Path TEST_RESOURCES = Path.of("").toAbsolutePath()
            .resolve("../core/src/test/resources").normalize();

    static final GenericContainer<?> s3 = S3ProxyContainers.filesystemBacked()
            .withCopyFileToContainer(fixture("plain_uncompressed.parquet"),
                    S3ProxyContainers.objectPath("plain_uncompressed.parquet"))
            .withCopyFileToContainer(fixture("dictionary_uncompressed.parquet"),
                    S3ProxyContainers.objectPath("dictionary_uncompressed.parquet"))
            .withCopyFileToContainer(fixture("delta_byte_array_test.parquet"),
                    S3ProxyContainers.objectPath("delta_byte_array_test.parquet"))
            .withCopyFileToContainer(fixture("deep_nested_struct_test.parquet"),
                    S3ProxyContainers.objectPath("deep_nested_struct_test.parquet"))
            .withCopyFileToContainer(fixture("list_basic_test.parquet"),
                    S3ProxyContainers.objectPath("list_basic_test.parquet"))
            .withCopyFileToContainer(fixture("unsigned_int_test.parquet"),
                    S3ProxyContainers.objectPath("unsigned_int_test.parquet"))
            .withCopyFileToContainer(fixture("column_index_pushdown.parquet"),
                    S3ProxyContainers.objectPath("column_index_pushdown.parquet"))
            .withCopyFileToContainer(fixture("filter_pushdown_int.parquet"),
                    S3ProxyContainers.objectPath("filter_pushdown_int.parquet"));

    private static MountableFile fixture(String name) {
        return MountableFile.forHostPath(TEST_RESOURCES.resolve(name));
    }

    static {
        s3.start();

        try {
            // Redirect AWS profile files to an empty temp file so the SDK does not parse
            // the developer's ~/.aws/config (which may contain non-standard profiles that
            // trigger parse warnings and interfere with the test credential provider chain).
            String emptyFile = Files.createTempFile("hardwood-test-aws", "").toString();
            System.setProperty("aws.configFile", emptyFile);
            System.setProperty("aws.sharedCredentialsFile", emptyFile);

            System.setProperty("aws.accessKeyId", S3ProxyContainers.ACCESS_KEY);
            System.setProperty("aws.secretAccessKey", S3ProxyContainers.SECRET_KEY);
            System.setProperty("aws.region", "us-east-1");
            System.setProperty("aws.endpointUrl", S3ProxyContainers.endpoint(s3));
            System.setProperty("aws.pathStyle", "true");
        }
        catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }
}
