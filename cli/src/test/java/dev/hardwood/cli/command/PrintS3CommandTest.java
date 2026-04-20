/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import io.quarkus.test.junit.main.QuarkusMainTest;

@QuarkusMainTest
class PrintS3CommandTest extends AbstractS3CommandTest implements PrintCommandContract {

    @Override
    public String plainFile() {
        return S3_FILE;
    }

    @Override
    public String byteArrayFile() {
        return S3_BYTE_ARRAY_FILE;
    }

    @Override
    public String deepNestedFile() {
        return S3_DEEP_NESTED_FILE;
    }

    @Override
    public String listFile() {
        return S3_LIST_FILE;
    }

    @Override
    public String nonexistentFile() {
        return S3_NONEXISTENT_FILE;
    }

    @Override
    public String unsignedIntFile() {
        return S3_UNSIGNED_INT_FILE;
    }

    @Override
    public String multiRowGroupIntFile() {
        return S3_MULTI_ROW_GROUP_INT_FILE;
    }
}
