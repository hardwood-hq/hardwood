/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.command;

import io.quarkus.test.junit.main.QuarkusMainTest;

@QuarkusMainTest
class ConvertS3CommandTest extends AbstractS3CommandTest implements ConvertCommandContract {

    @Override
    public String plainFile() {
        return S3_FILE;
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
}
