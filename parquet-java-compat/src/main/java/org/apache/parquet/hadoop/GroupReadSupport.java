/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.apache.parquet.hadoop;

/**
 * Read support for Group-based record reading.
 * <p>
 * This is a marker class for API compatibility with parquet-java.
 * In Hardwood, it signals that the reader should return Group objects.
 * </p>
 *
 * <pre>{@code
 * ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path).build();
 * }</pre>
 */
public class GroupReadSupport {

    /**
     * Create a GroupReadSupport.
     */
    public GroupReadSupport() {
    }
}
