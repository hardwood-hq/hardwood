/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.metadata;

/**
 * Physical types supported by Parquet format.
 * These represent how data is stored on disk.
 */
public enum PhysicalType {
    BOOLEAN(0),
    INT32(1),
    INT64(2),
    INT96(3), // Deprecated, used for legacy timestamp
    FLOAT(4),
    DOUBLE(5),
    BYTE_ARRAY(6),
    FIXED_LEN_BYTE_ARRAY(7);

    private final int thriftValue;

    PhysicalType(int thriftValue) {
        this.thriftValue = thriftValue;
    }

    public int getThriftValue() {
        return thriftValue;
    }

    public static PhysicalType fromThriftValue(int value) {
        for (PhysicalType type : values()) {
            if (type.thriftValue == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown physical type: " + value);
    }
}
