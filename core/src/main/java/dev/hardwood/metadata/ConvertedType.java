/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.metadata;

/**
 * Legacy converted types in Parquet schema (used by PyArrow for LIST/MAP annotation).
 * See parquet.thrift ConvertedType enum.
 */
public enum ConvertedType {
    UTF8(0),
    MAP(1),
    MAP_KEY_VALUE(2),
    LIST(3),
    ENUM(4),
    DECIMAL(5),
    DATE(6),
    TIME_MILLIS(7),
    TIME_MICROS(8),
    TIMESTAMP_MILLIS(9),
    TIMESTAMP_MICROS(10),
    UINT_8(11),
    UINT_16(12),
    UINT_32(13),
    UINT_64(14),
    INT_8(15),
    INT_16(16),
    INT_32(17),
    INT_64(18),
    JSON(19),
    BSON(20),
    INTERVAL(21);

    private final int thriftValue;

    ConvertedType(int thriftValue) {
        this.thriftValue = thriftValue;
    }

    public int getThriftValue() {
        return thriftValue;
    }

    public static ConvertedType fromThriftValue(int value) {
        for (ConvertedType type : values()) {
            if (type.thriftValue == value) {
                return type;
            }
        }
        return null; // Unknown converted types return null
    }
}
