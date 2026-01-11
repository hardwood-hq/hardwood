/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.metadata;

/**
 * Field repetition types in Parquet schema.
 */
public enum RepetitionType {
    REQUIRED(0), // Field must be present
    OPTIONAL(1), // Field may be null
    REPEATED(2); // Field can appear multiple times (list)

    private final int thriftValue;

    RepetitionType(int thriftValue) {
        this.thriftValue = thriftValue;
    }

    public int getThriftValue() {
        return thriftValue;
    }

    public static RepetitionType fromThriftValue(int value) {
        for (RepetitionType type : values()) {
            if (type.thriftValue == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown repetition type: " + value);
    }
}
