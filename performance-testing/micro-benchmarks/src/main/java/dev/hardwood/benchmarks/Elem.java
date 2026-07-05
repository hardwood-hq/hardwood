/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.benchmarks;

/// The primitive leaf type of a list fixture. The filename token keeps distinct
/// element types from colliding, and the type drives both the Avro schema and the
/// reader accessor the folds pick.
public enum Elem {
    INT64("int64"),
    FLOAT64("float64");

    private final String token;

    Elem(String token) {
        this.token = token;
    }

    public String token() {
        return token;
    }
}
