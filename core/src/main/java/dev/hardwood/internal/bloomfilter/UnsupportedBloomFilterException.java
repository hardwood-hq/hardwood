/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.bloomfilter;

/// Signals a bloom filter header naming an algorithm, hash or compression variant this
/// version does not implement.
///
/// An [UnsupportedOperationException], because the file is correct and it is this version
/// that cannot evaluate the filter. It has its own type so that
/// [dev.hardwood.internal.predicate.RowGroupBloomFilterSource] can decline to prune on
/// exactly this and let every other unsupported condition met while reading the filter
/// reach the caller.
public final class UnsupportedBloomFilterException extends UnsupportedOperationException {

    private static final long serialVersionUID = 1L;

    public UnsupportedBloomFilterException(String message) {
        super(message);
    }
}
