/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import dev.hardwood.internal.thrift.OffsetIndexReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.OffsetIndex;
import dev.hardwood.reader.ParquetReadException;

/// Holds an OffsetIndex shown as the file stores it to the checks a read applies, so a command or
/// a `dive` screen lists its pages and flags the contradiction a read would fail on.
public final class OffsetIndexCheck {

    private OffsetIndexCheck() {
    }

    /// The message a read of `offsetIndex` fails with, or `null` when a read accepts it.
    public static String violation(OffsetIndex offsetIndex, ColumnChunk chunk, long rowGroupRows) {
        try {
            OffsetIndexReader.check(offsetIndex, chunk, rowGroupRows);
            return null;
        }
        catch (ParquetReadException e) {
            return e.getMessage();
        }
    }
}
