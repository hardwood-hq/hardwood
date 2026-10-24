/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.step;

import java.io.IOException;
import java.nio.ByteBuffer;

import dev.hardwood.InputFile;

/// The bytes of a file a parse reads, as far as they have arrived. A parse asks for a range and
/// gets it, or `null` where the source does not hold it, which the parse then returns as the range
/// it needs ([Parsed]). Outside a read the source reads the file and holds every range ([#of]);
/// within one it holds what the read's [dev.hardwood.internal.reader.fetch.FetchSequence] entries
/// have delivered: the entries planned for a column's walk
/// ([dev.hardwood.internal.reader.fetch.EntryCursor]), or those the read's planning inserts as it
/// finds the ranges it needs ([dev.hardwood.internal.reader.plan.PlanningReads]). One parser serves
/// all three.
@FunctionalInterface
public interface ByteSource {

    /// The `length` bytes at `offset`, or `null` when the source does not hold them.
    ByteBuffer read(long offset, int length) throws IOException;

    /// A source that reads `inputFile`, waiting for each read, and so holds every range.
    static ByteSource of(InputFile inputFile) {
        return inputFile::readRange;
    }
}
