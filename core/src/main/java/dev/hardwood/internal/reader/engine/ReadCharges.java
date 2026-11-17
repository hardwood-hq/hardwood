/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.reader.memory.DecodeCharges;
import dev.hardwood.metadata.Encoding;

/// What a read's decodes charge as they decode. What a page's header gives, the read charged before
/// the decode started, from the page's place in its column's frontier ([JobScheduler]), so it
/// charges nothing here. The values a decode reconstructs beyond its page's bytes are drawn on the
/// decode's [JobScheduler.Draw], which the scheduler hands the decode in its buffers.
final class ReadCharges implements DecodeCharges {

    static final ReadCharges INSTANCE = new ReadCharges();

    /// The draw of the decode the buffers' thread runs, set for the decode's duration.
    static final DecodeBuffers.Slot<JobScheduler.Draw> DRAW = DecodeBuffers.slot();

    private ReadCharges() {
    }

    @Override
    public void dictionary(int numValues, int uncompressedSize) {
    }

    @Override
    public void page(int numValues, int uncompressedSize, long decodedBytes, Encoding encoding, boolean kept) {
    }

    /// Draws the reconstructed values twice: the task's buffer of them, and the chunks the fill
    /// copies them into.
    @Override
    public void reconstructed(int numValues, int uncompressedSize, long decodedBytes, Encoding encoding,
                              boolean kept, long valueBytes, DecodeBuffers buffers) {
        JobScheduler.Draw draw = buffers == null ? null : buffers.get(DRAW);
        if (draw != null) {
            draw.draw(2 * valueBytes);
        }
    }
}
