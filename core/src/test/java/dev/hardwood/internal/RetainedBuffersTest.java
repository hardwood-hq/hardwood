/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import dev.hardwood.HardwoodContext;
import dev.hardwood.InputFile;
import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.internal.reader.pipeline.FlatColumn;
import dev.hardwood.internal.thrift.RleExpansionFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;

import static org.assertj.core.api.Assertions.assertThat;

/// The buffers a decode keeps for later pages stay within [DecodeBuffers#RETAINED_BYTES]; a
/// larger page's are its own.
class RetainedBuffersTest {

    @Test
    void transientBytesUpToTheRetainedSizeAreReused() {
        DecodeBuffers buffers = TestDecodeBuffers.create();

        byte[] first = PageDecoder.transientBytes(buffers, DecodeBuffers.RETAINED_BYTES);

        assertThat(PageDecoder.transientBytes(buffers, 1)).isSameAs(first);
    }

    @Test
    void transientBytesAboveTheRetainedSizeAreNotKept() {
        DecodeBuffers buffers = TestDecodeBuffers.create();
        byte[] small = PageDecoder.transientBytes(buffers, 1);

        byte[] large = PageDecoder.transientBytes(buffers, DecodeBuffers.RETAINED_BYTES + 1);

        assertThat(large).hasSize(DecodeBuffers.RETAINED_BYTES + 1);
        assertThat(PageDecoder.transientBytes(buffers, 1)).isSameAs(small);
    }

    @Test
    void trimDropsOnlyScratchAboveTheRetainedSize() {
        PageDecoder.LevelScratch scratch = new PageDecoder.LevelScratch();
        int[] ids = scratch.ids(DecodeBuffers.RETAINED_BYTES / Integer.BYTES + 1);
        int[] levels = scratch.definitionLevels(DecodeBuffers.RETAINED_BYTES / Integer.BYTES);

        scratch.trim();

        assertThat(scratch.ids(1)).isNotSameAs(ids).hasSize(1);
        assertThat(scratch.definitionLevels(1)).isSameAs(levels);
    }

    @Test
    void readOfAPageAboveTheRetainedSizeLeavesTheContextNoneOfItsBuffers() throws IOException {
        // One page of 5 Mi dictionary ids: 20 MiB of ids, decoded on the context's one thread
        int pageValues = 5 * 1024 * 1024;
        byte[] file = RleExpansionFile.buildWide(1, 1, Long.BYTES, pageValues);

        try (HardwoodContext context = HardwoodContext.create(1)) {
            try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(ByteBuffer.wrap(file)), context);
                    RowReader rows = reader.buildRowReader().build()) {
                long count = 0;
                while (rows.hasNext()) {
                    rows.next();
                    count++;
                }
                assertThat(count).isEqualTo(pageValues);
            }

            // The pool's one set, which the read's page task decoded with
            DecodeBuffers buffers = ((HardwoodContextImpl) context).decode(set -> set);
            assertThat(PageDecoder.transientBytes(buffers, 1).length).isLessThanOrEqualTo(DecodeBuffers.RETAINED_BYTES);
            assertThat((long) FlatColumn.scratch(buffers).ids(1).length * Integer.BYTES)
                    .isLessThanOrEqualTo(DecodeBuffers.RETAINED_BYTES);
        }
    }
}
