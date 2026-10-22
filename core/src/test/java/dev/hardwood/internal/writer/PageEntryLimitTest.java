/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.internal.compression.CompressorFactory;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PageLocation;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnEncoding;
import dev.hardwood.writer.WriterConfig;

import static org.assertj.core.api.Assertions.assertThat;

/// A data page is cut at the entry limit whatever the byte and record targets allow, and at a
/// byte target clamped below the reader's page limit, so the writer never writes a page a reader
/// refuses for the values or the bytes it declares.
class PageEntryLimitTest {

    @Test
    void byteTargetIsClampedToHalfTheReadersPageLimit() {
        assertThat(ColumnChunkBuffer.cutTargetBytes(Integer.MAX_VALUE)).isEqualTo(128 * 1024 * 1024);
        assertThat(ColumnChunkBuffer.cutTargetBytes(128 * 1024 * 1024 + 1)).isEqualTo(128 * 1024 * 1024);
        assertThat(ColumnChunkBuffer.cutTargetBytes(WriterConfig.DEFAULT_PAGE_TARGET_BYTES))
                .isEqualTo(WriterConfig.DEFAULT_PAGE_TARGET_BYTES);
    }

    private static final int MAX_PAGE_ENTRIES = 1000;

    @Test
    void requiredColumnIsCutAtTheLimit() throws IOException {
        FileSchema schema = FileSchema.builder("schema").addColumn("v", PhysicalType.INT32, RepetitionType.REQUIRED)
                .build();
        RecordShredder shredder = new RecordShredder(schema);
        IntArrayColumnSource source = new IntArrayColumnSource(new int[2500]);
        shredder.bind(new ColumnSource[]{ source }, new Validity[1], Map.of(), Map.of(), Map.of());

        assertThat(firstRowIndexes(schema, shredder, source, 2500)).containsExactly(0L, 1000L, 2000L);
    }

    @Test
    void optionalColumnIsCutAtTheLimit() throws IOException {
        FileSchema schema = FileSchema.builder("schema").addColumn("v", PhysicalType.INT32, RepetitionType.OPTIONAL)
                .build();
        boolean[] nulls = new boolean[2500];
        for (int i = 0; i < nulls.length; i += 2) {
            nulls[i] = true;
        }
        RecordShredder shredder = new RecordShredder(schema);
        IntArrayColumnSource source = new IntArrayColumnSource(new int[2500]);
        shredder.bind(new ColumnSource[]{ source }, new Validity[]{ Validity.ofNulls(nulls) }, Map.of(), Map.of(),
                Map.of());

        assertThat(firstRowIndexes(schema, shredder, source, 2500)).containsExactly(0L, 1000L, 2000L);
    }

    @Test
    void repeatedColumnIsCutAtTheLastRecordWithinTheLimit() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        // Ten records of 300 elements: three fit in a page of 1000 entries, the fourth would not
        int[] offsets = new int[11];
        for (int r = 0; r < 10; r++) {
            offsets[r + 1] = offsets[r] + 300;
        }
        RecordShredder shredder = new RecordShredder(schema);
        IntArrayColumnSource source = new IntArrayColumnSource(new int[3000]);
        shredder.bind(new ColumnSource[]{ source }, new Validity[1], Map.of(), Map.of(), Map.of("v", offsets));

        assertThat(firstRowIndexes(schema, shredder, source, 10)).containsExactly(0L, 3L, 6L, 9L);
    }

    @Test
    void recordAboveTheLimitIsAPageOfItsOwn() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .list("v", RepetitionType.REQUIRED, el -> el.primitive(PhysicalType.INT32, RepetitionType.REQUIRED))
                .build();
        int[] offsets = { 0, 10, 1510, 1520 };
        RecordShredder shredder = new RecordShredder(schema);
        IntArrayColumnSource source = new IntArrayColumnSource(new int[1520]);
        shredder.bind(new ColumnSource[]{ source }, new Validity[1], Map.of(), Map.of(), Map.of("v", offsets));

        assertThat(firstRowIndexes(schema, shredder, source, 3)).containsExactly(0L, 1L, 2L);
    }

    /// The first row of each page the chunk is cut into, with byte and record targets no page
    /// here reaches.
    private static List<Long> firstRowIndexes(FileSchema schema, RecordShredder shredder, ColumnSource source,
                                              int records) throws IOException {
        ColumnChunkBuffer buffer = new ColumnChunkBuffer(schema.getColumn(0), 1 << 30, Integer.MAX_VALUE,
                MAX_PAGE_ENTRIES, 1 << 20, RowGroupBuffer.MAX_ROWS, ColumnEncoding.AUTO, 64,
                new CompressorFactory().getCompressor(CompressionCodec.UNCOMPRESSED), CompressionCodec.UNCOMPRESSED,
                RowGroupBuffer.MAX_STORE_CAPACITY, ColumnChunkBuffer.NO_BLOOM_FILTER);
        buffer.append(shredder, source, 0, 0, records);
        InMemoryOutputFile out = OutputFile.inMemory();
        out.create();
        ColumnChunkBuffer.Flushed flushed = buffer.encodeTo(ChunkSink.to(out), schema.getColumn(0), null);
        return flushed.offsetIndex().pageLocations().stream().map(PageLocation::firstRowIndex).toList();
    }
}
