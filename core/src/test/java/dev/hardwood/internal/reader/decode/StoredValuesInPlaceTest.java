/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.TestDecodeBuffers;
import dev.hardwood.internal.encoding.PlainEncoder;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// `PLAIN` fixed-width values whose value section is stored uncompressed are read where they lie in
/// the fetched page, never from a copy: a fill reads what the fetched buffer holds when it runs,
/// which the tests show by changing the buffer between the decode and the fill.
class StoredValuesInPlaceTest {

    private static final float[] VALUES = { 1.5f, 2.5f, 3.5f, 4.5f };

    @Test
    void readsUncompressedV1ValuesOfAFlatColumnFromTheFetchedPage() {
        ColumnSchema column = DataPages.optional(PhysicalType.FLOAT, null);
        ByteBuffer page = direct(DataPages.page(Encoding.PLAIN, VALUES.length, DataPages.concat(
                DataPages.levelSection(1, 1, 1, 1, 1), PlainEncoder.encodeFloats(VALUES, 0, VALUES.length))));
        FlatPageValues values = DataPages.decoder(column, CompressionCodec.UNCOMPRESSED)
                .decodeFlat(page, null, new PageDecoder.LevelScratch(), TestDecodeBuffers.create());

        overwriteLastValue(page, 9.5f);

        assertThat((float[]) DataPages.values(values, PhysicalType.FLOAT)).containsExactly(1.5f, 2.5f, 3.5f, 9.5f);
    }

    @Test
    void readsUncompressedV1ValuesOfANestedColumnFromTheFetchedPage() {
        ColumnSchema column = DataPages.repeated(PhysicalType.FLOAT, null);
        int present = column.maxDefinitionLevel();
        // One list of four elements
        ByteBuffer page = direct(DataPages.page(Encoding.PLAIN, VALUES.length, DataPages.concat(
                DataPages.levelSection(column.maxRepetitionLevel(), 0, 1, 1, 1),
                DataPages.levelSection(present, present, present, present, present),
                PlainEncoder.encodeFloats(VALUES, 0, VALUES.length))));
        PageDecoder.InPlaceNestedPage nested = DataPages.decoder(column, CompressionCodec.UNCOMPRESSED)
                .decodeNestedInPlace(page, null, new PageDecoder.LevelScratch(), TestDecodeBuffers.create());

        overwriteLastValue(page, 9.5f);

        assertThat((float[]) DataPages.values(nested, column)).containsExactly(1.5f, 2.5f, 3.5f, 9.5f);
    }

    @Test
    void readsUncompressedV1ValuesOfAFixedSizeListPageFromTheFetchedPage() {
        FixedSizeList list = new FixedSizeList();
        ByteBuffer page = direct(DataPages.page(Encoding.PLAIN, list.values.length, DataPages.concat(
                DataPages.levelSection(1, list.repetition), DataPages.levelSection(2, list.definition),
                PlainEncoder.encodeFloats(list.values, 0, list.values.length))));
        PageDecoder.InPlaceNestedPage nested = DataPages.decoder(list.column, CompressionCodec.UNCOMPRESSED)
                .decodeNestedInPlace(page, null, new PageDecoder.LevelScratch(), TestDecodeBuffers.create());
        assertThat(nested.fixedListK()).isEqualTo(2);

        overwriteLastValue(page, 99.5f);

        list.values[list.values.length - 1] = 99.5f;
        assertThat((float[]) DataPages.values(nested, list.column)).containsExactly(list.values);
    }

    @Test
    void readsStoredV2ValuesOfANestedColumnFromTheFetchedPage() {
        ColumnSchema column = DataPages.repeated(PhysicalType.FLOAT, null);
        int present = column.maxDefinitionLevel();
        // One list of four elements
        ByteBuffer page = direct(DataPages.pageV2(Encoding.PLAIN, VALUES.length, 1,
                DataPages.levels(column.maxRepetitionLevel(), 0, 1, 1, 1),
                DataPages.levels(present, present, present, present, present),
                PlainEncoder.encodeFloats(VALUES, 0, VALUES.length), CompressionCodec.SNAPPY, false));
        PageDecoder.InPlaceNestedPage nested = DataPages.decoder(column, CompressionCodec.SNAPPY)
                .decodeNestedInPlace(page, null, new PageDecoder.LevelScratch(), TestDecodeBuffers.create());

        overwriteLastValue(page, 9.5f);

        assertThat((float[]) DataPages.values(nested, column)).containsExactly(1.5f, 2.5f, 3.5f, 9.5f);
    }

    @Test
    void readsV2ValuesFlaggedCompressedInAnUncompressedChunkFromTheFetchedPage() {
        ColumnSchema column = DataPages.optional(PhysicalType.FLOAT, null);
        ByteBuffer page = direct(DataPages.pageV2(Encoding.PLAIN, VALUES.length, VALUES.length, new byte[0],
                DataPages.levels(1, 1, 1, 1, 1), PlainEncoder.encodeFloats(VALUES, 0, VALUES.length),
                CompressionCodec.UNCOMPRESSED, true));
        FlatPageValues values = DataPages.decoder(column, CompressionCodec.UNCOMPRESSED)
                .decodeFlat(page, null, new PageDecoder.LevelScratch(), TestDecodeBuffers.create());

        overwriteLastValue(page, 9.5f);

        assertThat((float[]) DataPages.values(values, PhysicalType.FLOAT)).containsExactly(1.5f, 2.5f, 3.5f, 9.5f);
    }

    @Test
    void refusesV2ValuesFlaggedCompressedInAnUncompressedChunkThatStoreAnotherSize() {
        ColumnSchema column = DataPages.required(PhysicalType.FLOAT, null);
        // Snappy-compressed values, which a chunk without a codec stores as they are
        ByteBuffer page = DataPages.pageV2(Encoding.PLAIN, VALUES.length, VALUES.length, new byte[0], new byte[0],
                PlainEncoder.encodeFloats(VALUES, 0, VALUES.length), CompressionCodec.SNAPPY, true);
        PageDecoder decoder = DataPages.decoder(column, CompressionCodec.UNCOMPRESSED);

        assertThatThrownBy(() -> decoder.decodeFlat(page, null, new PageDecoder.LevelScratch(), TestDecodeBuffers.create()))
                .isInstanceOf(ParquetReadException.class)
                .hasMessage("Uncompressed page size mismatch: expected 16, got 18");
    }

    @Test
    void readsStoredV2ValuesOfAFixedSizeListPageFromTheFetchedPage() {
        FixedSizeList list = new FixedSizeList();
        ByteBuffer page = direct(DataPages.pageV2(Encoding.PLAIN, list.values.length, FixedSizeList.LISTS,
                DataPages.levels(1, list.repetition), DataPages.levels(2, list.definition),
                PlainEncoder.encodeFloats(list.values, 0, list.values.length), CompressionCodec.SNAPPY, false));
        PageDecoder.InPlaceNestedPage nested = DataPages.decoder(list.column, CompressionCodec.SNAPPY)
                .decodeNestedInPlace(page, null, new PageDecoder.LevelScratch(), TestDecodeBuffers.create());
        assertThat(nested.fixedListK()).isEqualTo(2);

        overwriteLastValue(page, 99.5f);

        list.values[list.values.length - 1] = 99.5f;
        assertThat((float[]) DataPages.values(nested, list.column)).containsExactly(list.values);
    }

    /// Sixteen lists of two elements in an optional list of required elements, the shape the
    /// fixed-size-list fast path takes, with enough values for the definition levels to be a single
    /// RLE run.
    private static final class FixedSizeList {

        static final int LISTS = 16;

        final ColumnSchema column = new ColumnSchema(FieldPath.of("c", "list", "element"), PhysicalType.FLOAT,
                RepetitionType.REQUIRED, null, 0, 2, 1, null);
        final float[] values = new float[LISTS * 2];
        final int[] repetition = new int[values.length];
        final int[] definition = new int[values.length];

        FixedSizeList() {
            for (int i = 0; i < values.length; i++) {
                values[i] = i + 0.5f;
                repetition[i] = i % 2;
                definition[i] = 2;
            }
        }
    }

    @Test
    void copiesUncompressedV1ValuesFromAReadOnlyHeapBuffer() {
        ColumnSchema column = DataPages.optional(PhysicalType.FLOAT, null);
        ByteBuffer heap = DataPages.page(Encoding.PLAIN, VALUES.length, DataPages.concat(
                DataPages.levelSection(1, 1, 1, 1, 1), PlainEncoder.encodeFloats(VALUES, 0, VALUES.length)));
        // A read-only heap buffer exposes no array, and is neither read through one nor in place
        FlatPageValues values = DataPages.decoder(column, CompressionCodec.UNCOMPRESSED)
                .decodeFlat(heap.asReadOnlyBuffer(), null, new PageDecoder.LevelScratch(), TestDecodeBuffers.create());

        overwriteLastValue(heap, 9.5f);

        assertThat((float[]) DataPages.values(values, PhysicalType.FLOAT)).containsExactly(VALUES);
    }

    /// The page in a direct buffer, as a slice of a file's mapping holds it.
    private static ByteBuffer direct(ByteBuffer heap) {
        return ByteBuffer.allocateDirect(heap.remaining()).put(heap).flip();
    }

    /// Writes `value` over the page's last `FLOAT`, the last four bytes of its body.
    private static void overwriteLastValue(ByteBuffer page, float value) {
        page.order(ByteOrder.LITTLE_ENDIAN).putFloat(page.limit() - Float.BYTES, value);
    }
}
