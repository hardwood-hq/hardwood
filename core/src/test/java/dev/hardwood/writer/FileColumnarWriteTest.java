/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.RowReader;
import dev.hardwood.row.PqFile;
import dev.hardwood.row.PqStruct;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// `FILE` groups written through the columnar writer, whose children are ordinary columns and
/// whose present values a submitted batch must hold valid.
class FileColumnarWriteTest {

    private static final boolean[] NONE_NULL = new boolean[3];
    private static final boolean[] ALL_NULL = {true, true, true};

    private static final FileSchema TOP_LEVEL = FileSchema.builder("schema")
            .file("photo", RepetitionType.OPTIONAL)
            .build();

    @Test
    void writesValidValuesAndSkipsNullGroups() throws IOException {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, TOP_LEVEL)) {
            // Row 1 is a null group, whose children hold a value no reference may; it is not read.
            writer.columnWriter().writeBatch(batch -> photo(batch,
                    new boolean[] {false, true, false},
                    strings(null, null, "s3://b/c"), new long[] {0, 5, 0}, new boolean[] {true, false, true},
                    new long[] {0, 0, 9}, new boolean[] {true, true, false},
                    new byte[][] {{1}, null, {1, 2}}, new boolean[] {false, true, false}));
        }

        assertThat(readPhotos(out)).containsExactly(
                PqFile.inline(new byte[] {1}), null,
                PqFile.external("s3://b/c", 9).withInline(new byte[] {1, 2}));
    }

    @Test
    void rejectsABatchHoldingAnInvalidValue() throws IOException {
        assertRejected(batch -> photo(batch, NONE_NULL,
                        strings("s3://b/a", null, "s3://b/c"), new long[] {0, 4, 0}, new boolean[] {true, false, true},
                        new long[] {0, 0, 0}, ALL_NULL, new byte[][] {null, {1}, null}, new boolean[] {true, false, true}),
                "FILE group photo value 1 is invalid: 'offset' is set without 'uri'");
        assertRejected(batch -> photo(batch, NONE_NULL,
                        strings("s3://b/a", "s3://b/b", "s3://b/c"), new long[3], ALL_NULL,
                        new long[] {0, 0, -3}, new boolean[] {true, true, false}, new byte[3][], ALL_NULL),
                "FILE group photo value 2 is invalid: 'size' is negative: -3");
    }

    @Test
    void rejectsAMalformedChecksum() throws IOException {
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), TOP_LEVEL)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch
                    .struct("photo", Validity.ofNulls(NONE_NULL))
                    .bytes("photo.uri", strings("s3://b/a", "s3://b/b", "s3://b/c"), NONE_NULL)
                    .longs("photo.offset", new long[3], ALL_NULL)
                    .longs("photo.size", new long[3], ALL_NULL)
                    .bytes("photo.content_type", new byte[3][], ALL_NULL)
                    .bytes("photo.checksum", strings("XXH3:x", null, "CRC32:3610A686"), new boolean[] {false, true, false})
                    .bytes("photo.inline", new byte[3][], ALL_NULL)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("FILE group photo value 2 is invalid: 'checksum' digest of CRC32 must be 8 lowercase hex "
                            + "characters: CRC32:3610A686");
        }
    }

    @Test
    void anEmptyUriIsNotSet() throws IOException {
        assertRejected(batch -> photo(batch, NONE_NULL,
                        strings("s3://b/a", "", "s3://b/c"), new long[3], ALL_NULL,
                        new long[3], ALL_NULL, new byte[3][], ALL_NULL),
                "FILE group photo value 1 is invalid: neither 'inline' nor 'uri' is set");
    }

    @Test
    void checksListElementsAndSkipsGroupsUnderANullStruct() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .list("attachments", RepetitionType.OPTIONAL, element -> element.file(RepetitionType.OPTIONAL))
                .struct("meta", RepetitionType.OPTIONAL, meta -> meta.file("thumbnail", RepetitionType.OPTIONAL))
                .build();
        Consumer<ColumnBatch> attachments = batch -> batch
                .list("attachments", new int[] {0, 2, 3, 3})
                .bytes("attachments.list.element.uri", strings("s3://b/a", "s3://b/b", "s3://b/c"), NONE_NULL)
                .longs("attachments.list.element.offset", new long[3], ALL_NULL)
                .longs("attachments.list.element.size", new long[3], ALL_NULL)
                .bytes("attachments.list.element.content_type", new byte[3][], ALL_NULL)
                .bytes("attachments.list.element.checksum", new byte[3][], ALL_NULL)
                .bytes("attachments.list.element.inline", new byte[3][], ALL_NULL);
        // `meta` is null in row 0, so its thumbnail's offset without a uri is never written.
        Consumer<ColumnBatch> meta = batch -> batch
                .struct("meta", Validity.ofNulls(new boolean[] {true, false, false}))
                .bytes("meta.thumbnail.uri", strings(null, "s3://b/t", "s3://b/u"), new boolean[] {true, false, false})
                .longs("meta.thumbnail.offset", new long[] {7, 0, 0}, new boolean[] {false, true, true})
                .longs("meta.thumbnail.size", new long[3], ALL_NULL)
                .bytes("meta.thumbnail.content_type", new byte[3][], ALL_NULL)
                .bytes("meta.thumbnail.checksum", new byte[3][], ALL_NULL)
                .bytes("meta.thumbnail.inline", new byte[3][], ALL_NULL);
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(batch -> {
                attachments.accept(batch);
                meta.accept(batch);
            });
        }
        List<Object> thumbnails = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                PqStruct struct = rows.getStruct("meta");
                thumbnails.add(struct == null ? null : struct.getFile("thumbnail"));
            }
        }
        assertThat(thumbnails).containsExactly(null, PqFile.external("s3://b/t"), PqFile.external("s3://b/u"));

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), schema)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> {
                batch.list("attachments", new int[] {0, 2, 3, 3})
                        .bytes("attachments.list.element.uri", strings("s3://b/a", "s3://b/b", null),
                                new boolean[] {false, false, true})
                        .longs("attachments.list.element.offset", new long[3], ALL_NULL)
                        .longs("attachments.list.element.size", new long[] {0, 0, 4},
                                new boolean[] {true, true, false})
                        .bytes("attachments.list.element.content_type", new byte[3][], ALL_NULL)
                        .bytes("attachments.list.element.checksum", new byte[3][], ALL_NULL)
                        .bytes("attachments.list.element.inline", new byte[][] {null, null, {1}},
                                new boolean[] {true, true, false});
                meta.accept(batch);
            }))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("FILE group attachments.list.element value 2 is invalid: 'size' is set without 'uri'");
        }
    }

    @Test
    void checksARequiredGroupOnlyWhereItsStructIsPresent() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .struct("meta", RepetitionType.OPTIONAL, meta -> meta.file("thumbnail", RepetitionType.REQUIRED))
                .build();
        // Value 0 sets an offset without a uri under a null struct, which is never written.
        Consumer<ColumnBatch> valid = batch -> thumbnail(batch, new boolean[] {true, false, false},
                strings(null, "s3://b/t", "s3://b/u"), new boolean[] {true, false, false});
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, schema)) {
            writer.columnWriter().writeBatch(valid);
        }
        List<Object> thumbnails = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                PqStruct meta = rows.getStruct("meta");
                thumbnails.add(meta == null ? null : meta.getFile("thumbnail"));
            }
        }
        assertThat(thumbnails).containsExactly(null, PqFile.external("s3://b/t"), PqFile.external("s3://b/u"));

        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), schema)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> thumbnail(batch,
                    new boolean[] {true, false, false}, strings(null, "s3://b/t", null),
                    new boolean[] {true, false, true})))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("FILE group meta.thumbnail value 2 is invalid: neither 'inline' nor 'uri' is set");
        }
    }

    @Test
    void checksMapValues() throws IOException {
        FileSchema schema = FileSchema.builder("schema")
                .map("renditions", RepetitionType.OPTIONAL, PhysicalType.BYTE_ARRAY, LogicalType.string(),
                        value -> value.file(RepetitionType.OPTIONAL))
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), schema)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(batch -> batch
                    .map("renditions", new int[] {0, 2, 3})
                    .bytes("renditions.key_value.key", strings("a", "b", "c"))
                    .bytes("renditions.key_value.value.uri", strings("s3://b/a", null, "s3://b/c"),
                            new boolean[] {false, true, false})
                    .longs("renditions.key_value.value.offset", new long[3], ALL_NULL)
                    .longs("renditions.key_value.value.size", new long[3], ALL_NULL)
                    .bytes("renditions.key_value.value.content_type", new byte[3][], ALL_NULL)
                    .bytes("renditions.key_value.value.checksum", new byte[3][], ALL_NULL)
                    .bytes("renditions.key_value.value.inline", new byte[3][], ALL_NULL)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("FILE group renditions.key_value.value value 1 is invalid: "
                            + "neither 'inline' nor 'uri' is set");
        }
    }

    /// The batch of `meta.thumbnail`, whose value 0 sets an offset without a uri.
    private static void thumbnail(ColumnBatch batch, boolean[] metaNulls, byte[][] uris, boolean[] uriNulls) {
        batch.struct("meta", Validity.ofNulls(metaNulls))
                .bytes("meta.thumbnail.uri", uris, uriNulls)
                .longs("meta.thumbnail.offset", new long[] {7, 0, 0}, new boolean[] {false, true, true})
                .longs("meta.thumbnail.size", new long[3], ALL_NULL)
                .bytes("meta.thumbnail.content_type", new byte[3][], ALL_NULL)
                .bytes("meta.thumbnail.checksum", new byte[3][], ALL_NULL)
                .bytes("meta.thumbnail.inline", new byte[3][], ALL_NULL);
    }

    private static void photo(ColumnBatch batch, boolean[] groupNulls, byte[][] uris, long[] offsets,
                              boolean[] offsetNulls, long[] sizes, boolean[] sizeNulls, byte[][] inline,
                              boolean[] inlineNulls) {
        batch.struct("photo", Validity.ofNulls(groupNulls))
                .bytes("photo.uri", uris, nullsOf(uris))
                .longs("photo.offset", offsets, offsetNulls)
                .longs("photo.size", sizes, sizeNulls)
                .bytes("photo.content_type", new byte[uris.length][], allNull(uris.length))
                .bytes("photo.checksum", new byte[uris.length][], allNull(uris.length))
                .bytes("photo.inline", inline, inlineNulls);
    }

    private static void assertRejected(Consumer<ColumnBatch> filler, String message) throws IOException {
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.inMemory(), TOP_LEVEL)) {
            assertThatThrownBy(() -> writer.columnWriter().writeBatch(filler))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(message);
        }
    }

    private static List<PqFile> readPhotos(InMemoryOutputFile out) throws IOException {
        List<PqFile> photos = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()));
             RowReader rows = reader.rowReader()) {
            while (rows.hasNext()) {
                rows.next();
                photos.add(rows.getFile("photo"));
            }
        }
        return photos;
    }

    /// UTF-8 encodings of `values`, a `null` entry standing for a null value.
    private static byte[][] strings(String... values) {
        byte[][] bytes = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = values[i] == null ? null : values[i].getBytes(StandardCharsets.UTF_8);
        }
        return bytes;
    }

    private static boolean[] allNull(int length) {
        boolean[] nulls = new boolean[length];
        Arrays.fill(nulls, true);
        return nulls;
    }

    private static boolean[] nullsOf(byte[][] values) {
        boolean[] nulls = new boolean[values.length];
        for (int i = 0; i < values.length; i++) {
            nulls[i] = values[i] == null;
        }
        return nulls;
    }
}
