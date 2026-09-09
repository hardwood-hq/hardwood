/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import dev.hardwood.InMemoryOutputFile;
import dev.hardwood.InputFile;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.BuildInfo;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.reader.ParquetFileReader;

import static dev.hardwood.writer.WriterTestSupport.oneColumn;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// The footer's two file-scope fields — `key_value_metadata` and `created_by` — are set on
/// [ParquetFileWriter] rather than in [WriterConfig], which governs a row group and the pages
/// within it. Both are serialized by [ParquetFileWriter#close()].
class WriterFooterMetadataTest {

    /// The `created_by` grammar Parquet readers apply, verbatim from parquet-java's
    /// `VersionParser.FORMAT`: application, optional version, optional build hash.
    private static final Pattern VERSION_PARSER_FORMAT =
            Pattern.compile("(.*?)\\s+version\\s*(?:([^(]*?)\\s*(?:\\(\\s*build\\s*([^)]*?)\\s*\\))?)?");

    @Test
    void writesNoMetadataByDefault() throws Exception {
        assertThat(write(writer -> {
        }).keyValueMetadata()).isEmpty();
    }

    @Test
    void writesTheEntriesItWasGiven() throws Exception {
        FileMetaData metaData = write(writer -> {
            writer.keyValueMetadata("ARROW:schema", "AAAA");
            writer.keyValueMetadata("owner", "analytics");
        });

        assertThat(metaData.keyValueMetadata())
                .containsEntry("ARROW:schema", "AAAA")
                .containsEntry("owner", "analytics");
    }

    /// The entries reach the file in the order they were given.
    @Test
    void preservesEntryOrder() throws Exception {
        FileMetaData metaData = write(writer -> {
            writer.keyValueMetadata("z", "1");
            writer.keyValueMetadata("a", "2");
            writer.keyValueMetadata("m", "3");
        });

        assertThat(metaData.keyValueMetadata().keySet()).containsExactly("z", "a", "m");
    }

    @Test
    void aRepeatedKeyReplacesItsValue() throws Exception {
        FileMetaData metaData = write(writer -> {
            writer.keyValueMetadata("key", "first");
            writer.keyValueMetadata("key", "second");
        });

        assertThat(metaData.keyValueMetadata()).containsExactlyEntriesOf(Map.of("key", "second"));
    }

    /// The map form adds to what is already held rather than replacing it.
    @Test
    void theMapFormAddsToWhatIsHeld() throws Exception {
        FileMetaData metaData = write(writer -> {
            writer.keyValueMetadata("kept", "1");
            writer.keyValueMetadata(Map.of("added", "2"));
        });

        assertThat(metaData.keyValueMetadata())
                .containsEntry("kept", "1")
                .containsEntry("added", "2");
    }

    /// `KeyValue.value` is optional in the format, so a null value writes a key carrying no
    /// value — which is what the reader reports it as, and so what it must be written back as.
    @Test
    void aNullValueWritesAKeyWithNoValue() throws Exception {
        FileMetaData metaData = write(writer -> writer.keyValueMetadata("marker", null));

        assertThat(metaData.keyValueMetadata()).containsEntry("marker", null);
    }

    /// The claim the field exists for: what a reader reports for one file can be handed
    /// straight back to a writer, and the next reader sees the same thing.
    @Test
    void metadataSurvivesAReadThenWrite() throws Exception {
        Map<String, String> original = new LinkedHashMap<>();
        original.put("ARROW:schema", "AAAA");
        original.put("org.apache.spark.sql.parquet.row.metadata", "{\"type\":\"struct\"}");
        original.put("valueless", null);

        FileMetaData first = write(writer -> writer.keyValueMetadata(original));
        FileMetaData second = write(writer -> writer.keyValueMetadata(first.keyValueMetadata()));

        assertThat(second.keyValueMetadata()).containsExactlyEntriesOf(original);
    }

    /// A value known only once the data is written can still be stated.
    @Test
    void metadataCanBeSetAfterWriting() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[]{ 1, 2, 3 }));
            writer.keyValueMetadata("row.count", "3");
        }

        assertThat(readFooter(out).keyValueMetadata()).containsEntry("row.count", "3");
    }

    @Test
    void rejectsMetadataAfterClose() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn());
        writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[]{ 1 }));
        writer.close();

        assertThatThrownBy(() -> writer.keyValueMetadata("late", "value"))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> writer.createdBy("late"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsANullKey() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[]{ 1 }));

            assertThatThrownBy(() -> writer.keyValueMetadata(null, "value"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> writer.keyValueMetadata((Map<String, String>) null))
                    .isInstanceOf(IllegalArgumentException.class);

            Map<String, String> withNullKey = new LinkedHashMap<>();
            withNullKey.put(null, "value");
            assertThatThrownBy(() -> writer.keyValueMetadata(withNullKey))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /// A map rejected for a null key leaves nothing behind: the entries that preceded the bad
    /// one in iteration order are not already in the footer.
    @Test
    void aRejectedMapAddsNothing() throws Exception {
        Map<String, String> withNullKey = new LinkedHashMap<>();
        withNullKey.put("good", "1");
        withNullKey.put(null, "2");

        FileMetaData metaData = write(writer -> assertThatThrownBy(() -> writer.keyValueMetadata(withNullKey))
                .isInstanceOf(IllegalArgumentException.class));

        assertThat(metaData.keyValueMetadata()).isEmpty();
    }

    @Test
    void writesTheDefaultCreatedBy() throws Exception {
        assertThat(write(writer -> {
        }).createdBy()).isEqualTo(ParquetFileWriter.DEFAULT_CREATED_BY);
    }

    @Test
    void writesTheCreatedByItWasGiven() throws Exception {
        assertThat(write(writer -> writer.createdBy("myapp version 1.2 (build abc)")).createdBy())
                .isEqualTo("myapp version 1.2 (build abc)");
    }

    @Test
    void rejectsANullCreatedBy() throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[]{ 1 }));

            assertThatThrownBy(() -> writer.createdBy(null))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /// The identifier must match the `<app> version <version> (build <hash>)` convention
    /// Parquet readers parse; a reader that cannot parse it treats the file as coming from an
    /// unidentifiable writer. This pins the shape offline — that parquet-java itself accepts
    /// the string is asserted by the write-path interop gate in `parquet-testing-runner`.
    @Test
    void defaultCreatedByFollowsTheParseableConvention() {
        assertThat(ParquetFileWriter.DEFAULT_CREATED_BY).matches(VERSION_PARSER_FORMAT);

        Matcher matcher = VERSION_PARSER_FORMAT.matcher(ParquetFileWriter.DEFAULT_CREATED_BY);
        matcher.matches();
        assertThat(matcher.group(1)).as("application").isEqualTo("hardwood");
        assertThat(matcher.group(2)).as("version").isEqualTo(BuildInfo.version());
        assertThat(matcher.group(3)).as("build hash").isEqualTo(BuildInfo.revisionWithDirtyMark());
    }

    /// Writes a three-row file, applying `footer` to the writer before it is closed, and
    /// returns the metadata a reader sees in the result.
    private static FileMetaData write(FooterSetter footer) throws Exception {
        InMemoryOutputFile out = OutputFile.inMemory();
        try (ParquetFileWriter writer = ParquetFileWriter.create(out, oneColumn())) {
            footer.apply(writer);
            writer.columnWriter().writeBatch(batch -> batch.ints(0, new int[]{ 1, 2, 3 }));
        }
        return readFooter(out);
    }

    private static FileMetaData readFooter(InMemoryOutputFile out) throws Exception {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(out.buffer()))) {
            return reader.getFileMetaData();
        }
    }

    @FunctionalInterface
    private interface FooterSetter {
        void apply(ParquetFileWriter writer);
    }
}
