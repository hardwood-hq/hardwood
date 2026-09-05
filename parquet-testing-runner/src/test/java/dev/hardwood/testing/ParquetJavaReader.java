/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.testing;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.CorruptDeltaByteArrays;
import org.apache.parquet.CorruptStatistics;
import org.apache.parquet.VersionParser;
import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.VersionParser.VersionParseException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.PhysicalType;

import static org.assertj.core.api.Assertions.assertThat;

/// Reads a Parquet file with parquet-java, the strict reader of the write-path interop gate
/// described in `_designs/WRITER_INTEROP_GATE.md`.
///
/// Rows come back as [Group]s rather than Avro records: the Group model materializes any valid
/// file with no object model in the way, so annotations Avro cannot represent — `UUID`,
/// `FLOAT16`, `INTERVAL`, the unsigned integer widths — are still readable, and a read failure
/// means the bytes are bad rather than that Avro could not model the type. The Avro path used by
/// the read-direction comparison lives in [Utils#readWithParquetJava].
final class ParquetJavaReader {

    private ParquetJavaReader() {
    }

    /// Every row of the file, materialized through parquet-java's Group record model.
    ///
    /// @param file the file to read
    /// @return the rows, in file order
    /// @throws IOException if parquet-java cannot read the file — which, for a file Hardwood
    ///         wrote, is the gate failing
    static List<Group> readGroups(Path file) throws IOException {
        observe(file);
        List<Group> rows = new ArrayList<>();
        try (ParquetReader<Group> reader = ParquetReader
                .builder(new GroupReadSupport(), hadoopPath(file))
                .withConf(new Configuration())
                .build()) {

            Group row;
            while ((row = reader.read()) != null) {
                rows.add(row);
            }
        }
        return rows;
    }

    /// The file's footer as parquet-java parses it: the schema, the row groups, and each column
    /// chunk's encodings and statistics.
    ///
    /// @param file the file to read
    /// @return the parsed footer
    /// @throws IOException if parquet-java cannot parse the footer
    static ParquetMetadata readFooter(Path file) throws IOException {
        observe(file);
        try (ParquetFileReader reader = ParquetFileReader
                .open(HadoopInputFile.fromPath(hadoopPath(file), new Configuration()))) {
            return reader.getFooter();
        }
    }

    /// The file's footer as the format's own Thrift structure, parsed by parquet-java. Some
    /// fields are folded into higher-level defaults by [#readFooter], so tests that need to
    /// assert the field is really present on the wire use this path.
    ///
    /// @param file the file to read
    /// @return the parsed format footer
    /// @throws IOException if parquet-java cannot parse the footer
    static FileMetaData readFormatFooter(Path file) throws IOException {
        observe(file);
        try (SeekableInputStream in = HadoopInputFile
                .fromPath(hadoopPath(file), new Configuration()).newStream()) {
            return readThriftFooter(file, in);
        }
    }

    /// Asserts that parquet-java can identify the writer that produced the file.
    ///
    /// A `created_by` its [VersionParser] cannot parse leaves parquet-java unable to tell which
    /// implementation wrote the file, so it assumes the worst and applies its writer-specific
    /// correctness workarounds: under the PARQUET-251 heuristic it discards the deprecated
    /// `min` / `max` of a `BINARY` or `FIXED_LEN_BYTE_ARRAY` column outright. Both halves are
    /// asserted — that the identifier parses, and that the heuristic consequently spares the
    /// statistics of the two types it gates.
    ///
    /// @param footer the footer parquet-java parsed
    static void assertParseableCreatedBy(ParquetMetadata footer) {
        String createdBy = footer.getFileMetaData().getCreatedBy();

        ParsedVersion parsed;
        try {
            parsed = VersionParser.parse(createdBy);
        }
        catch (VersionParseException e) {
            throw new AssertionError("parquet-java cannot parse created_by: " + createdBy, e);
        }
        assertThat(parsed.application).as("created_by application").isEqualTo("hardwood");
        assertThat(parsed.version).as("created_by version").isNotNull();
        assertThat(parsed.hasSemanticVersion()).as("created_by version is a semantic version").isTrue();

        for (PrimitiveTypeName gated : List.of(PrimitiveTypeName.BINARY, PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)) {
            assertThat(CorruptStatistics.shouldIgnoreStatistics(createdBy, gated))
                    .as("PARQUET-251 discards %s statistics written by '%s'", gated, createdBy)
                    .isFalse();
        }

        // PARQUET-251's counterpart, reached by an encoding rather than by statistics: a
        // DELTA_BYTE_ARRAY chunk is forced onto a slower sequential read when the writer is one
        // parquet-java knows the defect of, or one it cannot identify at all. A parseable
        // created_by is what keeps hardwood's own files off that path, and the assertion is here
        // rather than beside the encoding because it is a property of the footer, not the chunk.
        assertThat(CorruptDeltaByteArrays.requiresSequentialReads(parsed, Encoding.DELTA_BYTE_ARRAY))
                .as("PARQUET-246 forces sequential DELTA_BYTE_ARRAY reads for '%s'", createdBy)
                .isFalse();
    }

    /// Walks every data page of every column chunk through parquet-java's page readers, and
    /// reports what they were: how many there were, and which encodings their *values* declared.
    ///
    /// The page-level value encoding is the only place the encoding choice is observable. A
    /// column chunk's `encodings` list is a union that always contains `PLAIN` — a dictionary
    /// page body is itself `PLAIN` — so it cannot tell a dictionary-only chunk from one that
    /// overflowed its dictionary and fell back mid-chunk. Each page header declares its own
    /// encoding, and those do distinguish them.
    ///
    /// Each page is materialized on the way past, so a page that does not decompress or whose
    /// header does not parse fails here.
    ///
    /// @param file the file to read
    /// @return what the walk found
    /// @throws IOException if parquet-java cannot read a page
    static Pages readPages(Path file) throws IOException {
        observe(file);
        int count = 0;
        List<Set<Encoding>> chunkValueEncodings = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader
                .open(HadoopInputFile.fromPath(hadoopPath(file), new Configuration()))) {

            List<ColumnDescriptor> columns = reader.getFileMetaData().getSchema().getColumns();
            PageReadStore rowGroup;
            while ((rowGroup = reader.readNextRowGroup()) != null) {
                for (ColumnDescriptor column : columns) {
                    PageReader pageReader = rowGroup.getPageReader(column);
                    Set<Encoding> chunkEncodings = EnumSet.noneOf(Encoding.class);
                    DataPage page;
                    while ((page = pageReader.readPage()) != null) {
                        count++;
                        chunkEncodings.add(valueEncoding(page));
                    }
                    chunkValueEncodings.add(chunkEncodings);
                }
            }
        }
        return new Pages(count, List.copyOf(chunkValueEncodings));
    }

    /// The index bit width each data page declares, for a file of one flat `REQUIRED` column.
    ///
    /// A V1 page body is `[rep levels?][def levels?][value section]`, so with neither level stream
    /// present the first body byte *is* the value section's leading bit width. Reading it is the
    /// only way to hold the per-page width to anything: the width a page declares is independent
    /// of how many entries the chunk's dictionary holds, so a file whose values still decode
    /// correctly proves nothing about which width was written.
    ///
    /// @param file the file to read, whose single column must be flat and `REQUIRED`
    /// @return one width per data page, in the order the pages were walked
    /// @throws IOException if parquet-java cannot read a page
    static List<Integer> readIndexBitWidths(Path file) throws IOException {
        observe(file);
        List<Integer> widths = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader
                .open(HadoopInputFile.fromPath(hadoopPath(file), new Configuration()))) {

            List<ColumnDescriptor> columns = reader.getFileMetaData().getSchema().getColumns();
            if (columns.size() != 1 || columns.get(0).getMaxDefinitionLevel() != 0
                    || columns.get(0).getMaxRepetitionLevel() != 0) {
                throw new IllegalArgumentException("Index bit widths are only readable from a file of one"
                        + " flat REQUIRED column, where no level stream precedes the value section");
            }
            PageReadStore rowGroup;
            while ((rowGroup = reader.readNextRowGroup()) != null) {
                PageReader pageReader = rowGroup.getPageReader(columns.get(0));
                DataPage page;
                while ((page = pageReader.readPage()) != null) {
                    Encoding encoding = valueEncoding(page);
                    if (encoding != Encoding.RLE_DICTIONARY) {
                        throw new IllegalStateException(
                                "Page declares " + encoding + ", so it carries no index stream to size");
                    }
                    widths.add(((DataPageV1) page).getBytes().toByteArray()[0] & 0xFF);
                }
            }
        }
        return widths;
    }

    /// The `distinct_count` each column chunk's statistics carry, read from the footer's Thrift
    /// rather than through parquet-java's `Statistics`, which does not surface the field. Reading
    /// the raw structure is the point: it proves the writer put field 4 on the wire with the type
    /// a third-party Thrift decoder expects, which a round trip through Hardwood's own reader
    /// cannot.
    ///
    /// @param file the file to read
    /// @return one entry per column chunk in footer order, null where the chunk carries no count
    /// @throws IOException if the footer cannot be read
    static List<Long> readDistinctCounts(Path file) throws IOException {
        List<Long> counts = new ArrayList<>();
        FileMetaData metaData = readFormatFooter(file);
        for (RowGroup rowGroup : metaData.getRow_groups()) {
            for (ColumnChunk chunk : rowGroup.getColumns()) {
                Statistics statistics = chunk.getMeta_data().getStatistics();
                counts.add(statistics != null && statistics.isSetDistinct_count()
                        ? statistics.getDistinct_count()
                        : null);
            }
        }
        return counts;
    }

    /// Records what this file contains into [CoverageRegistry], so that the write-path coverage
    /// assertion described in `_designs/WRITE_COVERAGE_ASSERTION.md` counts it as produced.
    ///
    /// Every entry point of this class calls it, which is what keeps the recording free of any
    /// opt-in: a test contributes by reading its file back, and one added later contributes by
    /// existing. The file is walked once however many entry points a single test uses.
    ///
    /// What is recorded comes out of the bytes rather than out of the test's intent — the
    /// encoding each data page declares, the codec and null count the footer reports, the
    /// annotation its schema carries. A page that cannot be read fails here, as it does in
    /// [#readPages].
    ///
    /// @param file the file to record
    /// @throws IOException if parquet-java cannot read the file
    static void observe(Path file) throws IOException {
        if (!CoverageRegistry.claim(file)) {
            return;
        }
        try (ParquetFileReader reader = ParquetFileReader
                .open(HadoopInputFile.fromPath(hadoopPath(file), new Configuration()))) {

            ParquetMetadata footer = reader.getFooter();
            MessageType schema = footer.getFileMetaData().getSchema();
            observeGroupAnnotations(schema);

            List<ColumnDescriptor> columns = schema.getColumns();
            List<BlockMetaData> blocks = footer.getBlocks();
            int blockIndex = 0;
            PageReadStore rowGroup;
            while ((rowGroup = reader.readNextRowGroup()) != null) {
                Map<String, ColumnChunkMetaData> chunks = new HashMap<>();
                for (ColumnChunkMetaData chunk : blocks.get(blockIndex++).getColumns()) {
                    chunks.put(chunk.getPath().toDotString(), chunk);
                }
                for (ColumnDescriptor column : columns) {
                    Set<Encoding> encodings = EnumSet.noneOf(Encoding.class);
                    PageReader pageReader = rowGroup.getPageReader(column);
                    DataPage page;
                    while ((page = pageReader.readPage()) != null) {
                        encodings.add(valueEncoding(page));
                    }
                    observeChunk(column, chunks.get(String.join(".", column.getPath())), encodings);
                }
            }
        }
    }

    /// Records one column chunk under the physical type, length, annotation, encodings, codec and
    /// repetition shape parquet-java reports for it.
    private static void observeChunk(ColumnDescriptor column, ColumnChunkMetaData chunk,
            Set<Encoding> encodings) {

        if (chunk == null || encodings.isEmpty()) {
            return;
        }
        PrimitiveType primitive = column.getPrimitiveType();
        Integer typeLength = primitive.getPrimitiveTypeName() == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
                ? primitive.getTypeLength()
                : null;
        CoverageRegistry.observeColumnChunk(
                physicalType(primitive.getPrimitiveTypeName()),
                typeLength,
                LogicalTypeKey.of(primitive.getLogicalTypeAnnotation()),
                encodings,
                CompressionCodec.valueOf(chunk.getCodec().name()),
                repetitionShape(column, chunk));
    }

    /// Records the annotation of every group node of `group`, which carry `LIST` and `MAP` and
    /// have no column chunk of their own.
    private static void observeGroupAnnotations(GroupType group) {
        for (Type field : group.getFields()) {
            if (field.isPrimitive()) {
                continue;
            }
            if (field.getLogicalTypeAnnotation() != null) {
                CoverageRegistry.observeGroupAnnotation(
                        LogicalTypeKey.of(field.getLogicalTypeAnnotation()));
            }
            observeGroupAnnotations(field.asGroupType());
        }
    }

    /// The repetition shape of a column chunk.
    ///
    /// The three `OPTIONAL` shapes share a descriptor and differ only in the definition levels
    /// the chunk's values produced, so they are told apart by its null count against its value
    /// count — the latter counting nulls, as the format's `num_values` does. A chunk whose
    /// statistics do not state a null count is reported as `null`, leaving the shape unrecorded
    /// rather than guessed.
    private static Coverage.RepetitionShape repetitionShape(ColumnDescriptor column,
            ColumnChunkMetaData chunk) {

        if (column.getMaxRepetitionLevel() > 0) {
            return Coverage.RepetitionShape.REPEATED;
        }
        if (column.getMaxDefinitionLevel() == 0) {
            return Coverage.RepetitionShape.REQUIRED;
        }
        if (chunk.getStatistics() == null || !chunk.getStatistics().isNumNullsSet()) {
            return null;
        }
        long nulls = chunk.getStatistics().getNumNulls();
        if (nulls == 0) {
            return Coverage.RepetitionShape.OPTIONAL_ALL_PRESENT;
        }
        return nulls == chunk.getValueCount()
                ? Coverage.RepetitionShape.OPTIONAL_ALL_NULL
                : Coverage.RepetitionShape.OPTIONAL_SOME_NULL;
    }

    /// The Hardwood physical type parquet-java's primitive type names denote. The two disagree on
    /// one spelling only: what the format calls `BYTE_ARRAY` parquet-java calls `BINARY`.
    private static PhysicalType physicalType(PrimitiveTypeName name) {
        return switch (name) {
            case BOOLEAN -> PhysicalType.BOOLEAN;
            case INT32 -> PhysicalType.INT32;
            case INT64 -> PhysicalType.INT64;
            case INT96 -> PhysicalType.INT96;
            case FLOAT -> PhysicalType.FLOAT;
            case DOUBLE -> PhysicalType.DOUBLE;
            case BINARY -> PhysicalType.BYTE_ARRAY;
            case FIXED_LEN_BYTE_ARRAY -> PhysicalType.FIXED_LEN_BYTE_ARRAY;
        };
    }

    /// Reads the footer as the format's own Thrift structure: seek to the four-byte length ahead
    /// of the trailing magic, then decode that many bytes.
    private static FileMetaData readThriftFooter(Path file, SeekableInputStream in) throws IOException {
        long fileLength = Files.size(file);
        in.seek(fileLength - MAGIC_LENGTH - FOOTER_LENGTH_BYTES);
        byte[] lengthBytes = new byte[FOOTER_LENGTH_BYTES];
        in.readFully(lengthBytes);
        int footerLength = ByteBuffer.wrap(lengthBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
        in.seek(fileLength - MAGIC_LENGTH - FOOTER_LENGTH_BYTES - footerLength);
        byte[] footer = new byte[footerLength];
        in.readFully(footer);
        return Util.readFileMetaData(new ByteArrayInputStream(footer));
    }

    private static final int MAGIC_LENGTH = 4;
    private static final int FOOTER_LENGTH_BYTES = 4;

    /// What a page walk found: the data page count, and per column chunk the set of encodings
    /// that chunk's data pages declared. The sets are per chunk rather than per file because a
    /// chunk's encoding is chosen from the values that chunk holds, so two chunks of one column
    /// may legitimately differ while neither may vary within itself.
    ///
    /// @param dataPageCount how many data pages the file holds
    /// @param chunkValueEncodings the distinct value encodings each column chunk's pages declared,
    ///        in the order the chunks were walked
    record Pages(int dataPageCount, List<Set<Encoding>> chunkValueEncodings) {
    }

    /// The encoding a data page declares for its values. The writer produces DataPage V1 only, so
    /// anything else is a change in what is being written rather than a case this reader should
    /// quietly accommodate.
    private static Encoding valueEncoding(DataPage page) {
        if (!(page instanceof DataPageV1 v1)) {
            throw new IllegalStateException(
                    "Expected a V1 data page but got " + page.getClass().getSimpleName());
        }
        return v1.getValueEncoding();
    }

    private static org.apache.hadoop.fs.Path hadoopPath(Path file) {
        return new org.apache.hadoop.fs.Path(file.toUri());
    }
}
