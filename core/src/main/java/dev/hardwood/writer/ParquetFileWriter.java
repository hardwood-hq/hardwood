/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import dev.hardwood.Experimental;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.compression.Compressor;
import dev.hardwood.internal.compression.CompressorFactory;
import dev.hardwood.internal.encoding.LevelEncoder;
import dev.hardwood.internal.thrift.FileMetaDataWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.internal.writer.ColumnSource;
import dev.hardwood.internal.writer.LogicalTypeValueRange;
import dev.hardwood.internal.writer.RecordShredder;
import dev.hardwood.internal.writer.RowGroupBuffer;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Writes a Parquet file through a columnar batch API.
///
/// Every primitive physical type is written — `BOOLEAN`, `INT32`, `INT64`, `FLOAT`,
/// `DOUBLE`, `BYTE_ARRAY`, and `FIXED_LEN_BYTE_ARRAY` — flat `REQUIRED` / `OPTIONAL`, nested
/// inside `REQUIRED` / `OPTIONAL` `struct` groups, and inside `LIST`s and `MAP`s (including lists
/// of lists, lists of structs, and maps of any in-scope value). Data is supplied as
/// [ColumnBatch] slices; the writer packs each column into size-bounded data pages — a
/// levelled column's pages carrying an RLE definition-level stream ahead of the values — and
/// flushes a row group once its buffered data reaches the configured target, so peak memory is
/// bounded regardless of how much is written. Each column chunk is encoded one way throughout:
/// by default the writer weighs a dictionary against `PLAIN` once the row group is buffered and
/// takes the smaller, or a [ColumnEncoding] set per column names the encoding outright. Each page
/// body is compressed with the configured codec (`ZSTD` by default). All of these are
/// configurable through [WriterConfig]. The row groups and footer are finalized on [#close()].
///
/// The file is produced front to back and is valid only after `close()` returns.
public final class ParquetFileWriter implements Closeable {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    private static final int FORMAT_VERSION = 1;

    /// Nominal `BYTE_ARRAY` value length assumed when estimating the flush-check stride. Only the
    /// append granularity depends on it; the row group flushes on actual buffered bytes.
    private static final int ASSUMED_BYTE_ARRAY_LENGTH = 16;

    /// Fraction of the row-group target one column's dictionary may occupy while the writer
    /// decides how to encode that column. The bound is on memory, not on the encoding choice, so
    /// it is derived from the target that already states how much the writer may hold rather than
    /// being a knob of its own. Half is deliberately generous: a dictionary larger than half the
    /// data it describes cannot beat writing the values `PLAIN` by more than a few percent, so the
    /// cap bounds the pathological case without ever overruling a decision worth making.
    private static final int DICTIONARY_ANALYSIS_SHARE = 2;

    /// Floor under the derived cap, so a small row-group target does not reduce every chunk to
    /// `PLAIN` by starving the analysis.
    private static final long MIN_DICTIONARY_ANALYSIS_BYTES = 1 << 20;

    private final OutputFile out;
    private final FileSchema schema;
    private final WriterConfig config;
    /// Records appended before the per-record size is known, to learn it without overshooting a
    /// small row-group target on a batch of large variable-width values.
    private static final int PROBE_RECORDS = 64;

    private final int pageValues;
    private final long rowGroupTargetBits;
    private final RecordShredder shredder;
    private final Compressor compressor;
    /// The range each column's annotation declares, resolved once and handed to every batch.
    private final LogicalTypeValueRange[] ranges;
    private final List<RowGroup> rowGroups = new ArrayList<>();

    // Running actual buffered-bit average, learned across the whole write so the append stride
    // between flush checks lands near the row-group boundary regardless of value width.
    private long cumulativeBits;
    private long cumulativeRecords;

    private final RowGroupBuffer current;
    private long numRows;
    private boolean closed;

    /// Which of the two write APIs this file is being written through. A file is written
    /// through one or the other: rows and batches would otherwise interleave two independent
    /// staging states into one row group.
    private enum Mode { UNSET, BATCH, ROW }

    private Mode mode = Mode.UNSET;
    private RowWriter rowWriter;

    private ParquetFileWriter(OutputFile out, FileSchema schema, WriterConfig config, Compressor compressor,
            ColumnEncoding[] encodings) {
        this.out = out;
        this.schema = schema;
        this.config = config;
        this.pageValues = pageRowCapacity(config.pageTargetBytes(), schema);
        this.rowGroupTargetBits = Math.multiplyExact(config.rowGroupTargetBytes(), Byte.SIZE);
        this.shredder = new RecordShredder(schema);
        this.compressor = compressor;
        this.ranges = LogicalTypeValueRange.forSchema(schema);
        // One buffer serves every row group: flushing it resets it in place, so the writer's
        // largest allocation is made once per file rather than once per row group.
        this.current = new RowGroupBuffer(schema, pageValues, encodings,
                dictionaryAnalysisCapBytes(config), config.statisticsTruncationLength(), compressor, config.codec());
    }

    /// How large one column's dictionary may grow while the writer decides that column's encoding.
    private static long dictionaryAnalysisCapBytes(WriterConfig config) {
        return Math.max(config.rowGroupTargetBytes() / DICTIONARY_ANALYSIS_SHARE, MIN_DICTIONARY_ANALYSIS_BYTES);
    }

    /// Opens a writer with the default [WriterConfig].
    ///
    /// @param out the destination
    /// @param schema the schema to write
    /// @return an open writer
    /// @throws IOException if the destination cannot be opened
    /// @throws UnsupportedOperationException if the schema has a column of an unsupported
    ///         physical type
    public static ParquetFileWriter create(OutputFile out, FileSchema schema) throws IOException {
        return create(out, schema, WriterConfig.defaults());
    }

    /// Opens a writer, writing the leading magic bytes.
    ///
    /// @param out the destination
    /// @param schema the schema to write
    /// @param config the writer configuration
    /// @return an open writer
    /// @throws IOException if the destination cannot be opened
    /// @throws UnsupportedOperationException if the schema has a column of an unsupported
    ///         physical type, or the configured codec cannot be written
    /// @throws IllegalArgumentException if an encoding policy names a column the schema does not
    ///         have, or one its physical type cannot carry
    public static ParquetFileWriter create(OutputFile out, FileSchema schema, WriterConfig config)
            throws IOException {
        for (int c = 0; c < schema.getColumnCount(); c++) {
            ColumnSchema column = schema.getColumn(c);
            if (!isSupportedType(column.type())) {
                throw new UnsupportedOperationException(
                        "Writer does not support " + column.type() + " columns yet; column "
                                + column.name() + " is " + column.type());
            }
        }
        // Everything the configuration says about this schema is settled before the output is
        // touched, so a file the writer cannot honour is never begun: the encoding policies
        // against the schema's columns, then the codec and its library.
        ColumnEncoding[] encodings = resolveEncodings(schema, config);
        Compressor compressor = new CompressorFactory().getCompressor(config.codec());
        out.create();
        out.write(ByteBuffer.wrap(MAGIC));
        return new ParquetFileWriter(out, schema, config, compressor, encodings);
    }

    /// Resolves each leaf column's encoding policy, rejecting a configuration this schema cannot
    /// carry.
    ///
    /// Two things are rejected, and neither could be reported later: a path naming no leaf column
    /// is a typo whose only other effect would be to write the file in an encoding the caller did
    /// not ask for, and a policy illegal for a column's physical type has no honest resolution —
    /// quietly writing that column some other way is the silent divergence this check exists to
    /// prevent. The file-wide default is held to the same rule as an override, so a default no
    /// column of the schema can carry fails rather than applying to none of them.
    private static ColumnEncoding[] resolveEncodings(FileSchema schema, WriterConfig config) {
        Map<String, ColumnEncoding> overrides = config.columnEncodings();
        if (!overrides.isEmpty()) {
            Set<String> leafPaths = new LinkedHashSet<>();
            for (int c = 0; c < schema.getColumnCount(); c++) {
                leafPaths.add(schema.getColumn(c).fieldPath().toString());
            }
            for (String path : overrides.keySet()) {
                if (!leafPaths.contains(path)) {
                    throw new IllegalArgumentException("Encoding configured for column '" + path
                            + "', which the schema does not have. Its leaf columns are: " + leafPaths);
                }
            }
        }

        ColumnEncoding[] encodings = new ColumnEncoding[schema.getColumnCount()];
        for (int c = 0; c < schema.getColumnCount(); c++) {
            ColumnSchema column = schema.getColumn(c);
            // Keyed on the dotted path rather than the leaf name: a schema may repeat a name at
            // several depths, and only the path identifies one leaf.
            String path = column.fieldPath().toString();
            ColumnEncoding encoding = config.encodingFor(path);
            if (!encoding.supports(column.type())) {
                throw new IllegalArgumentException("Encoding " + encoding + " cannot be written for column '"
                        + path + "', which is " + column.type()
                        + (overrides.containsKey(path)
                                ? ". Choose an encoding that column's type can carry."
                                : ". It is the file-wide default; set a per-column encoding instead."));
            }
            encodings[c] = encoding;
        }
        return encodings;
    }

    /// Writes one aligned batch of column values, flushing row groups as the buffered
    /// data crosses the row-group target. A batch that would overflow the current row
    /// group is split at the boundary.
    ///
    /// The writer creates the batch — bound to the schema — passes it to `filler` to be
    /// populated (columns addressed by index or name), then submits it. There is no
    /// separate build or submit step to forget.
    ///
    /// @param filler populates the batch's columns; must cover every column exactly once
    /// @throws IOException if the write fails
    /// @throws IllegalArgumentException if the batch does not cover every column, or its
    ///         per-layer inputs do not agree on a record count
    /// @throws UnsupportedOperationException if the schema has a shape the writer cannot
    ///         produce
    /// @throws IllegalStateException if the writer is closed, or [#rowWriter()] has already
    ///         been used on this file
    public void writeBatch(Consumer<ColumnBatch> filler) throws IOException {
        ensureOpen();
        latch(Mode.BATCH);
        writeStagedBatch(filler);
    }

    /// Returns the row-oriented view over this file: a record-shaped API for callers that
    /// hold records rather than columns. It stages records into batches and submits them
    /// through [#writeBatch], so the file it produces is the one the columnar API produces
    /// for the same data.
    ///
    /// The file writer keeps ownership: the returned view is not closeable, and its pending
    /// records are written by [#close()]. The same instance is returned on every call.
    ///
    /// @return the row-oriented view over this file
    /// @throws IllegalStateException if the writer is closed, or [#writeBatch] has already
    ///         been used on this file
    /// @throws UnsupportedOperationException if the schema has a shape the writer cannot
    ///         produce record by record
    @Experimental
    public RowWriter rowWriter() {
        ensureOpen();
        latch(Mode.ROW);
        if (rowWriter == null) {
            rowWriter = new RowWriter(this, schema, config);
        }
        return rowWriter;
    }

    /// Latches the file to one of the two write APIs, rejecting the other from then on.
    private void latch(Mode wanted) {
        if (mode == Mode.UNSET) {
            mode = wanted;
        }
        else if (mode != wanted) {
            throw new IllegalStateException("This file is already being written through "
                    + (mode == Mode.BATCH ? "writeBatch(...)" : "rowWriter()")
                    + "; a file is written through one of the two, not both");
        }
    }

    /// Writes one batch without latching the write mode, so the row-oriented layer can submit
    /// the batches it stages.
    void writeStagedBatch(Consumer<ColumnBatch> filler) throws IOException {
        ColumnBatch batch = new ColumnBatch(schema, ranges);
        filler.accept(batch);
        ColumnSource[] sources = batch.completedSources();
        shredder.bind(sources, batch.validities(), batch.structValidities(),
                batch.listValidities(), batch.listOffsets());
        batch.markConsumed();
        int rows = shredder.recordCount();
        int pos = 0;
        while (pos < rows) {
            int n = nextStride(rows - pos);
            long before = current.bufferedBits();
            current.appendRecords(shredder, sources, pos, n);
            cumulativeBits += current.bufferedBits() - before;
            cumulativeRecords += n;
            pos += n;
            if (current.bufferedBits() >= rowGroupTargetBits) {
                flushRowGroup();
            }
        }
    }

    /// How many of the next `remaining` records to append before re-checking the buffered-byte
    /// target. Until the per-record size is measured, a small probe learns it without
    /// overshooting; afterwards the stride is sized to just fill the remaining row-group budget
    /// from the running average (exact for fixed-width columns), so a row group lands on the
    /// target regardless of value width. Capped at one page's worth of entries.
    private int nextStride(int remaining) {
        if (cumulativeRecords == 0) {
            return Math.min(remaining, PROBE_RECORDS);
        }
        long avgBits = Math.max(1, cumulativeBits / cumulativeRecords);
        long remainingBits = Math.max(avgBits, rowGroupTargetBits - current.bufferedBits());
        long stride = Math.min(pageValues, ceilDiv(remainingBits, avgBits));
        return (int) Math.max(1, Math.min(remaining, stride));
    }

    private static long ceilDiv(long numerator, long denominator) {
        return (numerator + denominator - 1) / denominator;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (rowWriter != null) {
                rowWriter.flushPending();
            }
            flushRowGroup();
            writeFooter();
        }
        catch (IOException | RuntimeException e) {
            // The footer is incomplete, so the file is not valid. Discard it rather than
            // letting out.close() publish a truncated file.
            try {
                out.discard();
            }
            catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
        out.close();
    }

    private void flushRowGroup() throws IOException {
        if (current.isEmpty()) {
            return;
        }
        RowGroup rowGroup = current.flushTo(out);
        rowGroups.add(rowGroup);
        numRows += rowGroup.numRows();
        current.reset();
    }

    /// One `ColumnOrder` per leaf column, in schema order. The format requires the list wherever
    /// `Statistics` bounds are written, without which their meaning is undefined.
    ///
    /// Every column declares the type-defined order — the one every statistics collector
    /// computes in, floats included: their NaN exclusion and signed-zero normalization are the
    /// type-defined convention, and under the IEEE 754 total order a NaN is an ordinary value
    /// sorting beyond the infinities, so bounds that exclude it would let a total-order reader
    /// drop a page that holds one.
    private List<ColumnOrder> columnOrders() {
        return Collections.nCopies(schema.getColumnCount(), ColumnOrder.TYPE_DEFINED_ORDER);
    }

    private void writeFooter() throws IOException {
        FileMetaData metaData = new FileMetaData(
                FORMAT_VERSION,
                schema.toSchemaElements(),
                numRows,
                List.copyOf(rowGroups),
                Map.of(),
                config.createdBy(),
                columnOrders());

        ThriftCompactWriter footer = new ThriftCompactWriter();
        FileMetaDataWriter.write(footer, metaData);
        byte[] footerBytes = footer.toByteArray();

        out.write(ByteBuffer.wrap(footerBytes));
        out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footerBytes.length).flip());
        out.write(ByteBuffer.wrap(MAGIC));
    }

    /// Rows per data page whose encoded body fits the page target. A page costs each column's
    /// estimated `PLAIN` value bit width per row plus, for a levelled column, its RLE
    /// definition-level stream; sizing to the widest column's per-row bit cost keeps every
    /// column's page within the target. At least one row so a tiny target still makes progress.
    /// This is a page-level entry-count bound only; the actual row-group flush tracks buffered
    /// bytes, so a variable-width estimate here does not distort the produced file.
    private static int pageRowCapacity(long pageTargetBytes, FileSchema schema) {
        long maxColumnBitsPerRow = 1;
        for (int c = 0; c < schema.getColumnCount(); c++) {
            ColumnSchema column = schema.getColumn(c);
            int defBits = LevelEncoder.bitWidth(column.maxDefinitionLevel());
            maxColumnBitsPerRow = Math.max(maxColumnBitsPerRow, estimatedValueBits(column) + defBits);
        }
        long rows = pageTargetBytes * Byte.SIZE / maxColumnBitsPerRow;
        return (int) Math.max(1, Math.min(rows, Integer.MAX_VALUE));
    }

    /// The estimated `PLAIN` bit width of one of a column's values, used only to bound the
    /// per-page entry count: exact for the fixed-width scalars and for `FIXED_LEN_BYTE_ARRAY`
    /// (its schema type length), and a nominal estimate for `BYTE_ARRAY` (a 4-byte length prefix
    /// plus an assumed value length).
    private static long estimatedValueBits(ColumnSchema column) {
        return switch (column.type()) {
            case BOOLEAN -> 1;
            case INT32, FLOAT -> Integer.SIZE;
            case INT64, DOUBLE -> Long.SIZE;
            case FIXED_LEN_BYTE_ARRAY -> (long) requireTypeLength(column) * Byte.SIZE;
            case BYTE_ARRAY -> (long) (Integer.BYTES + ASSUMED_BYTE_ARRAY_LENGTH) * Byte.SIZE;
            case INT96 -> throw new IllegalArgumentException("INT96 is not supported by the writer");
        };
    }

    private static int requireTypeLength(ColumnSchema column) {
        if (column.typeLength() == null) {
            throw new IllegalArgumentException(
                    "FIXED_LEN_BYTE_ARRAY column " + column.name() + " requires a type length");
        }
        return column.typeLength();
    }

    /// Whether the writer supports producing a column of this physical type.
    private static boolean isSupportedType(PhysicalType type) {
        return switch (type) {
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> true;
            case INT96 -> false;
        };
    }

    void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Writer is closed");
        }
    }
}
