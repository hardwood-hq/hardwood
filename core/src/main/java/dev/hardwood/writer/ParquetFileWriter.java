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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import dev.hardwood.Experimental;
import dev.hardwood.OutputFile;
import dev.hardwood.internal.BuildInfo;
import dev.hardwood.internal.compression.Compressor;
import dev.hardwood.internal.compression.CompressorFactory;
import dev.hardwood.internal.thrift.FileMetaDataWriter;
import dev.hardwood.internal.thrift.ThriftCompactWriter;
import dev.hardwood.internal.writer.ColumnSource;
import dev.hardwood.internal.writer.LogicalTypeValueRange;
import dev.hardwood.internal.writer.RecordShredder;
import dev.hardwood.internal.writer.RowGroupBuffer;
import dev.hardwood.internal.writer.WriterSchemaShape;
import dev.hardwood.metadata.ColumnOrder;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

/// Writes a Parquet file through a columnar or a row-oriented API: [#columnWriter()] takes an
/// aligned slice of typed arrays, [#rowWriter()] takes one record at a time, and one file is
/// written through one of the two, not both.
///
/// Every primitive physical type is written — `BOOLEAN`, `INT32`, `INT64`, `FLOAT`,
/// `DOUBLE`, `BYTE_ARRAY`, and `FIXED_LEN_BYTE_ARRAY` — flat `REQUIRED` / `OPTIONAL`, nested
/// inside `REQUIRED` / `OPTIONAL` `struct` groups, and inside `LIST`s and `MAP`s (including lists
/// of lists, lists of structs, and maps of any in-scope value). Data is supplied as
/// [ColumnBatch] slices; the writer packs each column into size-bounded data pages — a
/// levelled column's pages carrying an RLE definition-level stream ahead of the values — and
/// flushes a row group once the bytes it holds for that group reach the configured target. Each
/// row group is buffered, written and forgotten, so what the writer holds follows the target
/// rather than the size of the file — and the target counts what is held, so it is that number
/// and not a multiple of it, give or take the slack the value stores carry from growing
/// geometrically. Each column chunk is encoded one way throughout:
/// by default the writer weighs a dictionary against `PLAIN` once the row group is buffered and
/// takes the smaller, or a [ColumnEncoding] set per column names the encoding outright. Each page
/// body is compressed with the configured codec (`ZSTD` by default). All of these are
/// configurable through [WriterConfig]. The row groups and footer are finalized on [#close()].
///
/// The file is produced front to back and is valid only after `close()` returns. A writer that
/// has thrown from a write operation is marked as failed and, under the default
/// [WriteFailurePolicy#DISCARD], refuses to publish: `close()` discards the output silently, and
/// `try-with-resources` is safe by default. [WriteFailurePolicy#COMMIT_PREFIX] opts into publishing
/// whatever was flushed before the failure.
public final class ParquetFileWriter implements Closeable {

    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.UTF_8);
    private static final int FORMAT_VERSION = 1;

    /// Default `created_by` identifier written into the file footer, in the
    /// `<app> version <version> (build <hash>)` convention Parquet readers parse — for
    /// example `hardwood version 1.1.0 (build a093aab)`. The hash carries a `-dirty` suffix
    /// when the working tree was not clean at build time, and a build that cannot identify
    /// itself reports `unknown` in place of the version or the hash.
    ///
    /// A reader that cannot parse this field cannot tell which writer produced the file, and
    /// applies its writer-specific correctness workarounds to it by default.
    public static final String DEFAULT_CREATED_BY = defaultCreatedBy();



    private final OutputFile out;
    private final FileSchema schema;
    private final WriterConfig config;
    /// The configured row target, held down to what a chunk's buffers can index. One bound rather
    /// than two: a caller's row target above the structural ceiling is the ceiling.
    private final int rowGroupTargetRows;
    private final RecordShredder shredder;
    private final Compressor compressor;
    /// The range each column's annotation declares, resolved once and handed to every batch.
    private final LogicalTypeValueRange[] ranges;
    private final List<RowGroup> rowGroups = new ArrayList<>();

    /// The footer's two file-scope fields, held until [#close()] serializes them. Insertion
    /// ordered so the entries reach the file in the order they were given.
    private final Map<String, String> keyValueMetadata = new LinkedHashMap<>();
    private String createdBy = DEFAULT_CREATED_BY;

    private final WriteFailurePolicy writeFailurePolicy;
    private final RowGroupBuffer current;
    private long numRows;
    private boolean closed;
    private boolean failed;

    /// Which of the two write APIs this file is being written through. A file is written
    /// through one or the other: rows and batches would otherwise interleave two independent
    /// staging states into one row group.
    private enum Mode { UNSET, BATCH, ROW }

    private Mode mode = Mode.UNSET;
    private ColumnWriter columnWriter;
    private RowWriter rowWriter;

    private ParquetFileWriter(OutputFile out, FileSchema schema, WriterConfig config, Compressor compressor,
            ColumnEncoding[] encodings) {
        this.out = out;
        this.schema = schema;
        this.config = config;
        this.rowGroupTargetRows = (int) Math.min(config.rowGroupTargetRows(), RowGroupBuffer.MAX_ROWS);
        this.shredder = new RecordShredder(schema);
        this.compressor = compressor;
        this.ranges = LogicalTypeValueRange.forSchema(schema);
        this.writeFailurePolicy = config.writeFailurePolicy();
        // One buffer serves every row group: flushing it resets it in place, so the writer's
        // largest allocation is made once per file rather than once per row group.
        this.current = new RowGroupBuffer(schema, config.pageTargetBytes(),
                config.rowGroupBufferTargetBytes(), encodings,
                config.statisticsTruncationLength(), compressor, config.codec());
    }

    /// The bytes the open row group retains. Exposed to the tests that hold this number against a
    /// measurement of the heap, which is the only check that it means what it says.
    long retainedBytes() {
        return current.retainedBytes();
    }

    /// The most this writer has held for one row group over the file so far.
    ///
    /// A row group is cut on what it holds, so what a caller has actually been charged is the
    /// high-water mark across the file's row groups rather than whatever the open one holds now —
    /// and the peak of a batch that spans several row groups falls inside a single `writeBatch`,
    /// where nothing outside the writer can observe it.
    long peakRetainedBytes() {
        return peakRetainedBytes;
    }

    private long peakRetainedBytes;


    /// Opens a writer with the default [WriterConfig].
    ///
    /// @param out the destination
    /// @param schema the schema to write
    /// @return an open writer
    /// @throws IOException if the destination cannot be opened
    /// @throws UnsupportedOperationException if the schema has a column of an unsupported
    ///         physical type, or a shape the writer cannot produce
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
    ///         physical type, a shape the writer cannot produce, or the configured codec cannot
    ///         be written
    /// @throws IllegalArgumentException if an encoding policy names a column the schema does not
    ///         have, or one its physical type cannot carry
    public static ParquetFileWriter create(OutputFile out, FileSchema schema, WriterConfig config)
            throws IOException {
        // Everything this schema and configuration decide is settled before the output is
        // touched, so a file the writer cannot honour is never begun: the columns' physical
        // types, the schema's shape, the encoding policies against the schema's columns, then
        // the codec and its library.
        for (int c = 0; c < schema.getColumnCount(); c++) {
            ColumnSchema column = schema.getColumn(c);
            if (!isSupportedType(column.type())) {
                throw new UnsupportedOperationException(
                        "Writer does not support " + column.type() + " columns yet; column "
                                + column.name() + " is " + column.type());
            }
        }
        // Settled here rather than by whichever view meets it first, so one unproducible shape
        // is one rejection, at one moment, with one wording.
        WriterSchemaShape.validate(schema);
        ColumnEncoding[] encodings = resolveEncodings(schema, config);
        Compressor compressor = new CompressorFactory().getCompressor(config.codec());
        out.create();
        try {
            out.write(ByteBuffer.wrap(MAGIC));
            return new ParquetFileWriter(out, schema, config, compressor, encodings);
        }
        catch (IOException | RuntimeException e) {
            // The destination is open and holds no valid file. Discard it rather than leaving
            // the temporary artefact a local OutputFile streams into orphaned at the target.
            try {
                out.discard();
            }
            catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
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

    /// Stamps one application-defined key-value pair onto the file footer, replacing any value
    /// already held for that key.
    ///
    /// The footer's `key_value_metadata` is where the ecosystem records what a schema alone
    /// does not carry — `ARROW:schema`, `pandas`, Spark's
    /// `org.apache.spark.sql.parquet.row.metadata`, and the table-format stamps. Parquet itself
    /// does not interpret these entries, and nothing here validates them beyond requiring a key.
    ///
    /// Callable until [#close()], so a value the caller knows only once the data is written — a
    /// row count, a digest over what was produced — can still be stated.
    ///
    /// @param key the entry's key
    /// @param value the entry's value, or `null` to write a key carrying no value, which the
    ///        format allows and which is how a key read from such a file is written back
    /// @throws IllegalArgumentException if `key` is `null`
    /// @throws IllegalStateException if the writer is closed
    public void keyValueMetadata(String key, String value) {
        ensureOpen();
        if (key == null) {
            throw new IllegalArgumentException("Metadata key must not be null");
        }
        keyValueMetadata.put(key, value);
    }

    /// Stamps every entry of `metadata` onto the file footer, replacing any value already held
    /// for a key it names and leaving the rest in place.
    ///
    /// Passing the map a reader returns from `FileMetaData.keyValueMetadata()` reproduces that
    /// file's application metadata, entries carrying no value included.
    ///
    /// @param metadata the entries to add
    /// @throws IllegalArgumentException if `metadata` is `null` or holds a `null` key
    /// @throws IllegalStateException if the writer is closed
    /// @see #keyValueMetadata(String, String)
    public void keyValueMetadata(Map<String, String> metadata) {
        ensureOpen();
        if (metadata == null) {
            throw new IllegalArgumentException("Metadata must not be null");
        }
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException("Metadata key must not be null");
            }
        }
        keyValueMetadata.putAll(metadata);
    }

    /// Replaces the footer's `created_by` identifier, which defaults to [#DEFAULT_CREATED_BY].
    ///
    /// Readers that key compatibility workarounds off this field expect the
    /// `<app> version <version> (build <hash>)` shape; a bare application name is rejected by
    /// some of them.
    ///
    /// @param createdBy the identifier to write
    /// @throws IllegalArgumentException if `createdBy` is `null`
    /// @throws IllegalStateException if the writer is closed
    public void createdBy(String createdBy) {
        ensureOpen();
        if (createdBy == null) {
            throw new IllegalArgumentException("createdBy must not be null");
        }
        this.createdBy = createdBy;
    }

    /// Returns the column-oriented view over this file: a batch-shaped API for callers that
    /// hold columns rather than records. It takes an aligned slice of typed arrays through
    /// [ColumnBatch], shreds it, and pages it into the file.
    ///
    /// The file writer keeps ownership: the returned view is not closeable, and the row group
    /// it has buffered is written by [#close()]. The same instance is returned on every call.
    ///
    /// @return the column-oriented view over this file
    /// @throws IllegalStateException if the writer is closed, or [#rowWriter()] has already
    ///         been used on this file
    public ColumnWriter columnWriter() {
        ensureOpen();
        latch(Mode.BATCH);
        if (columnWriter == null) {
            columnWriter = new ColumnWriter(this);
        }
        return columnWriter;
    }

    /// Returns the row-oriented view over this file: a record-shaped API for callers that
    /// hold records rather than columns. It stages records into batches and submits them
    /// through [ColumnWriter#writeBatch], so the file it produces is the one the columnar API
    /// produces for the same data.
    ///
    /// The file writer keeps ownership: the returned view is not closeable, and its pending
    /// records are written by [#close()]. The same instance is returned on every call.
    ///
    /// @return the row-oriented view over this file
    /// @throws IllegalStateException if the writer is closed, or [#columnWriter()] has already
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
                    + (mode == Mode.BATCH ? "columnWriter()" : "rowWriter()")
                    + "; a file is written through one of the two, not both");
        }
    }

    /// Writes one batch without latching the write mode, so both views can submit through it:
    /// [ColumnWriter] the batch its caller filled, [RowWriter] the batches it stages.
    ///
    /// A failure anywhere in the batch — validation or I/O — marks the writer as failed.
    /// Under the default [WriteFailurePolicy#DISCARD], a failed writer refuses subsequent writes
    /// and discards the output on [#close()] rather than publishing a truncated file.
    void writeStagedBatch(Consumer<ColumnBatch> filler) throws IOException {
        ensureNotFailed();
        try {
            ColumnBatch batch = new ColumnBatch(schema, ranges);
            filler.accept(batch);
            ColumnSource[] sources = batch.completedSources();
            shredder.bind(sources, batch.validities(), batch.structValidities(),
                    batch.listValidities(), batch.listOffsets());
            batch.markConsumed();
            int rows = shredder.recordCount();
            int pos = 0;
            // Carried across iterations: what the row group holds after a slice is also the room the
            // next slice is sized against, so it is read once per slice rather than once for each.
            long retained = current.retainedBytes();
            while (pos < rows) {
                // A slice at a time. What a range actually costs is only known once it has been
                // appended — a value interned against a live dictionary retains an index where it
                // repeats and an index plus the value where it does not, which needs the hash — so
                // the writer sizes a slice by what it *could* cost and then reads what the row group
                // turned out to hold. Sizing it by the bound is what keeps a batch whose records
                // widen part way through from carrying a row group far past its target, which no
                // measurement of the records already appended could anticipate.
                int slice = Math.min(Math.min(rows - pos, SLICE_RECORDS),
                        rowGroupTargetRows - current.rowCount());
                slice = current.sliceThatFits(shredder, sources, pos, slice,
                        config.rowGroupBufferTargetBytes() - retained);
                current.appendRecords(shredder, sources, pos, slice);
                pos += slice;
                retained = current.retainedBytes();
                if (retained > peakRetainedBytes) {
                    peakRetainedBytes = retained;
                }
                // Either target closes the group, whichever is reached first.
                if (retained >= config.rowGroupBufferTargetBytes()
                        || current.rowCount() >= rowGroupTargetRows) {
                    flushRowGroup();
                    retained = current.retainedBytes();
                }
            }
        }
        catch (IOException | RuntimeException e) {
            failed = true;
            throw e;
        }
    }

    /// Records appended between two readings of what the row group holds.
    ///
    /// It bounds how far a row group can overshoot its byte target: at most one slice, and a
    /// slice is drawn from the batch the caller has already materialized, a [ColumnSource]
    /// holding the caller's arrays by reference rather than copying them. Small enough that the
    /// overshoot is a fraction of the target for any record a caller can hold; large enough that
    /// a column's slice is one bulk copy rather than a call per value.
    private static final int SLICE_RECORDS = 4096;

    /// Closes this writer, finalizing the file or discarding it.
    ///
    /// Under the default [WriteFailurePolicy#DISCARD], a writer that has been marked as failed — by
    /// a previous exception from [ColumnWriter#writeBatch] or from a [RowWriter] flush —
    /// discards its output silently: `close()` calls [OutputFile#discard()] and returns
    /// without throwing, so `try-with-resources` propagates only the original exception.
    ///
    /// Under [WriteFailurePolicy#COMMIT_PREFIX], a failed writer proceeds normally, writing a valid
    /// footer over whatever rows were flushed before the failure.
    ///
    /// A failure during `close()` itself — the footer cannot be serialized or written —
    /// always discards, regardless of the policy.
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (failed && writeFailurePolicy == WriteFailurePolicy.DISCARD) {
            // A write has failed and the policy says: leave nothing behind.
            // Discard silently so try-with-resources propagates only the original exception.
            try {
                out.discard();
            }
            catch (IOException ignored) {
                // Best-effort cleanup; the caller already has the write failure.
            }
            return;
        }
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
                Collections.unmodifiableMap(new LinkedHashMap<>(keyValueMetadata)),
                createdBy,
                columnOrders());

        ThriftCompactWriter footer = new ThriftCompactWriter();
        FileMetaDataWriter.write(footer, metaData);
        byte[] footerBytes = footer.toByteArray();

        out.write(ByteBuffer.wrap(footerBytes));
        out.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footerBytes.length).flip());
        out.write(ByteBuffer.wrap(MAGIC));
    }

    /// Assembles this build's `created_by` identifier from [BuildInfo].
    private static String defaultCreatedBy() {
        return "hardwood version " + BuildInfo.version() + " (build " + BuildInfo.revisionWithDirtyMark() + ")";
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

    /// Rejects a write after a previous write has failed. Separate from [#ensureOpen()] so
    /// that metadata setters — [#keyValueMetadata], [#createdBy] — remain usable after a
    /// failure; only further writes are blocked.
    private void ensureNotFailed() {
        if (failed) {
            throw new IllegalStateException(
                    "Writer has failed; it cannot accept more data and will discard on close");
        }
    }
}
