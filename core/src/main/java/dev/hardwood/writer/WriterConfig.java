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

import dev.hardwood.internal.compression.CodecLibraries;
import dev.hardwood.metadata.CompressionCodec;

/// Tuning knobs for [ParquetFileWriter].
///
/// Three size targets govern the writer's output granularity:
///
/// - **Page target** — the writer cuts a data page once the entries it holds would encode to
///   this many bytes. The page is cut *before* the entry that would cross it, so this is a
///   ceiling rather than something a page overshoots; only a single value larger than the whole
///   target can breach it, a value not being divisible across pages.
/// - **Row-group row target** — the writer cuts a row group once it holds this many records.
///   This is the control over how a file is banded, and it is exactly what it says: a row count
///   needs no estimate and does not vary with the data. It binds for narrow records.
/// - **Row-group buffer target** — the writer also cuts a row group once the bytes it holds for
///   that group reach this many: its level streams, dictionary indices, value stores and
///   dictionaries. This is the memory control, and it bounds a row group of records wider than
///   expected. A row group passes it by at most one record.
///
/// A row group is cut at whichever of the two row-group targets is reached first.
///
/// Neither of them is the size of what reaches the file. A row group is encoded and compressed
/// after it is buffered, and both steps shrink it by a factor of the data's own repetitiveness:
/// the same buffer target produces a row group a twentieth of its size on dictionary-friendly
/// data and most of its size on incompressible data. A caller who needs a particular on-disk
/// size measures one file and scales the setting; a caller who needs a particular banding sets
/// the row target. See [ParquetFileWriter] for what bounds memory.
///
/// How a column's values are stored is a [ColumnEncoding], set file-wide or per leaf column;
/// how the resulting page bodies are compressed is the [CompressionCodec]. Both are configured
/// here rather than on the schema.
///
/// A file's `created_by` identifier and its key-value metadata are set on [ParquetFileWriter].
///
/// Obtain the defaults with [#defaults] or override individual knobs through [#builder].
public final class WriterConfig {

    /// Default page target: 1 MiB of encoded values per data page.
    public static final int DEFAULT_PAGE_TARGET_BYTES = 1 << 20;

    /// Default row-group buffer target: 128 MiB of buffered values per row group.
    public static final long DEFAULT_ROW_GROUP_BUFFER_TARGET_BYTES = 128L << 20;

    /// Default row-group row target: 1,048,576 records.
    ///
    /// Both Arrow implementations cap a row group's records here — DuckDB caps lower, at 122,880
    /// — and on a flat three-column fixture the cap costs a quarter of a percent in file size for
    /// four times the banding. It binds wherever a record is narrower than the buffer target's share of it — for
    /// anything under about 128 bytes a record — and the buffer target takes over above that, so
    /// a narrow schema is banded by its record count and a wide one by what it holds.
    public static final long DEFAULT_ROW_GROUP_TARGET_ROWS = 1L << 20;

    /// Default statistics truncation length: `BYTE_ARRAY` `min` / `max` bounds longer than
    /// 64 bytes are truncated and flagged inexact.
    public static final int DEFAULT_STATISTICS_TRUNCATION_LENGTH = 64;

    /// Default page compression codec: `ZSTD` when the zstd-jni library is on the classpath,
    /// otherwise `UNCOMPRESSED`. Choosing a codec explicitly through [Builder#codec] still
    /// requires that codec's library and fails at writer creation when it is missing; this
    /// default only avoids imposing the ZSTD dependency on callers who did not ask to compress.
    public static final CompressionCodec DEFAULT_CODEC = defaultCodec();

    /// Default precision-loss policy: reject a value the column cannot hold exactly, rather
    /// than silently dropping the digits that do not fit.
    public static final PrecisionLossPolicy DEFAULT_PRECISION_LOSS_POLICY = PrecisionLossPolicy.REJECT;

    /// Default write-failure policy: discard the output when a write has failed, so `try-with-resources`
    /// is safe by default and a failure leaves nothing behind.
    public static final WriteFailurePolicy DEFAULT_WRITE_FAILURE_POLICY = WriteFailurePolicy.DISCARD;

    /// Default encoding policy: [ColumnEncoding#AUTO], leaving each column chunk's encoding to
    /// the size comparison the writer makes once the row group is buffered.
    public static final ColumnEncoding DEFAULT_ENCODING = ColumnEncoding.AUTO;

    private final int pageTargetBytes;
    private final long rowGroupBufferTargetBytes;
    private final long rowGroupTargetRows;
    private final ColumnEncoding defaultEncoding;
    private final Map<String, ColumnEncoding> columnEncodings;
    private final int statisticsTruncationLength;
    private final CompressionCodec codec;
    private final PrecisionLossPolicy precisionLossPolicy;
    private final WriteFailurePolicy writeFailurePolicy;

    private WriterConfig(Builder builder) {
        this.pageTargetBytes = builder.pageTargetBytes;
        this.rowGroupBufferTargetBytes = builder.rowGroupBufferTargetBytes;
        this.rowGroupTargetRows = builder.rowGroupTargetRows;
        this.defaultEncoding = builder.defaultEncoding;
        this.columnEncodings = Map.copyOf(builder.columnEncodings);
        this.statisticsTruncationLength = builder.statisticsTruncationLength;
        this.codec = builder.codec;
        this.precisionLossPolicy = builder.precisionLossPolicy;
        this.writeFailurePolicy = builder.writeFailurePolicy;
    }

    /// The default configuration.
    public static WriterConfig defaults() {
        return builder().build();
    }

    /// A builder pre-populated with the defaults.
    public static Builder builder() {
        return new Builder();
    }

    /// Encoded-byte threshold at which a data page is cut.
    public int pageTargetBytes() {
        return pageTargetBytes;
    }

    /// Byte threshold at which a row group is cut, counted as the bytes the writer holds for it.
    public long rowGroupBufferTargetBytes() {
        return rowGroupBufferTargetBytes;
    }

    /// Record count at which a row group is cut.
    public long rowGroupTargetRows() {
        return rowGroupTargetRows;
    }

    /// The encoding policy for columns with no override of their own.
    public ColumnEncoding defaultEncoding() {
        return defaultEncoding;
    }

    /// The per-column encoding policies, keyed by dotted leaf path. Unmodifiable, and empty
    /// where no column was named.
    public Map<String, ColumnEncoding> columnEncodings() {
        return columnEncodings;
    }

    /// The policy governing `columnPath`: its own override where it has one, the file-wide
    /// default otherwise. Package-private — [ParquetFileWriter] resolves every column through
    /// it at creation, and a caller has [#defaultEncoding] and [#columnEncodings] to read.
    ///
    /// @param columnPath the column's dotted leaf path
    /// @return the policy in force for that column
    ColumnEncoding encodingFor(String columnPath) {
        return columnEncodings.getOrDefault(columnPath, defaultEncoding);
    }

    /// The maximum length of a `BYTE_ARRAY` `min` / `max` statistics bound before it is
    /// truncated (and flagged inexact).
    public int statisticsTruncationLength() {
        return statisticsTruncationLength;
    }

    /// The codec each page body is compressed with.
    public CompressionCodec codec() {
        return codec;
    }

    /// What [RowWriter] does with a value carrying more precision than its column can hold.
    public PrecisionLossPolicy precisionLossPolicy() {
        return precisionLossPolicy;
    }

    /// What [ParquetFileWriter#close()] does when a write has failed: discard the output
    /// (the default) or commit the successfully-written prefix.
    public WriteFailurePolicy writeFailurePolicy() {
        return writeFailurePolicy;
    }

    /// `ZSTD` when its library is loadable, otherwise `UNCOMPRESSED`. The class is only probed
    /// for presence, not initialized, so picking the default never triggers the native load.
    private static CompressionCodec defaultCodec() {
        return CodecLibraries.isPresent("com.github.luben.zstd.Zstd")
                ? CompressionCodec.ZSTD
                : CompressionCodec.UNCOMPRESSED;
    }

    /// Builder for [WriterConfig].
    public static final class Builder {

        private int pageTargetBytes = DEFAULT_PAGE_TARGET_BYTES;
        private long rowGroupBufferTargetBytes = DEFAULT_ROW_GROUP_BUFFER_TARGET_BYTES;
        private long rowGroupTargetRows = DEFAULT_ROW_GROUP_TARGET_ROWS;
        private ColumnEncoding defaultEncoding = DEFAULT_ENCODING;
        private final Map<String, ColumnEncoding> columnEncodings = new LinkedHashMap<>();
        private int statisticsTruncationLength = DEFAULT_STATISTICS_TRUNCATION_LENGTH;
        private CompressionCodec codec = DEFAULT_CODEC;
        private PrecisionLossPolicy precisionLossPolicy = DEFAULT_PRECISION_LOSS_POLICY;
        private WriteFailurePolicy writeFailurePolicy = DEFAULT_WRITE_FAILURE_POLICY;

        private Builder() {
        }

        /// Sets the page target; must be at least one `INT32` (4 bytes).
        public Builder pageTargetBytes(int pageTargetBytes) {
            if (pageTargetBytes < Integer.BYTES) {
                throw new IllegalArgumentException(
                        "pageTargetBytes must be at least " + Integer.BYTES + " but was " + pageTargetBytes);
            }
            this.pageTargetBytes = pageTargetBytes;
            return this;
        }

        /// Sets the byte threshold at which a row group is cut; must be positive.
        ///
        /// The bytes counted are the bytes the writer holds for the open row group: the level
        /// streams, the dictionary indices, the value stores and the dictionaries. This is the
        /// writer's memory control, and peak heap follows it. Two things sit on top: the buffers
        /// hold more than they are charged for while they grow — the value stores by half again,
        /// the level streams and packed content and every dictionary's arrays by double — and a
        /// schema with enough columns that each one's share falls below the floor under a
        /// column's buffers opens at a multiple of the target. Neither grows with how much is
        /// written.
        ///
        /// It is not the size the row group takes on disk. That is smaller by whatever the
        /// encoding and the codec win, which is a property of the data rather than of this
        /// setting. A row group passes this threshold by at most one record, a record not being
        /// divisible across row groups.
        public Builder rowGroupBufferTargetBytes(long rowGroupBufferTargetBytes) {
            if (rowGroupBufferTargetBytes <= 0) {
                throw new IllegalArgumentException(
                        "rowGroupBufferTargetBytes must be positive but was " + rowGroupBufferTargetBytes);
            }
            this.rowGroupBufferTargetBytes = rowGroupBufferTargetBytes;
            return this;
        }

        /// Sets the record count at which a row group is cut; must be positive.
        ///
        /// This is the control over how a file is banded, and the row groups it produces hold
        /// exactly this many records apart from the last. A row group is cut at this count or at
        /// [#rowGroupBufferTargetBytes], whichever is reached first, so a row target set above
        /// what the buffer target allows has no effect beyond it.
        ///
        /// A structural ceiling sits under both: a chunk accumulates into `int`-indexed buffers,
        /// so a row group holds at most `Integer.MAX_VALUE - 8` records however many are asked
        /// for. A target above that is that ceiling, which is what makes `Long.MAX_VALUE` the way
        /// to say "no row limit, cut on bytes alone".
        public Builder rowGroupTargetRows(long rowGroupTargetRows) {
            if (rowGroupTargetRows <= 0) {
                throw new IllegalArgumentException(
                        "rowGroupTargetRows must be positive but was " + rowGroupTargetRows);
            }
            this.rowGroupTargetRows = rowGroupTargetRows;
            return this;
        }

        /// Sets the encoding policy for every column without an override of its own; must be
        /// non-null. Defaults to [ColumnEncoding#AUTO].
        ///
        /// A default that no column of the schema can carry is rejected when the writer is
        /// created, so a file-wide `BYTE_STREAM_SPLIT` over a schema holding a `BYTE_ARRAY`
        /// column fails rather than quietly resolving that column to something else.
        public Builder encoding(ColumnEncoding encoding) {
            if (encoding == null) {
                throw new IllegalArgumentException("encoding must not be null");
            }
            this.defaultEncoding = encoding;
            return this;
        }

        /// Sets the encoding policy for one leaf column, overriding the file-wide default; both
        /// arguments must be non-null.
        ///
        /// The column is named by its **dotted leaf path** as the schema spells it, synthetic
        /// `list.element` and `key_value.key` segments included — `readings.list.element`, not
        /// `readings`. A path matching no leaf column of the schema, or a policy its physical
        /// type cannot carry, is rejected when the writer is created.
        public Builder encoding(String columnPath, ColumnEncoding encoding) {
            if (columnPath == null) {
                throw new IllegalArgumentException("columnPath must not be null");
            }
            if (encoding == null) {
                throw new IllegalArgumentException("encoding must not be null for column " + columnPath);
            }
            this.columnEncodings.put(columnPath, encoding);
            return this;
        }

        /// Sets the maximum `BYTE_ARRAY` `min` / `max` statistics bound length; must be
        /// positive. A bound longer than this is truncated and flagged inexact.
        public Builder statisticsTruncationLength(int statisticsTruncationLength) {
            if (statisticsTruncationLength <= 0) {
                throw new IllegalArgumentException("statisticsTruncationLength must be positive but was "
                        + statisticsTruncationLength);
            }
            this.statisticsTruncationLength = statisticsTruncationLength;
            return this;
        }

        /// Sets the codec each page body is compressed with; must be non-null.
        ///
        /// `UNCOMPRESSED`, `GZIP`, `SNAPPY`, `ZSTD`, `LZ4_RAW` and `BROTLI` are written.
        /// Everything but the first two needs its library on the classpath, which is checked
        /// when the writer is created rather than here.
        ///
        /// The other two members of [CompressionCodec] are not produced, and neither is
        /// waiting on a later release. `LZ4` names the Hadoop framing the format deprecated in
        /// favour of `LZ4_RAW`; files already written with it are still read, so the refusal is
        /// on this side only. `LZO` has no maintained JVM implementation and is refused in both
        /// directions. Asking for either fails when the writer is created.
        public Builder codec(CompressionCodec codec) {
            if (codec == null) {
                throw new IllegalArgumentException("codec must not be null");
            }
            this.codec = codec;
            return this;
        }

        /// Sets what [RowWriter] does with a value carrying more precision than its column
        /// can hold — an [java.time.Instant] with microseconds written to a
        /// `TIMESTAMP(MILLIS)` column, say; must be non-null. Defaults to
        /// [PrecisionLossPolicy#REJECT].
        public Builder precisionLossPolicy(PrecisionLossPolicy precisionLossPolicy) {
            if (precisionLossPolicy == null) {
                throw new IllegalArgumentException("precisionLossPolicy must not be null");
            }
            this.precisionLossPolicy = precisionLossPolicy;
            return this;
        }

        /// Sets what [ParquetFileWriter#close()] does when a write has failed; must be non‑null.
        /// Defaults to [WriteFailurePolicy#DISCARD].
        ///
        /// Under [WriteFailurePolicy#DISCARD], a writer that has thrown once refuses to publish:
        /// `close()` discards the output silently, and `try‑with‑resources` is safe by default.
        /// Under [WriteFailurePolicy#COMMIT_PREFIX], `close()` writes a valid footer over the rows
        /// that were flushed before the failure.
        public Builder writeFailurePolicy(WriteFailurePolicy policy) {
            if (policy == null) {
                throw new IllegalArgumentException("writeFailurePolicy must not be null");
            }
            this.writeFailurePolicy = policy;
            return this;
        }

        /// Builds the immutable configuration.
        public WriterConfig build() {
            return new WriterConfig(this);
        }
    }
}
