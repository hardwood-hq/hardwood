/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dev.hardwood.internal.compression.CodecLibraries;
import dev.hardwood.metadata.SortingColumn;

/// Tuning knobs for [ParquetFileWriter].
///
/// Four targets govern the writer's output granularity:
///
/// - **Page byte target** — the writer cuts a data page once the records it holds would encode
///   to this many bytes. The page is cut *before* the record that would cross it, so this is a
///   ceiling rather than something a page overshoots; only a single record larger than the whole
///   target can breach it, a page holding whole records.
/// - **Page row target** — the writer also cuts a data page once it holds this many records,
///   which sets how finely the page index lets a reader skip within a column chunk.
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
/// how the resulting page bodies are compressed is a [Compression]. Both are configured
/// here rather than on the schema.
///
/// A column can carry a Bloom filter in every row group, which lets a reader skip a row group
/// that does not hold a value an equality or `IN` predicate asks for, where the row group's
/// `min` / `max` span it. No column has one unless named through [Builder#bloomFilter(String)].
///
/// Rows written already sorted can be declared so through [Builder#sortingColumns(SortingColumn...)]:
/// every row group records the order in its metadata, where a reader can find it, and the
/// writer fails the write at the first row out of that order.
///
/// A file's `created_by` identifier and its key-value metadata are set on [ParquetFileWriter].
///
/// Obtain the defaults with [#defaults] or override individual knobs through [#builder].
public final class WriterConfig {

    /// Default page target: 1 MiB of encoded values per data page.
    public static final int DEFAULT_PAGE_TARGET_BYTES = 1 << 20;

    /// Default page row target: 20,000 records per data page.
    public static final int DEFAULT_PAGE_TARGET_ROWS = 20_000;

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

    /// Default page compression: `ZSTD` at its default level when the zstd-jni library is on the
    /// classpath, otherwise `UNCOMPRESSED`. Choosing a codec explicitly through
    /// [Builder#compression] still requires that codec's library and fails at writer creation
    /// when it is missing; this default only avoids imposing the ZSTD dependency on callers who
    /// did not ask to compress.
    public static final Compression DEFAULT_COMPRESSION = defaultCompression();

    /// Default precision-loss policy: reject a value the column cannot hold exactly, rather
    /// than silently dropping the digits that do not fit.
    public static final PrecisionLossPolicy DEFAULT_PRECISION_LOSS_POLICY = PrecisionLossPolicy.REJECT;

    /// Default encoding policy: [ColumnEncoding#AUTO], leaving each column chunk's encoding to
    /// the size comparison the writer makes once the row group is buffered.
    public static final ColumnEncoding DEFAULT_ENCODING = ColumnEncoding.AUTO;

    /// Default false-positive probability of a column's Bloom filter: 1%.
    public static final double DEFAULT_BLOOM_FILTER_FPP = 0.01;

    private final int pageTargetBytes;
    private final int pageTargetRows;
    private final long rowGroupBufferTargetBytes;
    private final long rowGroupTargetRows;
    private final ColumnEncoding defaultEncoding;
    private final Map<String, ColumnEncoding> columnEncodings;
    private final Map<String, Double> bloomFilters;
    private final int statisticsTruncationLength;
    private final Compression compression;
    private final PrecisionLossPolicy precisionLossPolicy;
    private final List<SortingColumn> sortingColumns;

    private WriterConfig(Builder builder) {
        this.pageTargetBytes = builder.pageTargetBytes;
        this.pageTargetRows = builder.pageTargetRows;
        this.rowGroupBufferTargetBytes = builder.rowGroupBufferTargetBytes;
        this.rowGroupTargetRows = builder.rowGroupTargetRows;
        this.defaultEncoding = builder.defaultEncoding;
        this.columnEncodings = Map.copyOf(builder.columnEncodings);
        this.bloomFilters = Map.copyOf(builder.bloomFilters);
        this.statisticsTruncationLength = builder.statisticsTruncationLength;
        this.compression = builder.compression;
        this.precisionLossPolicy = builder.precisionLossPolicy;
        this.sortingColumns = builder.sortingColumns;
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

    /// Record count at which a data page is cut.
    public int pageTargetRows() {
        return pageTargetRows;
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

    /// The columns that carry a Bloom filter, keyed by dotted leaf path, each with its filter's
    /// false-positive probability. Unmodifiable, and empty where no column was named.
    public Map<String, Double> bloomFilters() {
        return bloomFilters;
    }

    /// The maximum length of a `BYTE_ARRAY` `min` / `max` statistics bound before it is
    /// truncated (and flagged inexact).
    public int statisticsTruncationLength() {
        return statisticsTruncationLength;
    }

    /// How each page body is compressed.
    public Compression compression() {
        return compression;
    }

    /// What [RowWriter] does with a value carrying more precision than its column can hold.
    public PrecisionLossPolicy precisionLossPolicy() {
        return precisionLossPolicy;
    }

    /// The columns every row group is declared sorted by, in order; empty when no order is
    /// declared.
    public List<SortingColumn> sortingColumns() {
        return sortingColumns;
    }

    /// `ZSTD` when its library is loadable, otherwise `UNCOMPRESSED`. The class is only probed
    /// for presence, not initialized, so picking the default never triggers the native load.
    private static Compression defaultCompression() {
        return CodecLibraries.Library.ZSTD.isPresent()
                ? Compression.zstd()
                : Compression.uncompressed();
    }

    /// Builder for [WriterConfig].
    public static final class Builder {

        private int pageTargetBytes = DEFAULT_PAGE_TARGET_BYTES;
        private int pageTargetRows = DEFAULT_PAGE_TARGET_ROWS;
        private long rowGroupBufferTargetBytes = DEFAULT_ROW_GROUP_BUFFER_TARGET_BYTES;
        private long rowGroupTargetRows = DEFAULT_ROW_GROUP_TARGET_ROWS;
        private ColumnEncoding defaultEncoding = DEFAULT_ENCODING;
        private final Map<String, ColumnEncoding> columnEncodings = new LinkedHashMap<>();
        private final Map<String, Double> bloomFilters = new LinkedHashMap<>();
        private int statisticsTruncationLength = DEFAULT_STATISTICS_TRUNCATION_LENGTH;
        private Compression compression = DEFAULT_COMPRESSION;
        private PrecisionLossPolicy precisionLossPolicy = DEFAULT_PRECISION_LOSS_POLICY;
        private List<SortingColumn> sortingColumns = List.of();

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

        /// Sets the record count at which a data page is cut; must be positive.
        ///
        /// A page is cut at this count or at [#pageTargetBytes], whichever is reached first.
        /// Each page's bounds are recorded in the file's page index, so the count sets how finely
        /// a reader can skip within a column chunk. It binds on columns whose values encode small,
        /// such as dictionary-encoded ones, which would otherwise hold a whole row group in a few
        /// pages. A page holds whole records, so on a repeated column it counts records rather
        /// than values.
        public Builder pageTargetRows(int pageTargetRows) {
            if (pageTargetRows <= 0) {
                throw new IllegalArgumentException("pageTargetRows must be positive but was " + pageTargetRows);
            }
            this.pageTargetRows = pageTargetRows;
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
        /// so a row group holds at most `Integer.MAX_VALUE - 9` records however many are asked
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

        /// Writes a Bloom filter for one leaf column, at [#DEFAULT_BLOOM_FILTER_FPP]; must be
        /// non-null. See [#bloomFilter(String, double)].
        public Builder bloomFilter(String columnPath) {
            return bloomFilter(columnPath, DEFAULT_BLOOM_FILTER_FPP);
        }

        /// Writes a Bloom filter for one leaf column, with the given false-positive probability;
        /// the path must be non-null and the probability strictly between 0 and 1.
        ///
        /// Every row group gets a filter for the column, holding each distinct value its chunk
        /// holds. The filter is sized from the values the chunk turns out to hold, so a lower
        /// probability costs bytes in proportion to the chunk's distinct values. Naming the
        /// column again replaces the probability.
        ///
        /// The column is named by its dotted leaf path, as for [#encoding(String, ColumnEncoding)].
        /// A path matching no leaf column of the schema, or naming a `BOOLEAN` column, is
        /// rejected when the writer is created.
        public Builder bloomFilter(String columnPath, double fpp) {
            if (columnPath == null) {
                throw new IllegalArgumentException("columnPath must not be null");
            }
            if (!(fpp > 0 && fpp < 1)) {
                throw new IllegalArgumentException("Bloom filter false-positive probability must be between 0 and 1"
                        + " (exclusive) but was " + fpp + " for column " + columnPath);
            }
            this.bloomFilters.put(columnPath, fpp);
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

        /// Sets how each page body is compressed; must be non-null. See [Compression] for the
        /// codecs and their levels.
        ///
        /// A codec's library is required when the writer is created rather than here.
        public Builder compression(Compression compression) {
            if (compression == null) {
                throw new IllegalArgumentException("compression must not be null");
            }
            this.compression = compression;
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

        /// Declares the order every row group's rows are sorted in; a second call replaces the
        /// first, and no columns declares none. See
        /// [#sortingColumns(List)].
        public Builder sortingColumns(SortingColumn... sortingColumns) {
            if (sortingColumns == null) {
                throw new IllegalArgumentException("sortingColumns must not be null");
            }
            return sortingColumns(Arrays.asList(sortingColumns));
        }

        /// Declares the order every row group's rows are sorted in: by the first column, rows
        /// equal there by the second, and so on. A second call replaces the first, and an empty
        /// list declares none. The list must be non-null, hold no `null` and name each column
        /// once.
        ///
        /// Each row group's metadata records the order. The writer checks it as rows arrive,
        /// within each row group: the first row that sorts before the row ahead of it fails
        /// the write with an `IllegalArgumentException`. [SortingColumn] defines how each
        /// column's values compare.
        ///
        /// A column is named by its dotted leaf path, as for
        /// [#encoding(String, ColumnEncoding)]. A path matching no leaf column, a column that is
        /// repeated or sits inside a list or map, and a column whose type has no defined order
        /// (`INTERVAL`, `GEOMETRY`, `GEOGRAPHY`, `UNKNOWN`) are rejected when the
        /// writer is created.
        public Builder sortingColumns(List<SortingColumn> sortingColumns) {
            if (sortingColumns == null) {
                throw new IllegalArgumentException("sortingColumns must not be null");
            }
            Set<String> seen = new HashSet<>();
            for (SortingColumn column : sortingColumns) {
                if (column == null) {
                    throw new IllegalArgumentException("sortingColumns must not contain null: " + sortingColumns);
                }
                if (!seen.add(column.columnPath())) {
                    throw new IllegalArgumentException("Column '" + column.columnPath()
                            + "' is declared more than once among the sorting columns " + sortingColumns);
                }
            }
            this.sortingColumns = List.copyOf(sortingColumns);
            return this;
        }

        /// Builds the immutable configuration.
        public WriterConfig build() {
            return new WriterConfig(this);
        }
    }
}
