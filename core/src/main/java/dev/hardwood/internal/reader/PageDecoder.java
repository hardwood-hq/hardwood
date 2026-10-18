/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.encoding.AlpDecoder;
import dev.hardwood.internal.encoding.ByteStreamSplitDecoder;
import dev.hardwood.internal.encoding.DeltaBinaryPackedDecoder;
import dev.hardwood.internal.encoding.DeltaByteArrayDecoder;
import dev.hardwood.internal.encoding.DeltaLengthByteArrayDecoder;
import dev.hardwood.internal.encoding.PlainDecoder;
import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;
import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.jfr.PageDecodedEvent;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// Decoder for individual Parquet data pages.
///
/// This class provides page decoding via [#decodePage].
/// Page location is handled by the [FetchPlan] implementations, dictionary
/// parsing by [DictionaryParser].
public class PageDecoder {

    /// Reusable level-decoding buffers owned by one in-flight page slot.
    static final class LevelScratch {
        private int[] repetitionLevels;
        private int[] definitionLevels;

        int[] repetitionLevels(int size) {
            if (repetitionLevels == null || repetitionLevels.length < size) {
                repetitionLevels = new int[size];
            }
            return repetitionLevels;
        }

        int[] definitionLevels(int size) {
            if (definitionLevels == null || definitionLevels.length < size) {
                definitionLevels = new int[size];
            }
            return definitionLevels;
        }

        private int[] ids;

        int[] ids(int size) {
            if (ids == null || ids.length < size) {
                ids = new int[size];
            }
            return ids;
        }

        private int[] bytePrefix;

        int[] bytePrefix(int size) {
            if (bytePrefix == null || bytePrefix.length < size) {
                bytePrefix = new int[size];
            }
            return bytePrefix;
        }

        private int[] recordStarts;

        int[] recordStarts(int size) {
            if (recordStarts == null || recordStarts.length < size) {
                recordStarts = new int[size];
            }
            return recordStarts;
        }

        private byte[] bytes;
        private byte[] levelBytes;
        private int[] lengths;
        private byte[] valueBytes;
        private int[] keptDefinitionLevels;
        private int[] keptRepetitionLevels;

        /// The definition levels of a masked page's kept values.
        int[] keptDefinitionLevels(int size) {
            if (keptDefinitionLevels == null || keptDefinitionLevels.length < size) {
                keptDefinitionLevels = new int[size];
            }
            return keptDefinitionLevels;
        }

        /// The repetition levels of a masked page's kept values.
        int[] keptRepetitionLevels(int size) {
            if (keptRepetitionLevels == null || keptRepetitionLevels.length < size) {
                keptRepetitionLevels = new int[size];
            }
            return keptRepetitionLevels;
        }

        /// The lengths of a page's delta-encoded binary values.
        int[] lengths(int size) {
            if (lengths == null || lengths.length < size) {
                lengths = new int[size];
            }
            return lengths;
        }

        /// A buffer for values a page holds nowhere whole: `DELTA_BYTE_ARRAY`'s, reconstructed.
        byte[] valueBytes(int size) {
            if (valueBytes == null || valueBytes.length < size) {
                valueBytes = new byte[size];
            }
            return valueBytes;
        }

        /// A buffer for a `V2` page's level sections.
        byte[] levelBytes(int size) {
            if (levelBytes == null || levelBytes.length < size) {
                levelBytes = new byte[size];
            }
            return levelBytes;
        }

        /// A buffer for a page's decompressed bytes, which a page read in two passes keeps until
        /// its cells are filled.
        byte[] bytes(int size) {
            if (bytes == null || bytes.length < size) {
                bytes = new byte[size];
            }
            return bytes;
        }
    }

    /// A data page of a column decoded through levels, read in two passes: its levels decoded and
    /// its values left in its own decompressed bytes, which the fill reads in place.
    ///
    /// @param definitionLevels `null` when every value is present
    /// @param repetitionLevels `null` for a column without repetition
    /// @param flat the values of a fixed-width numeric column, `null` for a binary one
    /// @param binary the values of a binary column, `null` for a numeric one
    /// @param runs where a masked page's kept values lie in the page, its levels and size those of
    ///        the kept values only; `null` for an unmasked page
    record InPlaceNestedPage(int size, int[] definitionLevels, int[] repetitionLevels, int fixedListK,
                             FlatPageValues flat, BinaryPageValues binary, KeptRuns runs) {
    }

    private final ColumnMetaData columnMetaData;
    private final ColumnSchema column;
    private final DecompressorFactory decompressorFactory;

    /// Whether the fixed-size-list read fast path may engage for this column,
    /// resolved from the reader's [dev.hardwood.reader.ReaderConfig] option
    /// (default disabled).
    private final boolean fixedListFastPathEnabled;

    /// Constructor for page decoding, with the fixed-size-list fast path enabled.
    public PageDecoder(ColumnMetaData columnMetaData, ColumnSchema column, DecompressorFactory decompressorFactory) {
        this(columnMetaData, column, decompressorFactory, true);
    }

    /// Constructor for page decoding.
    ///
    /// @param columnMetaData metadata for the column
    /// @param column column schema
    /// @param decompressorFactory factory for creating decompressors
    /// @param fixedListFastPathEnabled whether the fixed-size-list fast path may engage
    public PageDecoder(ColumnMetaData columnMetaData, ColumnSchema column, DecompressorFactory decompressorFactory,
                       boolean fixedListFastPathEnabled) {
        this.columnMetaData = columnMetaData;
        this.column = column;
        this.decompressorFactory = decompressorFactory;
        this.fixedListFastPathEnabled = fixedListFastPathEnabled;
    }

    /// Checks if this PageDecoder is compatible with the given column metadata.
    /// Used for cross-file prefetching to determine if PageDecoder can be reused.
    ///
    /// @param otherMetaData the column metadata to check against
    /// @return true if compatible (same codec), false otherwise
    public boolean isCompatibleWith(ColumnMetaData otherMetaData) {
        return columnMetaData.codec() == otherMetaData.codec();
    }

    /// Gets the decompressor factory used by this PageDecoder.
    ///
    /// @return the decompressor factory
    public DecompressorFactory getDecompressorFactory() {
        return decompressorFactory;
    }

    /// Produces an all-null typed [Page] of the given size, without reading or
    /// decompressing any data. Used when inline page statistics have proven that
    /// no row in the page can match the active filter predicate — row alignment
    /// with sibling columns is preserved while decompression and value decoding
    /// are skipped entirely. The row-level filter drops the synthetic nulls via
    /// SQL three-valued logic (`null <op> x → unknown → non-match`).
    ///
    /// Only valid for columns where `maxDefinitionLevel > 0`; for required
    /// columns the caller must not skip the page.
    public Page nullPage(int numValues) {
        int maxDefLevel = column.maxDefinitionLevel();
        if (maxDefLevel == 0) {
            throw new IllegalStateException("Cannot create null placeholder page for required column '"
                    + column.name() + "' — maxDefinitionLevel is 0");
        }
        int[] definitionLevels = new int[numValues]; // zero-initialised → all null
        int[] repetitionLevels = column.maxRepetitionLevel() > 0 ? new int[numValues] : null;
        PhysicalType type = column.type();
        return switch (type) {
            case INT64 -> new Page.LongPage(new long[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case DOUBLE -> new Page.DoublePage(new double[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case INT32 -> new Page.IntPage(new int[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case FLOAT -> new Page.FloatPage(new float[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case BOOLEAN -> new Page.BooleanPage(new boolean[numValues], definitionLevels, repetitionLevels, maxDefLevel, numValues);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 ->
                    new Page.ByteArrayPage(new byte[numValues][], definitionLevels, repetitionLevels, maxDefLevel, numValues);
        };
    }

    /// Decode a single data page from a buffer.
    ///
    /// The buffer should contain the complete page including header.
    ///
    /// @param pageBuffer buffer containing just this page (header + data)
    /// @param dictionary dictionary for this page, or null if not dictionary-encoded
    /// @return decoded page
    public Page decodePage(ByteBuffer pageBuffer, Dictionary dictionary) {
        // Standalone callers have no reorder slot to reuse across, so a throwaway
        // scratch (allocated fresh here, buffers grown lazily) matches the old
        // per-call allocation while keeping the decode path free of null handling.
        return decodePage(pageBuffer, dictionary, new LevelScratch());
    }

    /// Decode a single data page, reusing the supplied slot-owned level scratch.
    /// `scratch` must not be null; standalone callers pass a throwaway instance via
    /// the two-argument overload.
    Page decodePage(ByteBuffer pageBuffer, Dictionary dictionary, LevelScratch scratch) {
        PageDecodedEvent event = new PageDecodedEvent();
        event.begin();

        // Parse page header directly from buffer
        ThriftCompactReader headerReader = new ThriftCompactReader(pageBuffer, 0);
        PageHeader pageHeader = PageHeaderReader.read(headerReader);
        int headerSize = headerReader.getBytesRead();

        // Slice the page data (avoids copying)
        int compressedSize = pageHeader.compressedPageSize();
        ByteBuffer pageData = pageBuffer.slice(headerSize, compressedSize);

        if (pageHeader.crc() != null) {
            CrcValidator.assertCorrectCrc(pageHeader.crc(), pageData);
        }

        Page result = switch (pageHeader.type()) {
            case DATA_PAGE -> {
                Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());
                int uncompressedSize = pageHeader.uncompressedPageSize();
                byte[] uncompressedData = decompressor.decompress(pageData, uncompressedSize);
                yield parseDataPage(pageHeader.dataPageHeader(), uncompressedData, uncompressedSize, dictionary,
                        scratch);
            }
            case DATA_PAGE_V2 -> {
                yield parseDataPageV2(pageHeader.dataPageHeaderV2(), pageData,
                        pageHeader.uncompressedPageSize(), dictionary, scratch);
            }
            default -> throw new ParquetReadException("Unexpected page type for single-page decode: " + pageHeader.type());
        };

        event.column = column.name();
        event.compressedSize = compressedSize;
        event.uncompressedSize = pageHeader.uncompressedPageSize();
        event.commit();

        return result;
    }

    /// Decodes a page of a column without repetition whose values a fill reads in place: a data
    /// page (`V1` or `V2`) of a fixed-width numeric column, `PLAIN` or dictionary-encoded. Any
    /// other page returns `null` before anything is decompressed, and is decoded by [#decodePage]
    /// instead.
    ///
    /// The returned values hold `scratch`'s definition levels and ids until the fill is done.
    FlatPageValues decodeFlat(ByteBuffer pageBuffer, Dictionary dictionary, LevelScratch scratch) {
        if (!isNumeric(column.type()) || column.maxRepetitionLevel() > 0) {
            return null;
        }
        InPlacePage page = readInPlace(pageBuffer, scratch, false);
        if (page == null) {
            return null;
        }
        FlatPageValues values = numericValues(page, dictionary, scratch);
        page.commit(column);
        return values;
    }

    /// Decodes a page of a binary column without repetition whose values a fill reads in place:
    /// a data page (`V1` or `V2`), `PLAIN` or dictionary-encoded. Any other page returns `null`
    /// before anything is decompressed, and is decoded by [#decodePage] instead.
    ///
    /// The returned values hold `scratch`'s definition levels, ids and byte prefix, and the
    /// decompressor's buffer, until the fill is done.
    BinaryPageValues decodeFlatBinary(ByteBuffer pageBuffer, Dictionary dictionary, LevelScratch scratch) {
        if (binaryWidth() < 0 || column.maxRepetitionLevel() > 0) {
            return null;
        }
        InPlacePage page = readInPlace(pageBuffer, scratch, false);
        if (page == null) {
            return null;
        }
        BinaryPageValues values = binaryValues(page, dictionary, scratch);
        page.commit(column);
        return values;
    }

    /// Decodes the levels of a data page (`V1` or `V2`) of a column decoded through levels,
    /// `PLAIN` or dictionary-encoded, of a fixed-width numeric or binary type, and leaves its
    /// values in place in `scratch`'s own byte buffer for a fill to read later
    /// ([InPlaceNestedPage]). A page the fixed-size-list fast path takes keeps no levels and
    /// carries its `k`. Any other page returns `null` before anything is decompressed, and is
    /// decoded by [#decodePage] instead.
    InPlaceNestedPage decodeNestedInPlace(ByteBuffer pageBuffer, Dictionary dictionary, LevelScratch scratch) {
        boolean numeric = isNumeric(column.type());
        if (!numeric && binaryWidth() < 0) {
            return null;
        }
        InPlacePage page = readInPlace(pageBuffer, scratch, true);
        if (page == null) {
            return null;
        }
        FlatPageValues flat = numeric ? numericValues(page, dictionary, scratch) : null;
        BinaryPageValues binary = numeric ? null : binaryValues(page, dictionary, scratch);
        page.commit(column);
        return new InPlaceNestedPage(page.numValues, page.definitionLevels, page.repetitionLevels,
                page.fixedListK, flat, binary, null);
    }

    /// Whether the column's values are read in place through [FlatPageValues]: fixed-width
    /// numbers, and booleans.
    /// The values of a null placeholder page, every one null, for a fill that reads them in place:
    /// all-zero definition (and repetition) levels in `scratch` and no value bytes. `null` for a
    /// binary column whose width is left to the decoder to refuse.
    ///
    /// @throws IllegalStateException for a required column, which a placeholder cannot stand in for
    InPlaceNestedPage nullsInPlace(int numValues, LevelScratch scratch) {
        int maxDefLevel = column.maxDefinitionLevel();
        if (maxDefLevel == 0) {
            throw new IllegalStateException("Cannot create null placeholder page for required column '"
                    + column.name() + "' — maxDefinitionLevel is 0");
        }
        boolean numeric = isNumeric(column.type());
        if (!numeric && binaryWidth() < 0) {
            return null;
        }
        int[] definitionLevels = scratch.definitionLevels(numValues);
        Arrays.fill(definitionLevels, 0, numValues, 0);
        int[] repetitionLevels = null;
        if (column.maxRepetitionLevel() > 0) {
            repetitionLevels = scratch.repetitionLevels(numValues);
            Arrays.fill(repetitionLevels, 0, numValues, 0);
        }
        FlatPageValues flat = numeric
                ? FlatPageValues.plain(column.type(), definitionLevels, maxDefLevel, numValues, NO_BYTES, 0, 0)
                : null;
        BinaryPageValues binary = numeric
                ? null
                : BinaryPageValues.plain(column.type(), binaryWidth(), definitionLevels, maxDefLevel, numValues,
                        NO_BYTES, 0, 0, scratch);
        return new InPlaceNestedPage(numValues, definitionLevels, repetitionLevels, 0, flat, binary, null);
    }

    private static final byte[] NO_BYTES = new byte[0];

    private static boolean isNumeric(PhysicalType type) {
        return type == PhysicalType.INT32 || type == PhysicalType.INT64
                || type == PhysicalType.FLOAT || type == PhysicalType.DOUBLE || type == PhysicalType.BOOLEAN;
    }

    /// The width of the column's plain binary values: `0` for length-prefixed values, `-1` for a
    /// column that is not binary or whose width is left to the decoder to refuse.
    private int binaryWidth() {
        Integer typeLength = column.typeLength();
        return switch (column.type()) {
            case BYTE_ARRAY -> 0;
            case INT96 -> 12;
            case FIXED_LEN_BYTE_ARRAY -> typeLength != null && typeLength > 0 ? typeLength : -1;
            default -> -1;
        };
    }

    private FlatPageValues numericValues(InPlacePage page, Dictionary dictionary, LevelScratch scratch) {
        int maxDefLevel = column.maxDefinitionLevel();
        if (page.encoding == Encoding.RLE) {
            int idCount = FlatPageValues.countNonNull(page.definitionLevels, maxDefLevel, page.numValues);
            int rleLength = readSectionLength(page.data, page.valuesOffset, page.limit, "RLE boolean values");
            int[] ids = scratch.ids(idCount);
            new RleBitPackingHybridDecoder(page.data, page.valuesOffset + 4, rleLength, 1).readInts(ids, 0, idCount);
            return FlatPageValues.rleBooleans(page.numValues, page.definitionLevels, maxDefLevel, ids, idCount);
        }
        if (page.encoding == Encoding.DELTA_BINARY_PACKED) {
            return FlatPageValues.delta(column.type(), page.definitionLevels, maxDefLevel, page.numValues, page.data,
                    page.valuesOffset, page.limit);
        }
        if (page.encoding == Encoding.ALP) {
            return FlatPageValues.alp(column.type(), page.definitionLevels, maxDefLevel, page.numValues, page.data,
                    page.valuesOffset, page.limit);
        }
        if (page.encoding == Encoding.BYTE_STREAM_SPLIT) {
            return FlatPageValues.byteStreamSplit(column.type(), page.definitionLevels, maxDefLevel, page.numValues,
                    page.data, page.valuesOffset, page.limit);
        }
        if (page.encoding == Encoding.PLAIN) {
            return FlatPageValues.plain(column.type(), page.definitionLevels, maxDefLevel, page.numValues, page.data,
                    page.valuesOffset, page.limit);
        }
        int idCount = FlatPageValues.countNonNull(page.definitionLevels, maxDefLevel, page.numValues);
        int[] ids = decodeIds(page.encoding, page.data, page.valuesOffset, page.limit, dictionary, idCount, scratch);
        return FlatPageValues.dictionary(column.type(), page.numValues, page.definitionLevels, maxDefLevel, ids,
                idCount, dictionary);
    }

    private BinaryPageValues binaryValues(InPlacePage page, Dictionary dictionary, LevelScratch scratch) {
        int maxDefLevel = column.maxDefinitionLevel();
        if (page.encoding == Encoding.BYTE_STREAM_SPLIT) {
            return BinaryPageValues.byteStreamSplit(binaryWidth(), page.definitionLevels, maxDefLevel,
                    page.numValues, page.data, page.valuesOffset, page.limit, scratch);
        }
        if (page.encoding == Encoding.DELTA_LENGTH_BYTE_ARRAY) {
            return BinaryPageValues.deltaLength(page.definitionLevels, maxDefLevel, page.numValues, page.data,
                    page.valuesOffset, page.limit, scratch);
        }
        if (page.encoding == Encoding.DELTA_BYTE_ARRAY) {
            return BinaryPageValues.deltaByteArray(page.definitionLevels, maxDefLevel, page.numValues, page.data,
                    page.valuesOffset, page.limit, scratch);
        }
        if (page.encoding == Encoding.PLAIN) {
            return BinaryPageValues.plain(column.type(), binaryWidth(), page.definitionLevels, maxDefLevel,
                    page.numValues, page.data, page.valuesOffset, page.limit, scratch);
        }
        int idCount = FlatPageValues.countNonNull(page.definitionLevels, maxDefLevel, page.numValues);
        int[] ids = decodeIds(page.encoding, page.data, page.valuesOffset, page.limit, dictionary, idCount, scratch);
        if (!(dictionary instanceof Dictionary.ByteArrayDictionary binaryDictionary)) {
            throw new ParquetReadException("A " + column.type() + " page refers to a dictionary of another type");
        }
        return BinaryPageValues.ids(page.definitionLevels, maxDefLevel, page.numValues, ids, idCount,
                binaryDictionary);
    }

    /// A data page read for a fill that reads its values in place: its values `[valuesOffset,
    /// limit)` of `data`, its decoded levels, and the decode event that spans it.
    ///
    /// @param definitionLevels `null` when every value is present, and for a fixed-size-list page
    /// @param repetitionLevels `null` for a column without repetition, and for a fixed-size-list page
    /// @param fixedListK the elements per record of a fixed-size-list page, `0` for any other page
    private record InPlacePage(Encoding encoding, byte[] data, int limit, int valuesOffset, int numValues,
                               int[] definitionLevels, int[] repetitionLevels, int fixedListK, int compressedSize,
                               int uncompressedSize, PageDecodedEvent event) {

        void commit(ColumnSchema column) {
            event.column = column.name();
            event.compressedSize = compressedSize;
            event.uncompressedSize = uncompressedSize;
            event.commit();
        }
    }

    /// Reads the header of a page a fill may read in place, and decodes its levels when it is one:
    /// a `V1` or `V2` data page, `PLAIN` or dictionary-encoded, with `RLE` levels. Returns `null`
    /// for any other page before decompressing it.
    ///
    /// @param ownBytes whether the values must outlive the task: they are then decompressed or
    ///        copied into `scratch`'s own buffer rather than the decompressor's
    private InPlacePage readInPlace(ByteBuffer pageBuffer, LevelScratch scratch, boolean ownBytes) {
        ThriftCompactReader headerReader = new ThriftCompactReader(pageBuffer, 0);
        PageHeader pageHeader = PageHeaderReader.read(headerReader);
        Encoding encoding;
        if (pageHeader.type() == PageType.DATA_PAGE) {
            encoding = pageHeader.dataPageHeader().encoding();
        }
        else if (pageHeader.type() == PageType.DATA_PAGE_V2) {
            encoding = pageHeader.dataPageHeaderV2().encoding();
        }
        else {
            return null;
        }
        if (!readsInPlace(encoding)) {
            return null;
        }

        PageDecodedEvent event = new PageDecodedEvent();
        event.begin();
        int headerSize = headerReader.getBytesRead();
        int compressedSize = pageHeader.compressedPageSize();
        ByteBuffer pageData = pageBuffer.slice(headerSize, compressedSize);
        if (pageHeader.crc() != null) {
            CrcValidator.assertCorrectCrc(pageHeader.crc(), pageData);
        }
        int uncompressedSize = pageHeader.uncompressedPageSize();
        return pageHeader.type() == PageType.DATA_PAGE
                ? readV1InPlace(pageHeader.dataPageHeader(), pageData, uncompressedSize, compressedSize, event,
                        scratch, ownBytes)
                : readV2InPlace(pageHeader.dataPageHeaderV2(), pageData, uncompressedSize, compressedSize, event,
                        scratch, ownBytes);
    }

    private InPlacePage readV1InPlace(DataPageHeader header, ByteBuffer pageData, int limit, int compressedSize,
                                      PageDecodedEvent event, LevelScratch scratch, boolean ownBytes) {
        Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());
        byte[] data = ownBytes
                ? decompressor.decompress(pageData, limit, scratch.bytes(limit))
                : decompressor.decompress(pageData, limit);
        int numValues = header.numValues();
        int offset = 0;
        int repOffset = 0;
        int repLength = 0;
        if (column.maxRepetitionLevel() > 0) {
            repLength = readSectionLength(data, offset, limit, "repetition level");
            repOffset = offset + 4;
            offset = repOffset + repLength;
        }
        int defOffset = 0;
        int defLength = 0;
        if (column.maxDefinitionLevel() > 0) {
            defLength = readSectionLength(data, offset, limit, "definition level");
            defOffset = offset + 4;
            offset = defOffset + defLength;
        }
        // The detector reads the RLE hybrid only; legacy BIT_PACKED levels keep their levels
        int k = header.repetitionLevelEncoding() == Encoding.RLE && header.definitionLevelEncoding() == Encoding.RLE
                ? fixedListK(data, repOffset, repLength, data, defOffset, defLength, numValues,
                        FixedSizeListDetector.ROWS_UNKNOWN)
                : 0;
        return levels(header.encoding(), data, limit, offset, numValues, data, repOffset, repLength, data,
                defOffset, defLength, k, compressedSize, limit, event, scratch);
    }

    private InPlacePage readV2InPlace(DataPageHeaderV2 header, ByteBuffer pageData, int uncompressedPageSize,
                                      int compressedSize, PageDecodedEvent event, LevelScratch scratch,
                                      boolean ownBytes) {
        int repLength = header.repetitionLevelsByteLength();
        int defLength = header.definitionLevelsByteLength();
        int valuesOffset = repLength + defLength;
        int storedValuesLength = pageData.remaining() - valuesOffset;
        int numValues = header.numValues();
        // The level sections are never compressed: they are decoded from the page's bytes
        byte[] levelData = scratch.levelBytes(valuesOffset);
        pageData.get(pageData.position(), levelData, 0, valuesOffset);
        int k = fixedListK(levelData, 0, repLength, levelData, repLength, defLength, numValues, header.numRows());

        byte[] data;
        int offset;
        int limit;
        if (isValueRegionCompressed(header, storedValuesLength)) {
            int valuesLength = uncompressedPageSize - repLength - defLength;
            ByteBuffer compressedValues = pageData.slice(valuesOffset, storedValuesLength);
            Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());
            data = ownBytes
                    ? decompressor.decompress(compressedValues, valuesLength, scratch.bytes(valuesLength))
                    : decompressor.decompress(compressedValues, valuesLength);
            offset = 0;
            limit = valuesLength;
        }
        else if (!ownBytes && pageData.hasArray()) {
            // Stored values are read where they lie, for a fill within the page's task
            data = pageData.array();
            offset = pageData.arrayOffset() + pageData.position() + valuesOffset;
            limit = offset + storedValuesLength;
        }
        else {
            data = scratch.bytes(storedValuesLength);
            pageData.get(pageData.position() + valuesOffset, data, 0, storedValuesLength);
            offset = 0;
            limit = storedValuesLength;
        }
        return levels(header.encoding(), data, limit, offset, numValues, levelData, 0, repLength, levelData,
                repLength, defLength, k, compressedSize, uncompressedPageSize, event, scratch);
    }

    /// Whether a page of `encoding` of this column has an in-place reader: `PLAIN` for every type,
    /// dictionary ids for every type but booleans, `BYTE_STREAM_SPLIT` for fixed-width numbers,
    /// `ALP` for floating-point numbers, `DELTA_BINARY_PACKED` for integers, the binary deltas for
    /// byte arrays, `RLE` for booleans.
    private boolean readsInPlace(Encoding encoding) {
        PhysicalType type = column.type();
        return switch (encoding) {
            case PLAIN -> true;
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> type != PhysicalType.BOOLEAN;
            case BYTE_STREAM_SPLIT -> type == PhysicalType.INT32 || type == PhysicalType.INT64
                    || type == PhysicalType.FLOAT || type == PhysicalType.DOUBLE
                    || (type == PhysicalType.FIXED_LEN_BYTE_ARRAY && binaryWidth() > 0);
            case ALP -> type == PhysicalType.FLOAT || type == PhysicalType.DOUBLE;
            case DELTA_BINARY_PACKED -> type == PhysicalType.INT32 || type == PhysicalType.INT64;
            case DELTA_LENGTH_BYTE_ARRAY -> type == PhysicalType.BYTE_ARRAY;
            case DELTA_BYTE_ARRAY -> type == PhysicalType.BYTE_ARRAY || type == PhysicalType.FIXED_LEN_BYTE_ARRAY;
            case RLE -> type == PhysicalType.BOOLEAN;
            default -> false;
        };
    }

    /// The `k` of a page the fixed-size-list fast path takes, `0` for any other page.
    private int fixedListK(byte[] repData, int repOffset, int repLength, byte[] defData, int defOffset,
                           int defLength, int numValues, int numRows) {
        if (!fixedListFastPathEnabled || !isFixedListElementSupported() || !hasFixedListLevelShape()
                || repLength == 0 || defLength == 0) {
            return 0;
        }
        FixedSizeListShape shape = FixedSizeListDetector.detect(repData, repOffset, repLength, defData, defOffset,
                defLength, numValues, numRows, column.maxRepetitionLevel(), column.maxDefinitionLevel());
        return shape instanceof FixedSizeListShape.FixedWidth(int k) ? k : 0;
    }

    /// Decodes a page's levels into `scratch`, none for a fixed-size-list page.
    private InPlacePage levels(Encoding encoding, byte[] data, int limit, int valuesOffset, int numValues,
                               byte[] repData, int repOffset, int repLength, byte[] defData, int defOffset,
                               int defLength, int k, int compressedSize, int uncompressedSize,
                               PageDecodedEvent event, LevelScratch scratch) {
        int[] repetitionLevels = null;
        int[] definitionLevels = null;
        if (k == 0) {
            if (column.maxRepetitionLevel() > 0) {
                repetitionLevels = decodeRepetitionLevels(repData, repOffset, repLength, numValues,
                        column.maxRepetitionLevel(), scratch);
            }
            if (column.maxDefinitionLevel() > 0) {
                definitionLevels = decodeDefinitionLevels(defData, defOffset, defLength, numValues, scratch);
            }
        }
        return new InPlacePage(encoding, data, limit, valuesOffset, numValues, definitionLevels, repetitionLevels,
                k, compressedSize, uncompressedSize, event);
    }

    /// Decodes the `idCount` dictionary ids of a dictionary-encoded `V1` page's value section
    /// `[offset, limit)` of `data` into `scratch`.
    private static int[] decodeIds(Encoding encoding, byte[] data, int offset, int limit, Dictionary dictionary,
                                   int idCount, LevelScratch scratch) {
        if (dictionary == null) {
            throw new ParquetReadException("Dictionary page not found for " + encoding + " encoding");
        }
        if (offset >= limit) {
            throw new ParquetReadException("Unexpected EOF reading the dictionary index bit width");
        }
        int bitWidth = data[offset++] & 0xFF;
        if (bitWidth > 32) {
            throw new ParquetReadException("Invalid dictionary index bit width: " + bitWidth
                    + ". Must be between 0 and 32");
        }
        int[] ids = scratch.ids(idCount);
        if (bitWidth == 0) {
            Arrays.fill(ids, 0, idCount, 0);
        }
        else {
            new RleBitPackingHybridDecoder(data, offset, limit - offset, bitWidth).readInts(ids, 0, idCount);
        }
        return ids;
    }

    /// Decode levels using RLE/Bit-Packing Hybrid encoding.
    private int[] decodeRepetitionLevels(byte[] levelData, int offset, int length, int numValues, int maxLevel,
            LevelScratch scratch) {
        int[] levels = scratch.repetitionLevels(numValues);
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(levelData, offset, length, getBitWidth(maxLevel));
        decoder.readInts(levels, 0, numValues);
        return levels;
    }

    /// Decode definition levels, applying the all-present fast path: when the
    /// stream is a single RLE run of `maxDef` (the common case for an optional
    /// but fully-populated column), skip materializing the per-value level array
    /// and represent "all present" as a `null` level array — the same
    /// representation used for required columns throughout the reader.
    ///
    /// Applies to flat and nested columns alike: nested assembly treats a `null`
    /// definition-level array as all-present (every leaf at `maxDef`) and takes a
    /// whole-page bulk-copy path; the repetition levels are still materialised, so
    /// record boundaries are preserved.
    private int[] decodeDefinitionLevels(byte[] levelData, int offset, int length, int numValues,
            LevelScratch scratch) {
        int maxDef = column.maxDefinitionLevel();
        int bitWidth = getBitWidth(maxDef);
        RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(levelData, offset, length, bitWidth);
        // The probe loads the first run; if it is not the all-present fast path,
        // readInts below resumes from that same loaded run on the same instance.
        if (decoder.isSingleRleRunOf(maxDef, numValues)) {
            return null;
        }
        int[] levels = scratch.definitionLevels(numValues);
        decoder.readInts(levels, 0, numValues);
        return levels;
    }

    /// Count non-null values based on definition levels.
    private int countNonNullValues(int numValues, int[] definitionLevels) {
        if (definitionLevels == null) {
            return numValues;
        }
        int maxDefLevel = column.maxDefinitionLevel();
        int count = 0;
        for (int i = 0; i < numValues; i++) {
            if (definitionLevels[i] == maxDefLevel) {
                count++;
            }
        }
        return count;
    }

    private int getBitWidth(int maxValue) {
        if (maxValue == 0) {
            return 0;
        }
        return 32 - Integer.numberOfLeadingZeros(maxValue);
    }

    /// The fixed-size-list fast path is restricted to primitive numeric element
    /// types, which decode into contiguous primitive arrays the fixed-width
    /// assembly can bulk-copy. Byte-array-backed types (`BYTE_ARRAY`,
    /// `FIXED_LEN_BYTE_ARRAY`, `INT96`) take the regular path.
    private boolean isFixedListElementSupported() {
        return switch (column.type()) {
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE -> true;
            default -> false;
        };
    }

    /// Whether the column's level geometry is a single-level `LIST` the fast path
    /// can target: `maxRep == 1` with either an optional list (`maxDef == 2`) or a
    /// required list of required elements (`maxDef == 1`).
    ///
    /// The required case is admitted where the leaf sits under a repeated group, so
    /// its own repetition type is not `REPEATED`: an annotated `LIST` group, or a
    /// bare repeated group with a required primitive child. A bare unannotated
    /// `repeated <primitive>` shares the same `maxRep == 1` / `maxDef == 1` levels
    /// but is left to the regular path.
    private boolean hasFixedListLevelShape() {
        if (column.maxRepetitionLevel() != 1) {
            return false;
        }
        return switch (column.maxDefinitionLevel()) {
            case 2 -> true;
            case 1 -> column.repetitionType() != RepetitionType.REPEATED;
            default -> false;
        };
    }

    /// @param data the decompressed page body, valid up to `limit`; the buffer may run on past it
    ///        with bytes of an earlier page
    /// @param limit the length of the page body
    private Page parseDataPage(DataPageHeader header, byte[] data, int limit, Dictionary dictionary,
            LevelScratch scratch) {
        int numValues = header.numValues();
        int offset = 0;

        int repLevelLength = 0;
        int repLevelOffset = 0;
        if (column.maxRepetitionLevel() > 0) {
            repLevelLength = readSectionLength(data, offset, limit, "repetition level");
            offset += 4;
            repLevelOffset = offset;
            offset += repLevelLength;
        }

        int defLevelLength = 0;
        int defLevelOffset = 0;
        if (column.maxDefinitionLevel() > 0) {
            defLevelLength = readSectionLength(data, offset, limit, "definition level");
            offset += 4;
            defLevelOffset = offset;
            offset += defLevelLength;
        }
        int valuesOffset = offset;

        // Fixed-size-list fast path: the V1 header carries no num_rows, so the
        // detector derives it. The levels are inline and (unlike V2) always
        // present here in decompressed form; the detector only understands the
        // RLE hybrid, so legacy BIT_PACKED levels are left to the regular path.
        if (fixedListFastPathEnabled && isFixedListElementSupported()
                && hasFixedListLevelShape()
                && header.repetitionLevelEncoding() == Encoding.RLE
                && header.definitionLevelEncoding() == Encoding.RLE
                && repLevelLength > 0 && defLevelLength > 0) {
            FixedSizeListShape shape = FixedSizeListDetector.detect(
                    data, repLevelOffset, repLevelLength,
                    data, defLevelOffset, defLevelLength,
                    numValues, FixedSizeListDetector.ROWS_UNKNOWN,
                    column.maxRepetitionLevel(), column.maxDefinitionLevel());
            if (shape instanceof FixedSizeListShape.FixedWidth(int k)) {
                Page page = decodeTypedValues(
                        header.encoding(), header.encodingValue(),
                        data, valuesOffset, limit, numValues, null, null, dictionary);
                return Page.withFixedListK(page, k);
            }
        }

        int[] repetitionLevels = column.maxRepetitionLevel() > 0
                ? decodeRepetitionLevels(data, repLevelOffset, repLevelLength, numValues,
                        column.maxRepetitionLevel(), scratch)
                : null;
        int[] definitionLevels = column.maxDefinitionLevel() > 0
                ? decodeDefinitionLevels(data, defLevelOffset, defLevelLength, numValues, scratch)
                : null;

        return decodeTypedValues(
                header.encoding(), header.encodingValue(), data, valuesOffset, limit, numValues,
                definitionLevels, repetitionLevels, dictionary);
    }

    /// Reads the 4-byte little-endian length prefix of a section of `data` at `offset`, and checks
    /// that the section it announces ends within the page.
    private static int readSectionLength(byte[] data, int offset, int limit, String section) {
        if (Integer.BYTES > limit - offset) {
            throw new ParquetReadException("Unexpected EOF reading the " + section + " length");
        }
        int length = ByteBuffer.wrap(data, offset, Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN).getInt();
        int remaining = limit - offset - Integer.BYTES;
        if (length < 0 || length > remaining) {
            throw new ParquetReadException("Invalid " + section + " length " + length + ": "
                    + remaining + " bytes remain in the page");
        }
        return length;
    }

    private Page parseDataPageV2(DataPageHeaderV2 header, ByteBuffer pageData, int uncompressedPageSize,
            Dictionary dictionary, LevelScratch scratch) {
        int repLevelLen = header.repetitionLevelsByteLength();
        int defLevelLen = header.definitionLevelsByteLength();
        int valuesOffset = repLevelLen + defLevelLen;
        int compressedValuesLen = pageData.remaining() - valuesOffset;
        int numValues = header.numValues();
        int valuesLimit = isValueRegionCompressed(header, compressedValuesLen)
                ? uncompressedPageSize - repLevelLen - defLevelLen
                : compressedValuesLen;

        byte[] repLevelData = null;
        if (column.maxRepetitionLevel() > 0 && repLevelLen > 0) {
            repLevelData = new byte[repLevelLen];
            pageData.slice(0, repLevelLen).get(repLevelData);
        }

        byte[] defLevelData = null;
        if (column.maxDefinitionLevel() > 0 && defLevelLen > 0) {
            defLevelData = new byte[defLevelLen];
            pageData.slice(repLevelLen, defLevelLen).get(defLevelData);
        }

        // Fixed-size-list fast path: when the level streams prove every row is a
        // present list of exactly k elements, skip level materialization and
        // decode only the values, stamping the shape onto the page. The regular
        // value decoders already read densely from a null definition-level array
        // (the all-present convention), so no value-decode change is needed.
        if (fixedListFastPathEnabled && isFixedListElementSupported()
                && repLevelData != null && defLevelData != null
                && hasFixedListLevelShape()) {
            FixedSizeListShape shape = FixedSizeListDetector.detect(
                    repLevelData, 0, repLevelLen, defLevelData, 0, defLevelLen,
                    numValues, header.numRows(),
                    column.maxRepetitionLevel(), column.maxDefinitionLevel());
            if (shape instanceof FixedSizeListShape.FixedWidth(int k)) {
                byte[] valuesData = readValueRegion(header, pageData, uncompressedPageSize,
                        repLevelLen, defLevelLen, valuesOffset, compressedValuesLen);
                Page page = decodeTypedValues(
                        header.encoding(), header.encodingValue(),
                        valuesData, 0, valuesLimit, numValues, null, null, dictionary);
                return Page.withFixedListK(page, k);
            }
        }

        int[] repetitionLevels = repLevelData != null
                ? decodeRepetitionLevels(repLevelData, 0, repLevelLen, numValues,
                        column.maxRepetitionLevel(), scratch)
                : null;
        int[] definitionLevels = defLevelData != null
                ? decodeDefinitionLevels(defLevelData, 0, defLevelLen, numValues, scratch)
                : null;

        byte[] valuesData = readValueRegion(header, pageData, uncompressedPageSize,
                repLevelLen, defLevelLen, valuesOffset, compressedValuesLen);
        return decodeTypedValues(
                header.encoding(), header.encodingValue(), valuesData, 0, valuesLimit, numValues,
                definitionLevels, repetitionLevels, dictionary);
    }

    /// Whether the value region of a `DataPageV2` body is stored compressed.
    private static boolean isValueRegionCompressed(DataPageHeaderV2 header, int compressedValuesLen) {
        return header.isCompressed() && compressedValuesLen > 0;
    }

    /// Extracts the value region of a `DataPageV2` body, decompressing it when
    /// the page marks its values compressed. The level regions precede the
    /// values and are never compressed.
    ///
    /// A decompressed region is valid up to `uncompressedPageSize - repLevelLen - defLevelLen`
    /// only, the buffer it comes in running on past that with bytes of an earlier page; a stored
    /// one is exactly as long as the region.
    private byte[] readValueRegion(DataPageHeaderV2 header, ByteBuffer pageData, int uncompressedPageSize,
            int repLevelLen, int defLevelLen, int valuesOffset, int compressedValuesLen) {
        if (isValueRegionCompressed(header, compressedValuesLen)) {
            ByteBuffer compressedValues = pageData.slice(valuesOffset, compressedValuesLen);
            Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());
            int uncompressedValuesSize = uncompressedPageSize - repLevelLen - defLevelLen;
            return decompressor.decompress(compressedValues, uncompressedValuesSize);
        }
        byte[] valuesData = new byte[compressedValuesLen];
        pageData.slice(valuesOffset, compressedValuesLen).get(valuesData);
        return valuesData;
    }

    /// Decode values into Page using primitive arrays where possible.
    ///
    /// @param data the bytes holding the values, which may run on past `limit` with bytes of an
    ///        earlier page
    /// @param offset the position in `data` the values start at
    /// @param limit the position in `data` the values end at, exclusive
    private Page decodeTypedValues(Encoding encoding, int encodingValue, byte[] data, int offset, int limit,
                                   int numValues,
                                   int[] definitionLevels, int[] repetitionLevels,
                                   Dictionary dictionary) {
        int maxDefLevel = column.maxDefinitionLevel();
        PhysicalType type = column.type();

        // Try to decode into primitive arrays for supported type/encoding combinations
        return switch (encoding) {
            case PLAIN -> {
                PlainDecoder decoder = new PlainDecoder(data, offset, limit, type, column.typeLength());
                Page page = switch (type) {
                    case INT64 -> {
                        long[] values = new long[numValues];
                        decoder.readLongs(values, definitionLevels, maxDefLevel);
                        yield new Page.LongPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case DOUBLE -> {
                        double[] values = new double[numValues];
                        decoder.readDoubles(values, definitionLevels, maxDefLevel);
                        yield new Page.DoublePage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case INT32 -> {
                        int[] values = new int[numValues];
                        decoder.readInts(values, definitionLevels, maxDefLevel);
                        yield new Page.IntPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case FLOAT -> {
                        float[] values = new float[numValues];
                        decoder.readFloats(values, definitionLevels, maxDefLevel);
                        yield new Page.FloatPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case BOOLEAN -> {
                        boolean[] values = new boolean[numValues];
                        decoder.readBooleans(values, definitionLevels, maxDefLevel);
                        yield new Page.BooleanPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> {
                        byte[][] values = new byte[numValues][];
                        decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                        yield new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                };
                // Values read at the wrong boundaries would decode into wrong values, not an EOF
                if (decoder.remaining() != 0) {
                    throw new ParquetReadException("PLAIN value section does not end with its "
                            + countNonNullValues(numValues, definitionLevels) + " " + type
                            + " values; leftover bytes: " + decoder.remaining());
                }
                yield page;
            }
            case DELTA_BINARY_PACKED -> {
                DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder(data, offset, limit);
                yield switch (type) {
                    case INT64 -> {
                        long[] values = new long[numValues];
                        decoder.readLongs(values, definitionLevels, maxDefLevel);
                        yield new Page.LongPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case INT32 -> {
                        int[] values = new int[numValues];
                        decoder.readInts(values, definitionLevels, maxDefLevel);
                        yield new Page.IntPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    default -> throw undefinedOverType(Encoding.DELTA_BINARY_PACKED, type,
                            PhysicalType.INT32, PhysicalType.INT64);
                };
            }
            case BYTE_STREAM_SPLIT -> {
                // The decoder derives its byte width from the physical type, so an undefined pair
                // has to be refused before it is constructed — otherwise the width lookup, and not
                // the format check, is what the file fails on.
                if (type == PhysicalType.BOOLEAN || type == PhysicalType.BYTE_ARRAY
                        || type == PhysicalType.INT96) {
                    throw byteStreamSplitUndefinedOverType(type);
                }
                int numNonNullValues = countNonNullValues(numValues, definitionLevels);
                ByteStreamSplitDecoder decoder = new ByteStreamSplitDecoder(
                        data, offset, limit, numNonNullValues, type, column.typeLength());
                yield switch (type) {
                    case INT64 -> {
                        long[] values = new long[numValues];
                        decoder.readLongs(values, definitionLevels, maxDefLevel);
                        yield new Page.LongPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case DOUBLE -> {
                        double[] values = new double[numValues];
                        decoder.readDoubles(values, definitionLevels, maxDefLevel);
                        yield new Page.DoublePage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case INT32 -> {
                        int[] values = new int[numValues];
                        decoder.readInts(values, definitionLevels, maxDefLevel);
                        yield new Page.IntPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case FLOAT -> {
                        float[] values = new float[numValues];
                        decoder.readFloats(values, definitionLevels, maxDefLevel);
                        yield new Page.FloatPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    case FIXED_LEN_BYTE_ARRAY -> {
                        byte[][] values = new byte[numValues][];
                        decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                        yield new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                    }
                    default -> throw byteStreamSplitUndefinedOverType(type);
                };
            }
            case ALP -> {
                if (type != PhysicalType.FLOAT && type != PhysicalType.DOUBLE) {
                    throw undefinedOverType(Encoding.ALP, type, PhysicalType.FLOAT, PhysicalType.DOUBLE);
                }
                int numNonNullValues = countNonNullValues(numValues, definitionLevels);
                AlpDecoder decoder = new AlpDecoder(data, offset, limit, numNonNullValues, type);
                if (type == PhysicalType.DOUBLE) {
                    double[] values = new double[numValues];
                    decoder.readDoubles(values, definitionLevels, maxDefLevel);
                    yield new Page.DoublePage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
                }
                float[] values = new float[numValues];
                decoder.readFloats(values, definitionLevels, maxDefLevel);
                yield new Page.FloatPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> {
                if (dictionary == null) {
                    throw new ParquetReadException("Dictionary page not found for " + encoding + " encoding");
                }
                if (offset >= limit) {
                    throw new ParquetReadException("Unexpected EOF reading the dictionary index bit width");
                }
                int bitWidth = data[offset++] & 0xFF;
                if (bitWidth > 32) {
                    throw new ParquetReadException("Invalid dictionary index bit width: " + bitWidth
                            + ". Must be between 0 and 32");
                }
                RleBitPackingHybridDecoder indexDecoder = new RleBitPackingHybridDecoder(data, offset, limit - offset, bitWidth);

                yield dictionary.decodePage(indexDecoder, numValues, definitionLevels, repetitionLevels, maxDefLevel);
            }
            case RLE -> {
                // In the value position RLE carries booleans and nothing else; the format also
                // gives it the level streams and dictionary indices, which do not come through
                // here. Refusing another type is the format's position, not a decoder this
                // release has yet to write.
                if (type != PhysicalType.BOOLEAN) {
                    throw new ParquetReadException(
                            "RLE encodes only boolean values in a data page, not " + type);
                }

                // Read 4-byte length prefix (little-endian)
                int rleLength = readSectionLength(data, offset, limit, "RLE boolean values");
                offset += 4;

                RleBitPackingHybridDecoder decoder = new RleBitPackingHybridDecoder(data, offset, rleLength, 1);
                boolean[] values = new boolean[numValues];
                decoder.readBooleans(values, definitionLevels, maxDefLevel);
                yield new Page.BooleanPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            case DELTA_LENGTH_BYTE_ARRAY -> {
                // The lengths are delta-encoded ahead of the bytes, so the values are byte arrays
                // whatever the column claims; decoding an integer column's page this way would
                // build a page of the wrong shape rather than fail.
                if (type != PhysicalType.BYTE_ARRAY) {
                    throw undefinedOverType(Encoding.DELTA_LENGTH_BYTE_ARRAY, type,
                            PhysicalType.BYTE_ARRAY);
                }
                int numNonNullValues = countNonNullValues(numValues, definitionLevels);
                DeltaLengthByteArrayDecoder decoder = new DeltaLengthByteArrayDecoder(data, offset, limit);
                decoder.initialize(numNonNullValues);
                byte[][] values = new byte[numValues][];
                decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                yield new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            case DELTA_BYTE_ARRAY -> {
                if (type != PhysicalType.BYTE_ARRAY && type != PhysicalType.FIXED_LEN_BYTE_ARRAY) {
                    throw undefinedOverType(Encoding.DELTA_BYTE_ARRAY, type,
                            PhysicalType.BYTE_ARRAY, PhysicalType.FIXED_LEN_BYTE_ARRAY);
                }
                int numNonNullValues = countNonNullValues(numValues, definitionLevels);
                DeltaByteArrayDecoder decoder = new DeltaByteArrayDecoder(data, offset, limit);
                decoder.initialize(numNonNullValues);
                byte[][] values = new byte[numValues][];
                decoder.readByteArrays(values, definitionLevels, maxDefLevel);
                yield new Page.ByteArrayPage(values, definitionLevels, repetitionLevels, maxDefLevel, numValues);
            }
            // BIT_PACKED encodes levels, never a page's values, so refusing it is the format's
            // position rather than a decoder this release has yet to write.
            case BIT_PACKED -> throw new ParquetReadException(
                    "BIT_PACKED encodes levels and is not valid for a data page's values");
            // UNKNOWN stands for every Thrift value this release cannot name, so on its own it
            // does not say which encoding was met; the raw value does. Like an ALP variant
            // AlpDecoder does not know, it is a gap in this release rather than a malformed file,
            // so it keeps UnsupportedOperationException.
            case UNKNOWN -> throw new UnsupportedOperationException(
                    "Encoding not yet supported: UNKNOWN (Thrift encoding value " + encodingValue + ")");
        };
    }

    /// Refusal for a page whose declared encoding is not defined over its column's physical type.
    ///
    /// The format names the physical types each encoding may carry, so such a page is a malformed
    /// file rather than a gap in this release — which is why this is a [ParquetReadException] and
    /// not the [UnsupportedOperationException] an unrecognized encoding raises. Decoding it anyway would
    /// build a page of the wrong shape rather than fail: wrong in the values, and wrong in the
    /// [Page] variant handed to the column reader.
    ///
    /// @param encoding the encoding the page declares
    /// @param type the column's physical type
    /// @param definedOver the physical types the format defines `encoding` over
    /// @return the exception to throw
    private static ParquetReadException undefinedOverType(Encoding encoding, PhysicalType type,
            PhysicalType... definedOver) {
        StringBuilder legalTypes = new StringBuilder();
        for (PhysicalType candidate : definedOver) {
            if (!legalTypes.isEmpty()) {
                legalTypes.append(", ");
            }
            legalTypes.append(candidate);
        }
        return new ParquetReadException(encoding + " is not defined over " + type
                + "; the format defines it over " + legalTypes + " only");
    }

    /// [#undefinedOverType] for `BYTE_STREAM_SPLIT`, whose legal types are named both before the
    /// decoder is constructed and by the value switch that follows it.
    private static ParquetReadException byteStreamSplitUndefinedOverType(PhysicalType type) {
        return undefinedOverType(Encoding.BYTE_STREAM_SPLIT, type, PhysicalType.INT32,
                PhysicalType.INT64, PhysicalType.FLOAT, PhysicalType.DOUBLE,
                PhysicalType.FIXED_LEN_BYTE_ARRAY);
    }
}
