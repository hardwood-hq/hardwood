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

import dev.hardwood.internal.DecodeBuffers;
import dev.hardwood.internal.compression.Decompressor;
import dev.hardwood.internal.compression.DecompressorFactory;
import dev.hardwood.internal.compression.UncompressedDecompressor;
import dev.hardwood.internal.encoding.RleBitPackingHybridDecoder;
import dev.hardwood.internal.metadata.DataPageHeader;
import dev.hardwood.internal.metadata.DataPageHeaderV2;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.schema.FixedWidthValidator;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.jfr.PageDecodedEvent;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// Reads the data pages of one column chunk in place: a page's header and levels, and the value
/// reader its fill writes the values with ([FlatPageValues], [BinaryPageValues]), through
/// [#decodeFlat], [#decodeFlatBinary] and [#decodeNestedInPlace]. A page no reader takes is
/// refused before it is decompressed.
///
/// Page location is handled by the [FetchPlan] implementations, dictionary parsing by
/// [DictionaryParser].
public class PageDecoder {

    /// A task's buffer for decompressed bytes that do not outlive the decode which asks
    /// for them: a page whose values are read within its task, or a dictionary page, whose entries
    /// [Dictionary#parse] copies out.
    private static final DecodeBuffers.Slot<byte[]> TRANSIENT_BYTES = DecodeBuffers.slot();

    /// The task's [#TRANSIENT_BYTES] buffer, grown to at least `size` bytes. Its contents are
    /// valid until the next call with `buffers`.
    static byte[] transientBytes(DecodeBuffers buffers, int size) {
        byte[] bytes = buffers.get(TRANSIENT_BYTES);
        if (bytes == null || bytes.length < size) {
            bytes = new byte[size];
            buffers.set(TRANSIENT_BYTES, bytes);
        }
        return bytes;
    }

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
    /// its values left in its own decompressed bytes, or in the fetched page for values stored
    /// uncompressed ([#readsStoredValues]), which the fill reads in place.
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

    /// The width of the column's plain binary values: `0` for length-prefixed values, `-1` for a
    /// column that is not binary.
    private final int binaryWidth;

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
    /// @throws dev.hardwood.reader.SchemaIncompatibleException if the column is a
    ///         `FIXED_LEN_BYTE_ARRAY` whose width is absent or not positive
    public PageDecoder(ColumnMetaData columnMetaData, ColumnSchema column, DecompressorFactory decompressorFactory,
                       boolean fixedListFastPathEnabled) {
        this.columnMetaData = columnMetaData;
        this.column = column;
        this.decompressorFactory = decompressorFactory;
        this.fixedListFastPathEnabled = fixedListFastPathEnabled;
        this.binaryWidth = switch (column.type()) {
            case BYTE_ARRAY -> 0;
            case INT96 -> 12;
            case FIXED_LEN_BYTE_ARRAY -> FixedWidthValidator.requireWidth(null, column);
            default -> -1;
        };
    }

    /// Whether this is the decoder of the column chunk `metaData` describes. A decoder checks a
    /// page against its own chunk's metadata, so a page of another chunk takes a decoder of its
    /// own, even one with the same codec.
    public boolean decodesChunk(ColumnMetaData metaData) {
        return columnMetaData == metaData;
    }

    /// Gets the decompressor factory used by this PageDecoder.
    ///
    /// @return the decompressor factory
    public DecompressorFactory getDecompressorFactory() {
        return decompressorFactory;
    }

    /// Reads a data page (`V1` or `V2`) of a fixed-width numeric or boolean column without
    /// repetition whose values a fill reads in place, in any encoding the format defines for its
    /// type.
    ///
    /// The returned values hold `scratch`'s definition levels and ids, and the task's
    /// [#transientBytes] or the fetched page, until the fill is done.
    ///
    /// @throws ParquetReadException if the page is malformed, or its encoding is not defined over
    ///         the column's type
    FlatPageValues decodeFlat(ByteBuffer pageBuffer, Dictionary dictionary, LevelScratch scratch,
                              DecodeBuffers buffers) {
        requireColumn(binaryWidth < 0 && column.maxRepetitionLevel() == 0, "decodeFlat");
        InPlacePage page = readInPlace(pageBuffer, dictionary, scratch, false, buffers);
        FlatPageValues values = numericValues(page, dictionary, scratch);
        page.commit(column);
        return values;
    }

    /// Reads a data page (`V1` or `V2`) of a binary column without repetition whose values a fill
    /// reads in place, in any encoding the format defines for its type.
    ///
    /// The returned values hold `scratch`'s definition levels, ids and byte prefix, and the
    /// task's [#transientBytes] or the fetched page, until the fill is done.
    ///
    /// @throws ParquetReadException as [#decodeFlat] does
    BinaryPageValues decodeFlatBinary(ByteBuffer pageBuffer, Dictionary dictionary, LevelScratch scratch,
                                      DecodeBuffers buffers) {
        requireColumn(binaryWidth >= 0 && column.maxRepetitionLevel() == 0, "decodeFlatBinary");
        InPlacePage page = readInPlace(pageBuffer, dictionary, scratch, false, buffers);
        BinaryPageValues values = binaryValues(page, dictionary, scratch);
        page.commit(column);
        return values;
    }

    /// Reads the levels of a data page (`V1` or `V2`) of a column decoded through levels, in any
    /// encoding the format defines for its type, and leaves its values in place in `scratch`'s own
    /// byte buffer, or in the fetched page for values stored uncompressed ([#readsStoredValues]),
    /// for a fill to read later ([InPlaceNestedPage]). A page the fixed-size-list fast
    /// path takes keeps no levels and carries its `k`.
    ///
    /// @throws ParquetReadException as [#decodeFlat] does
    InPlaceNestedPage decodeNestedInPlace(ByteBuffer pageBuffer, Dictionary dictionary, LevelScratch scratch,
                                          DecodeBuffers buffers) {
        InPlacePage page = readInPlace(pageBuffer, dictionary, scratch, true, buffers);
        FlatPageValues flat = binaryWidth < 0 ? numericValues(page, dictionary, scratch) : null;
        BinaryPageValues binary = binaryWidth < 0 ? null : binaryValues(page, dictionary, scratch);
        page.commit(column);
        return new InPlaceNestedPage(page.numValues, page.definitionLevels, page.repetitionLevels,
                page.fixedListK, flat, binary, null);
    }

    /// The values of a null placeholder page, every one null, for a fill that reads them in place:
    /// all-zero definition (and repetition) levels in `scratch` and no value bytes.
    ///
    /// @throws IllegalStateException for a required column, which a placeholder cannot stand in for
    InPlaceNestedPage nullsInPlace(int numValues, LevelScratch scratch) {
        int maxDefLevel = column.maxDefinitionLevel();
        if (maxDefLevel == 0) {
            throw new IllegalStateException("Cannot create null placeholder page for required column '"
                    + column.name() + "' — maxDefinitionLevel is 0");
        }
        int[] definitionLevels = scratch.definitionLevels(numValues);
        Arrays.fill(definitionLevels, 0, numValues, 0);
        int[] repetitionLevels = null;
        if (column.maxRepetitionLevel() > 0) {
            repetitionLevels = scratch.repetitionLevels(numValues);
            Arrays.fill(repetitionLevels, 0, numValues, 0);
        }
        FlatPageValues flat = binaryWidth < 0
                ? FlatPageValues.plain(column.type(), definitionLevels, maxDefLevel, numValues, NO_BYTES, 0, 0)
                : null;
        BinaryPageValues binary = binaryWidth < 0
                ? null
                : BinaryPageValues.plain(column.type(), binaryWidth, definitionLevels, maxDefLevel, numValues,
                        NO_BYTES, 0, 0, scratch);
        return new InPlaceNestedPage(numValues, definitionLevels, repetitionLevels, 0, flat, binary, null);
    }

    private static final byte[] NO_BYTES = new byte[0];

    /// Fails an entry point called for a column it does not read, which is the caller's error.
    private void requireColumn(boolean served, String entryPoint) {
        if (!served) {
            throw new IllegalStateException(entryPoint + " does not read column '" + column.name() + "' of type "
                    + column.type() + " with maximum repetition level " + column.maxRepetitionLevel());
        }
    }

    private FlatPageValues numericValues(InPlacePage page, Dictionary dictionary, LevelScratch scratch) {
        int maxDefLevel = column.maxDefinitionLevel();
        return switch (page.encoding) {
            case PLAIN -> page.storedValues != null
                    ? FlatPageValues.plain(column.type(), page.definitionLevels, maxDefLevel, page.numValues,
                            page.storedValues)
                    : FlatPageValues.plain(column.type(), page.definitionLevels, maxDefLevel, page.numValues,
                            page.data, page.valuesOffset, page.limit);
            case RLE -> {
                int idCount = FlatPageValues.countNonNull(page.definitionLevels, maxDefLevel, page.numValues);
                int rleLength = readSectionLength(page.data, page.valuesOffset, page.limit, "RLE boolean values");
                int[] ids = scratch.ids(idCount);
                new RleBitPackingHybridDecoder(page.data, page.valuesOffset + 4, rleLength, 1)
                        .readInts(ids, 0, idCount);
                yield FlatPageValues.rleBooleans(page.numValues, page.definitionLevels, maxDefLevel, ids, idCount);
            }
            case DELTA_BINARY_PACKED -> FlatPageValues.delta(column.type(), page.definitionLevels, maxDefLevel,
                    page.numValues, page.data, page.valuesOffset, page.limit);
            case ALP -> FlatPageValues.alp(column.type(), page.definitionLevels, maxDefLevel, page.numValues,
                    page.data, page.valuesOffset, page.limit);
            case BYTE_STREAM_SPLIT -> FlatPageValues.byteStreamSplit(column.type(), page.definitionLevels,
                    maxDefLevel, page.numValues, page.data, page.valuesOffset, page.limit);
            default -> {
                int idCount = FlatPageValues.countNonNull(page.definitionLevels, maxDefLevel, page.numValues);
                int[] ids = decodeIds(page.data, page.valuesOffset, page.limit, idCount, scratch);
                yield FlatPageValues.dictionary(column.type(), page.numValues, page.definitionLevels, maxDefLevel,
                        ids, idCount, dictionary);
            }
        };
    }

    private BinaryPageValues binaryValues(InPlacePage page, Dictionary dictionary, LevelScratch scratch) {
        int maxDefLevel = column.maxDefinitionLevel();
        return switch (page.encoding) {
            case PLAIN -> BinaryPageValues.plain(column.type(), binaryWidth, page.definitionLevels, maxDefLevel,
                    page.numValues, page.data, page.valuesOffset, page.limit, scratch);
            case BYTE_STREAM_SPLIT -> BinaryPageValues.byteStreamSplit(binaryWidth, page.definitionLevels,
                    maxDefLevel, page.numValues, page.data, page.valuesOffset, page.limit, scratch);
            case DELTA_LENGTH_BYTE_ARRAY -> BinaryPageValues.deltaLength(page.definitionLevels, maxDefLevel,
                    page.numValues, page.data, page.valuesOffset, page.limit, scratch);
            case DELTA_BYTE_ARRAY -> BinaryPageValues.deltaByteArray(binaryWidth, page.definitionLevels,
                    maxDefLevel, page.numValues, page.data, page.valuesOffset, page.limit, scratch);
            default -> {
                int idCount = FlatPageValues.countNonNull(page.definitionLevels, maxDefLevel, page.numValues);
                int[] ids = decodeIds(page.data, page.valuesOffset, page.limit, idCount, scratch);
                if (!(dictionary instanceof Dictionary.ByteArrayDictionary binaryDictionary)) {
                    throw new ParquetReadException("A " + column.type() + " page refers to a dictionary of another type");
                }
                yield BinaryPageValues.ids(page.definitionLevels, maxDefLevel, page.numValues, ids, idCount,
                        binaryDictionary);
            }
        };
    }

    /// A data page read for a fill that reads its values in place: its values `[valuesOffset,
    /// limit)` of `data`, or `storedValues`, its decoded levels, and the decode event that spans it.
    ///
    /// @param storedValues the value section where it lies in the fetched page, for a page whose
    ///        values are stored uncompressed and read without a copy ([#readsStoredValues]); `null`
    ///        when the values are in `data`
    /// @param definitionLevels `null` when every value is present, and for a fixed-size-list page
    /// @param repetitionLevels `null` for a column without repetition, and for a fixed-size-list page
    /// @param fixedListK the elements per record of a fixed-size-list page, `0` for any other page
    private record InPlacePage(Encoding encoding, byte[] data, int limit, int valuesOffset, ByteBuffer storedValues,
                               int numValues, int[] definitionLevels, int[] repetitionLevels, int fixedListK, int compressedSize,
                               int uncompressedSize, PageDecodedEvent event) {

        void commit(ColumnSchema column) {
            event.column = column.name();
            event.compressedSize = compressedSize;
            event.uncompressedSize = uncompressedSize;
            event.commit();
        }
    }

    /// Refuses a data page declaring more values than its whole column chunk. The page and the
    /// chunk then disagree about the column, and the page's count would size its scratch.
    private void requireWithinChunk(int pageValues) {
        if (pageValues > columnMetaData.numValues()) {
            throw new ParquetReadException("Data page declares " + pageValues
                    + " values, more than the " + columnMetaData.numValues() + " of its column chunk");
        }
    }

    /// Reads the header of a data page, refuses it before decompressing it if its encoding is not
    /// defined over the column's type ([#requireReader]), and decodes its levels.
    ///
    /// @param ownBytes whether the values must outlive the task: they are then decompressed or
    ///        copied into `scratch`'s own buffer rather than the task's [#transientBytes]
    /// @param buffers the buffers of the task decoding the page
    private InPlacePage readInPlace(ByteBuffer pageBuffer, Dictionary dictionary, LevelScratch scratch,
                                    boolean ownBytes, DecodeBuffers buffers) {
        ThriftCompactReader headerReader = new ThriftCompactReader(pageBuffer, 0);
        PageHeader pageHeader = PageHeaderReader.read(headerReader);
        if (pageHeader.type() == PageType.DATA_PAGE) {
            DataPageHeader header = pageHeader.dataPageHeader();
            requireWithinChunk(header.numValues());
            requireReader(header.encoding(), header.encodingValue(), dictionary);
        }
        else if (pageHeader.type() == PageType.DATA_PAGE_V2) {
            DataPageHeaderV2 header = pageHeader.dataPageHeaderV2();
            requireWithinChunk(header.numValues());
            requireReader(header.encoding(), header.encodingValue(), dictionary);
        }
        else {
            throw new ParquetReadException("Unexpected page type for a data page: " + pageHeader.type());
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
                        scratch, ownBytes, buffers)
                : readV2InPlace(pageHeader.dataPageHeaderV2(), pageData, uncompressedSize, compressedSize, event,
                        scratch, ownBytes, buffers);
    }

    private InPlacePage readV1InPlace(DataPageHeader header, ByteBuffer pageData, int limit, int compressedSize,
                                      PageDecodedEvent event, LevelScratch scratch, boolean ownBytes,
                                      DecodeBuffers buffers) {
        boolean stored = columnMetaData.codec() == CompressionCodec.UNCOMPRESSED
                && readsStoredValues(header.encoding(), pageData);
        byte[] data;
        ByteBuffer body;
        if (stored) {
            UncompressedDecompressor.requireStoredSize(pageData.remaining(), limit);
            data = null;
            body = pageData.order(ByteOrder.LITTLE_ENDIAN);
        }
        else {
            Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());
            data = decompressor.decompress(pageData, limit,
                    ownBytes ? scratch.bytes(limit) : transientBytes(buffers, limit), buffers);
            body = ByteBuffer.wrap(data, 0, limit).order(ByteOrder.LITTLE_ENDIAN);
        }
        int numValues = header.numValues();
        int offset = 0;
        int repOffset = 0;
        int repLength = 0;
        if (column.maxRepetitionLevel() > 0) {
            repLength = readSectionLength(body, offset, limit, "repetition level");
            repOffset = offset + 4;
            offset = repOffset + repLength;
        }
        int defOffset = 0;
        int defLength = 0;
        if (column.maxDefinitionLevel() > 0) {
            defLength = readSectionLength(body, offset, limit, "definition level");
            defOffset = offset + 4;
            offset = defOffset + defLength;
        }
        ByteBuffer storedValues = null;
        if (stored) {
            // The level sections are decoded from a copy, and the values read where they lie
            data = scratch.levelBytes(offset);
            body.get(0, data, 0, offset);
            storedValues = body.slice(offset, limit - offset);
        }
        // The detector reads the RLE hybrid only; legacy BIT_PACKED levels keep their levels
        int k = header.repetitionLevelEncoding() == Encoding.RLE && header.definitionLevelEncoding() == Encoding.RLE
                ? fixedListK(data, repOffset, repLength, data, defOffset, defLength, numValues,
                        FixedSizeListDetector.ROWS_UNKNOWN)
                : 0;
        return levels(header.encoding(), data, limit, offset, storedValues, numValues, data, repOffset, repLength,
                data, defOffset, defLength, k, compressedSize, limit, event, scratch);
    }

    private InPlacePage readV2InPlace(DataPageHeaderV2 header, ByteBuffer pageData, int uncompressedPageSize,
                                      int compressedSize, PageDecodedEvent event, LevelScratch scratch,
                                      boolean ownBytes, DecodeBuffers buffers) {
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
        ByteBuffer storedValues = null;
        boolean compressed = isValueRegionCompressed(header, storedValuesLength);
        if (readsStoredValues(header.encoding(), pageData)
                && (!compressed || columnMetaData.codec() == CompressionCodec.UNCOMPRESSED)) {
            if (compressed) {
                // Flagged compressed in a chunk without a codec, as parquet-java writes it
                UncompressedDecompressor.requireStoredSize(storedValuesLength, uncompressedPageSize - valuesOffset);
            }
            storedValues = pageData.slice(valuesOffset, storedValuesLength);
            data = null;
            offset = 0;
            limit = 0;
        }
        else if (compressed) {
            int valuesLength = uncompressedPageSize - repLength - defLength;
            ByteBuffer compressedValues = pageData.slice(valuesOffset, storedValuesLength);
            Decompressor decompressor = decompressorFactory.getDecompressor(columnMetaData.codec());
            data = decompressor.decompress(compressedValues, valuesLength,
                    ownBytes ? scratch.bytes(valuesLength) : transientBytes(buffers, valuesLength), buffers);
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
        return levels(header.encoding(), data, limit, offset, storedValues, numValues, levelData, 0, repLength,
                levelData, repLength, defLength, k, compressedSize, uncompressedPageSize, event, scratch);
    }

    /// Whether a page in `encoding` whose value section is stored uncompressed in `pageData` is
    /// read where it lies in the fetched page, rather than from a copy: `PLAIN` values of a
    /// fixed-width numeric or boolean column, which [FlatPageValues] reads from a direct buffer or
    /// through the array of a heap one. A read-only heap buffer, which has neither, is copied. The
    /// fetched page outlives every fill of the page, through its [PageInfo].
    private boolean readsStoredValues(Encoding encoding, ByteBuffer pageData) {
        return encoding == Encoding.PLAIN && binaryWidth < 0 && (pageData.isDirect() || pageData.hasArray());
    }

    /// Refuses a page whose encoding has no reader for the column's type. The format defines each
    /// encoding over a fixed set of physical types, and every pair it defines has a reader, so a
    /// refusal here is the format's: a malformed file, except for an encoding this release does not
    /// know.
    ///
    /// @throws ParquetReadException if the encoding is not defined over the column's type, or is
    ///         dictionary-encoded in a chunk without a dictionary
    /// @throws UnsupportedOperationException for an encoding this release does not know
    private void requireReader(Encoding encoding, int encodingValue, Dictionary dictionary) {
        PhysicalType type = column.type();
        switch (encoding) {
            case PLAIN -> {
            }
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> {
                if (dictionary == null) {
                    throw new ParquetReadException("Dictionary page not found for " + encoding + " encoding");
                }
            }
            case BYTE_STREAM_SPLIT -> {
                if (type == PhysicalType.BOOLEAN || type == PhysicalType.BYTE_ARRAY || type == PhysicalType.INT96) {
                    throw undefinedOverType(Encoding.BYTE_STREAM_SPLIT, type, PhysicalType.INT32, PhysicalType.INT64,
                            PhysicalType.FLOAT, PhysicalType.DOUBLE, PhysicalType.FIXED_LEN_BYTE_ARRAY);
                }
            }
            case ALP -> {
                if (type != PhysicalType.FLOAT && type != PhysicalType.DOUBLE) {
                    throw undefinedOverType(Encoding.ALP, type, PhysicalType.FLOAT, PhysicalType.DOUBLE);
                }
            }
            case DELTA_BINARY_PACKED -> {
                if (type != PhysicalType.INT32 && type != PhysicalType.INT64) {
                    throw undefinedOverType(Encoding.DELTA_BINARY_PACKED, type, PhysicalType.INT32, PhysicalType.INT64);
                }
            }
            case DELTA_LENGTH_BYTE_ARRAY -> {
                if (type != PhysicalType.BYTE_ARRAY) {
                    throw undefinedOverType(Encoding.DELTA_LENGTH_BYTE_ARRAY, type, PhysicalType.BYTE_ARRAY);
                }
            }
            case DELTA_BYTE_ARRAY -> {
                if (type != PhysicalType.BYTE_ARRAY && type != PhysicalType.FIXED_LEN_BYTE_ARRAY) {
                    throw undefinedOverType(Encoding.DELTA_BYTE_ARRAY, type, PhysicalType.BYTE_ARRAY,
                            PhysicalType.FIXED_LEN_BYTE_ARRAY);
                }
            }
            // In the value position RLE carries booleans and nothing else; the format also gives it
            // the level streams and dictionary indices, which do not come through here
            case RLE -> {
                if (type != PhysicalType.BOOLEAN) {
                    throw new ParquetReadException("RLE encodes only boolean values in a data page, not " + type);
                }
            }
            case BIT_PACKED -> throw new ParquetReadException(
                    "BIT_PACKED encodes levels and is not valid for a data page's values");
            // UNKNOWN stands for every Thrift value this release cannot name, so the raw value says
            // which encoding was met. Like an ALP variant AlpDecoder does not know, it is a gap in
            // this release rather than a malformed file.
            case UNKNOWN -> throw new UnsupportedOperationException(
                    "Encoding not yet supported: UNKNOWN (Thrift encoding value " + encodingValue + ")");
        }
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
    private InPlacePage levels(Encoding encoding, byte[] data, int limit, int valuesOffset, ByteBuffer storedValues,
                               int numValues,
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
        return new InPlacePage(encoding, data, limit, valuesOffset, storedValues, numValues, definitionLevels,
                repetitionLevels, k, compressedSize, uncompressedSize, event);
    }

    /// Decodes the `idCount` dictionary ids of a dictionary-encoded page's value section
    /// `[offset, limit)` of `data` into `scratch`.
    private static int[] decodeIds(byte[] data, int offset, int limit, int idCount, LevelScratch scratch) {
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

    /// Reads the 4-byte little-endian length prefix of a section of `data` at `offset`, and checks
    /// that the section it announces ends within the page.
    private static int readSectionLength(byte[] data, int offset, int limit, String section) {
        return readSectionLength(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN), offset, limit, section);
    }

    /// Reads the 4-byte length prefix of a section of `data` at `offset`, and checks that the
    /// section it announces ends within the page.
    ///
    /// @param data the page's bytes, little-endian
    private static int readSectionLength(ByteBuffer data, int offset, int limit, String section) {
        if (Integer.BYTES > limit - offset) {
            throw new ParquetReadException("Unexpected EOF reading the " + section + " length");
        }
        int length = data.getInt(offset);
        int remaining = limit - offset - Integer.BYTES;
        if (length < 0 || length > remaining) {
            throw new ParquetReadException("Invalid " + section + " length " + length + ": "
                    + remaining + " bytes remain in the page");
        }
        return length;
    }

    /// Whether the value region of a `DataPageV2` body is stored compressed.
    private static boolean isValueRegionCompressed(DataPageHeaderV2 header, int compressedValuesLen) {
        return header.isCompressed() && compressedValuesLen > 0;
    }

    /// Refusal for a page whose declared encoding is not defined over its column's physical type.
    ///
    /// The format names the physical types each encoding may carry, so such a page is a malformed
    /// file rather than a gap in this release — which is why this is a [ParquetReadException] and
    /// not the [UnsupportedOperationException] an unrecognized encoding raises. Reading it anyway
    /// would take values of the wrong shape: wrong in the values, and wrong in the type the
    /// consumer casts to.
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
}
