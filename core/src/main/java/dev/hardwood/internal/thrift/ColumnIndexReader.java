/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.Collections;
import java.util.List;

import dev.hardwood.internal.thrift.ThriftCompactConstants.FieldType.Codes;
import dev.hardwood.metadata.ColumnIndex;
import dev.hardwood.reader.ParquetReadException;

/// Reader for ColumnIndex from Thrift Compact Protocol.
///
/// Parquet ColumnIndex struct fields:
///
/// - 1: null_pages (list<bool>)
/// - 2: min_values (list<binary>)
/// - 3: max_values (list<binary>)
/// - 4: boundary_order (enum BoundaryOrder)
/// - 5: null_counts (list<i64>, optional)
/// - 6: repetition_level_histograms (list<i64>, optional)
/// - 7: definition_level_histograms (list<i64>, optional)
/// - 8: nan_counts (list<i64>, optional)
///
/// The histograms of fields 6 and 7 are stored one per page, concatenated page-major;
/// they are surfaced in that layout.
///
/// Every member is indexed by page, so they only describe a column chunk together. The
/// lengths are cross-checked once the struct has been read — see [#checkPageCounts].
public class ColumnIndexReader {

    /// Ids of the fields the format requires of a `ColumnIndex`.
    private static final int[] REQUIRED_FIELDS = { 1, 2, 3, 4 };

    public static ColumnIndex read(ThriftCompactReader reader) {
        int saved = reader.pushFieldIdContext(ThriftStruct.COLUMN_INDEX);
        try {
            return readInternal(reader);
        }
        finally {
            reader.popFieldIdContext(saved);
        }
    }

    private static ColumnIndex readInternal(ThriftCompactReader reader) {
        boolean[] nullPages = new boolean[0];
        List<byte[]> minValues = Collections.emptyList();
        List<byte[]> maxValues = Collections.emptyList();
        ColumnIndex.BoundaryOrder boundaryOrder = ColumnIndex.BoundaryOrder.UNORDERED;
        long[] nullCounts = null;
        long[] repetitionLevelHistograms = null;
        long[] definitionLevelHistograms = null;
        long[] nanCounts = null;
        long seen = 0;

        while (true) {
            int header = reader.readFieldHeader();
            if (header == ThriftCompactReader.STOP_FIELD) {
                break;
            }

            int fieldId = ThriftCompactReader.fieldId(header);
            switch (fieldId) {
                // Fields 1-4 are required, so a wrong wire type fails here rather than being
                // reported as a field that never arrived.
                case 1: // null_pages (required list<bool>)
                    reader.requireField(header, Codes.LIST);
                    nullPages = reader.readBoolArray();
                    seen |= 1L << fieldId;
                    break;
                case 2: // min_values (required list<binary>)
                    reader.requireField(header, Codes.LIST);
                    minValues = reader.readBinaryList();
                    seen |= 1L << fieldId;
                    break;
                case 3: // max_values (required list<binary>)
                    reader.requireField(header, Codes.LIST);
                    maxValues = reader.readBinaryList();
                    seen |= 1L << fieldId;
                    break;
                case 4: // boundary_order (required enum)
                    reader.requireField(header, Codes.I32);
                    boundaryOrder = boundaryOrder(reader.readI32());
                    seen |= 1L << fieldId;
                    break;
                case 5: // null_counts (list<i64>, optional)
                    if (reader.acceptField(header, Codes.LIST)) {
                        nullCounts = reader.readOptionalI64Array();
                    }
                    break;
                case 6: // repetition_level_histograms (list<i64>, optional)
                    if (reader.acceptField(header, Codes.LIST)) {
                        repetitionLevelHistograms = reader.readOptionalI64Array();
                    }
                    break;
                case 7: // definition_level_histograms (list<i64>, optional)
                    if (reader.acceptField(header, Codes.LIST)) {
                        definitionLevelHistograms = reader.readOptionalI64Array();
                    }
                    break;
                case 8: // nan_counts (list<i64>, optional)
                    if (reader.acceptField(header, Codes.LIST)) {
                        nanCounts = reader.readOptionalI64Array();
                    }
                    break;
                default:
                    reader.skipField(ThriftCompactReader.fieldType(header));
                    break;
            }
        }

        ThriftCompactReader.requireFields(ThriftStruct.COLUMN_INDEX, seen, REQUIRED_FIELDS);

        checkPageCounts(nullPages.length, minValues.size(), maxValues.size(), nullCounts, nanCounts,
                repetitionLevelHistograms, definitionLevelHistograms);

        return new ColumnIndex(nullPages, minValues, maxValues, boundaryOrder, nullCounts,
                repetitionLevelHistograms, definitionLevelHistograms, nanCounts);
    }

    private static ColumnIndex.BoundaryOrder boundaryOrder(int value) {
        return switch (value) {
            case 1 -> ColumnIndex.BoundaryOrder.ASCENDING;
            case 2 -> ColumnIndex.BoundaryOrder.DESCENDING;
            default -> ColumnIndex.BoundaryOrder.UNORDERED;
        };
    }

    /// Checks that every per-page member describes the same number of pages.
    ///
    /// `null_pages` is required and defines the page count. Page filtering indexes the other
    /// members with a count taken from elsewhere — from `OffsetIndex.page_locations`, or from
    /// `null_pages` for the arrays read here — so lengths that disagree surface as an unchecked
    /// `IndexOutOfBoundsException` thrown from inside the filter, naming neither the file nor
    /// the column. Rejecting the chunk here keeps that a controlled, attributable failure.
    ///
    /// The histograms hold `maxLevel + 1` entries per page rather than one, so only their
    /// divisibility by the page count can be checked.
    private static void checkPageCounts(int pageCount, int minValueCount, int maxValueCount,
            long[] nullCounts, long[] nanCounts, long[] repetitionLevelHistograms,
            long[] definitionLevelHistograms) {

        checkPerPageLength("min_values", minValueCount, pageCount);
        checkPerPageLength("max_values", maxValueCount, pageCount);
        if (nullCounts != null) {
            checkPerPageLength("null_counts", nullCounts.length, pageCount);
        }
        if (nanCounts != null) {
            checkPerPageLength("nan_counts", nanCounts.length, pageCount);
        }
        checkHistogramLength("repetition_level_histograms", repetitionLevelHistograms, pageCount);
        checkHistogramLength("definition_level_histograms", definitionLevelHistograms, pageCount);
    }

    private static void checkPerPageLength(String field, int length, int pageCount) {
        if (length != pageCount) {
            throw new ParquetReadException("Malformed Parquet metadata: ColumnIndex." + field + " has length "
                    + length + " but the index describes " + pageCount + " pages");
        }
    }

    private static void checkHistogramLength(String field, long[] histograms, int pageCount) {
        if (histograms == null) {
            return;
        }
        if (pageCount == 0 ? histograms.length != 0 : histograms.length % pageCount != 0) {
            throw new ParquetReadException("Malformed Parquet metadata: ColumnIndex." + field + " has length "
                    + histograms.length + " for " + pageCount
                    + " pages, which is not a whole number of entries per page");
        }
    }
}
