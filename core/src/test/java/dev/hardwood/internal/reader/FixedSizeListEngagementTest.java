/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

/// Decode-level engagement checks for the fixed-size-list fast path. The fast
/// path is transparent at the public API (levels are synthesized on demand), so
/// engagement is observed here at its source — [Page#fixedListK()] on the page
/// [PageDecoder] produces — rather than through any user-visible difference.
class FixedSizeListEngagementTest {

    private static final Path FIXED_WIDTH_FLOAT = Paths.get("src/test/resources/fixed_size_list_k4_v2.parquet");
    private static final Path BYTE_ARRAY = Paths.get("src/test/resources/fixed_size_list_flba_v2.parquet");
    private static final Path PAGED =
            Paths.get("src/test/resources/differential/diff_fixed_size_list_paged.parquet");
    private static final Path REQUIRED_LIST =
            Paths.get("src/test/resources/unannotated_repeated_annotated_list_test.parquet");
    private static final Path BARE_REPEATED =
            Paths.get("src/test/resources/unannotated_repeated_primitive_test.parquet");

    /// A required list of required elements (`maxDef == 1`) engages the fast path,
    /// but only for the annotated `LIST` group. The two fixtures share identical
    /// data pages and differ only in the footer schema: the annotated
    /// `required group (LIST)` has a `REQUIRED` leaf and engages (`k == 2`, the row
    /// `[42, 7]`); the collapsed bare `repeated int32` has a `REPEATED` leaf and is
    /// deliberately left to the regular path.
    @Test
    void requiredListEngagesOnlyWhenListAnnotated() throws Exception {
        assertThat(firstPageFixedListK(REQUIRED_LIST, true))
                .as("annotated required LIST engages").isEqualTo(2);
        assertThat(firstPageFixedListK(BARE_REPEATED, true))
                .as("bare unannotated repeated declines").isZero();
    }

    @Test
    void flagAndElementTypeControlEngagement() throws Exception {
        assertThat(firstPageFixedListK(FIXED_WIDTH_FLOAT, true)).as("fixed-width float list, fast path on").isEqualTo(4);
        assertThat(firstPageFixedListK(FIXED_WIDTH_FLOAT, false)).as("fast path disabled").isZero();
        assertThat(firstPageFixedListK(BYTE_ARRAY, true)).as("byte-array element declines").isZero();
    }

    /// The fixed-width columns of the differential base fixture span every
    /// primitive element type the fast path bulk-copies (FLOAT/INT32/INT64/DOUBLE/
    /// BOOLEAN, `vec_i32` additionally dictionary-encoded). Each must engage on
    /// every page so the differential oracle check exercises `copyValueRun`'s
    /// per-type arm rather than passing through the regular fallback.
    @Test
    void allPrimitiveElementTypesEngageFastPath() throws Exception {
        Path base = Paths.get("src/test/resources/differential/diff_fixed_size_list.parquet");
        for (String column : List.of("vec_f32", "vec_i32", "vec_i64", "vec_f64", "vec_bool")) {
            assertThat(allPageFixedListK(base, column))
                    .as("%s engages the fast path on every page", column)
                    .isNotEmpty()
                    .allMatch(k -> k > 0);
        }
    }

    /// The `vec_mixed` column of the paged fixture interleaves fixed-width pages (fast
    /// path) with pages carrying null rows (fallback). This proves the fixture
    /// actually exercises both, so the mixed-batch handling in `NestedColumnWorker`
    /// is genuinely covered rather than passing vacuously.
    @Test
    void mixedColumnHasBothFixedWidthAndFallbackPages() throws Exception {
        List<Integer> perPageK = allPageFixedListK(PAGED, "vec_mixed");
        assertThat(perPageK).as("some pages engage the fast path").contains(4);
        assertThat(perPageK).as("some pages fall back").contains(0);
    }

    /// Decodes the column's first data page with the fast path forced on or off
    /// and returns the resulting page's `fixedListK` (`> 0` iff the fast path
    /// engaged).
    private static int firstPageFixedListK(Path path, boolean fastPath) throws Exception {
        InputFile inputFile = InputFile.of(path);
        inputFile.open();
        HardwoodContextImpl context = HardwoodContextImpl.create();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            FileSchema schema = reader.getFileSchema();
            RowGroup rowGroup = reader.getFileMetaData().rowGroups().get(0);
            // Locate the fixed-size-list leaf (the only column with maxRep == 1);
            // some fixtures also carry a flat `id` column.
            int leaf = -1;
            for (int c = 0; c < rowGroup.columns().size(); c++) {
                if (schema.getColumn(c).maxRepetitionLevel() == 1) {
                    leaf = c;
                    break;
                }
            }
            ColumnChunk chunk = rowGroup.columns().get(leaf);
            ColumnSchema columnSchema = schema.getColumn(leaf);
            SequentialFetchPlan plan = SequentialFetchPlan.build(
                    inputFile, columnSchema, chunk, context, 0, inputFile.name(), 0);
            Iterator<PageInfo> pages = plan.pages();
            PageInfo pageInfo = pages.next();
            PageDecoder decoder = new PageDecoder(
                    pageInfo.columnMetaData(), pageInfo.columnSchema(),
                    context.decompressorFactory(), fastPath);
            Page page = decoder.decodePage(pageInfo.pageData(), pageInfo.dictionary());
            return page.fixedListK();
        }
        finally {
            inputFile.close();
            context.close();
        }
    }

    /// Decodes every data page of the named top-level column (across all row
    /// groups) with the fast path on and returns each page's `fixedListK`.
    private static List<Integer> allPageFixedListK(Path path, String topLevelName) throws Exception {
        InputFile inputFile = InputFile.of(path);
        inputFile.open();
        HardwoodContextImpl context = HardwoodContextImpl.create();
        List<Integer> result = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(path))) {
            FileSchema schema = reader.getFileSchema();
            int leaf = -1;
            for (int c = 0; c < schema.getColumnCount(); c++) {
                if (schema.getColumn(c).fieldPath().topLevelName().equals(topLevelName)) {
                    leaf = c;
                    break;
                }
            }
            ColumnSchema columnSchema = schema.getColumn(leaf);
            List<RowGroup> rowGroups = reader.getFileMetaData().rowGroups();
            for (RowGroup rowGroup : rowGroups) {
                ColumnChunk chunk = rowGroup.columns().get(leaf);
                SequentialFetchPlan plan = SequentialFetchPlan.build(
                        inputFile, columnSchema, chunk, context, 0, inputFile.name(), 0);
                Iterator<PageInfo> pages = plan.pages();
                while (pages.hasNext()) {
                    PageInfo pageInfo = pages.next();
                    PageDecoder decoder = new PageDecoder(
                            pageInfo.columnMetaData(), pageInfo.columnSchema(),
                            context.decompressorFactory(), true);
                    Page page = decoder.decodePage(pageInfo.pageData(), pageInfo.dictionary());
                    result.add(page.fixedListK());
                }
            }
        }
        finally {
            inputFile.close();
            context.close();
        }
        return result;
    }
}
