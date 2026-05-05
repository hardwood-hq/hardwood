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

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;

import static org.assertj.core.api.Assertions.assertThat;

/// Verifies [PageFormatProbe.firstDataPageType] against fixtures whose
/// writer-version is known at generation time. The probe's result feeds the
/// row-group-wide mask gate in [RowGroupIterator]; its correctness is the
/// load-bearing piece of the nested-v1 fallback path that this codebase has
/// no dedicated fixture for.
class PageFormatProbeTest {

    /// `inline_page_stats.parquet` is generated with `data_page_version='1.0'`.
    private static final Path V1_FIXTURE =
            Paths.get("src/test/resources/inline_page_stats.parquet");

    /// `misaligned_pages_no_index.parquet` is generated with `data_page_version='2.0'`.
    private static final Path V2_FIXTURE =
            Paths.get("src/test/resources/misaligned_pages_no_index.parquet");

    @Test
    void testProbeReturnsDataPageForV1Fixture() throws Exception {
        try (InputFile file = InputFile.of(V1_FIXTURE)) {
            file.open();
            FileMetaData meta = ParquetMetadataReader.readMetadata(file);
            RowGroup rg = meta.rowGroups().get(0);
            for (int c = 0; c < rg.columns().size(); c++) {
                PageHeader.PageType type =
                        PageFormatProbe.firstDataPageType(file, rg.columns().get(c));
                assertThat(type)
                        .as("column %d (path=%s)", c, rg.columns().get(c).metaData().pathInSchema())
                        .isEqualTo(PageHeader.PageType.DATA_PAGE);
            }
        }
    }

    @Test
    void testProbeReturnsDataPageV2ForV2Fixture() throws Exception {
        try (InputFile file = InputFile.of(V2_FIXTURE)) {
            file.open();
            FileMetaData meta = ParquetMetadataReader.readMetadata(file);
            RowGroup rg = meta.rowGroups().get(0);
            for (int c = 0; c < rg.columns().size(); c++) {
                PageHeader.PageType type =
                        PageFormatProbe.firstDataPageType(file, rg.columns().get(c));
                assertThat(type)
                        .as("column %d (path=%s)", c, rg.columns().get(c).metaData().pathInSchema())
                        .isEqualTo(PageHeader.PageType.DATA_PAGE_V2);
            }
        }
    }
}
