/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.junit.jupiter.api.Test;

import dev.hardwood.HardwoodContext;
import dev.hardwood.metadata.FileMetaData;
import dev.hardwood.metadata.RowGroup;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;

public class PageCursorTest {

    @Test
    void testPageCursorRemovesPageInfoEntriesAfterDecoding() throws Exception {
        Path file = Paths.get("src/test/resources/delta_binary_packed_test.parquet");
        FileMetaData fileMetaData;
        FileSchema schema;

        try (ParquetFileReader reader = ParquetFileReader.open(file)) {
            fileMetaData = reader.getFileMetaData();
            schema = reader.getFileSchema();
        }

        try (HardwoodContext context = HardwoodContext.create();
            // Open the file
            FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {

            // Configure buffer and metadata
            MappedByteBuffer mapping = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
            ColumnSchema columnSchema = schema.getColumn(0);
            RowGroup rowGroup = fileMetaData.rowGroups().get(0);

            // Begin scanning file
            PageScanner scanner = new PageScanner(columnSchema, rowGroup.columns().get(0), context, mapping, 0);
            List<PageInfo> scannedPages = scanner.scanPages();
            assertThat(scannedPages).isNotEmpty();
            int pageCount = scannedPages.size();

            // Create a cursor and consume all pages
            PageCursor cursor = new PageCursor(scannedPages, context);
            while (cursor.hasNext()) {
                cursor.nextPage();
            }

            // After consumption, verify all internal PageInfo has been cleared out
            List<PageInfo> internalList = getPageInfos(cursor);
            assertThat(internalList).hasSize(pageCount);
            for (int i = 0; i < pageCount; i++) {
                assertThat(internalList.get(i))
                        .as("PageInfo at index %d should be null after decoding", i)
                        .isNull();
            }
        }
    }

    /**
     * Read the internal PageInfo values (via reflection) for testing purposes
     */
    @SuppressWarnings("unchecked")
    private static List<PageInfo> getPageInfos(PageCursor cursor) throws Exception {
        Field field = PageCursor.class.getDeclaredField("pageInfos");
        field.setAccessible(true);
        return (List<PageInfo>) field.get(cursor);
    }
}
