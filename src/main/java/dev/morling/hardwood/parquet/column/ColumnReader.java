/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright The original authors
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.hardwood.parquet.column;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import dev.morling.hardwood.parquet.metadata.ColumnChunk;
import dev.morling.hardwood.parquet.metadata.ColumnMetaData;
import dev.morling.hardwood.parquet.page.PageReader;
import dev.morling.hardwood.parquet.schema.Column;

/**
 * Reader for a column chunk.
 */
public class ColumnReader {

    private final Column column;
    private final ColumnMetaData columnMetaData;
    private final PageReader pageReader;

    public ColumnReader(RandomAccessFile file, Column column, ColumnChunk columnChunk) {
        this.column = column;
        this.columnMetaData = columnChunk.metaData();
        this.pageReader = new PageReader(file, columnMetaData, column);
    }

    /**
     * Read all values from this column chunk.
     * Null values are represented as null in the returned list.
     */
    public List<Object> readAll() throws IOException {
        List<Object> allValues = new ArrayList<>();
        int maxDefinitionLevel = column.getMaxDefinitionLevel();

        while (true) {
            PageReader.Page page = pageReader.readPage();
            if (page == null) {
                break;
            }

            // Process values from page
            for (int i = 0; i < page.numValues(); i++) {
                if (maxDefinitionLevel == 0) {
                    // Required field - all values present
                    allValues.add(page.values()[i]);
                }
                else {
                    // Optional field - check definition level
                    if (page.definitionLevels()[i] == maxDefinitionLevel) {
                        // Value is present
                        allValues.add(page.values()[i]);
                    }
                    else {
                        // Value is null
                        allValues.add(null);
                    }
                }
            }
        }

        return allValues;
    }

    public Column getColumn() {
        return column;
    }

    public ColumnMetaData getColumnMetaData() {
        return columnMetaData;
    }
}
