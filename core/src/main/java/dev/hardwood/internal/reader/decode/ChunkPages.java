/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.decode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.select.PageRowMask;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnChunk;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.PageType;
import dev.hardwood.schema.ColumnSchema;

/// The data pages of one column chunk, for tests and benchmarks that decode pages outside a read:
/// the chunk is read in one request and its page headers walked in order, the dictionary page
/// parsed for the pages after it, and every data page kept whole.
public final class ChunkPages {

    private ChunkPages() {
    }

    /// The data pages of `chunk`, the chunk of `column` in `file`, in file order.
    public static List<PageInfo> of(InputFile file, ColumnSchema column, ColumnChunk chunk,
                                    HardwoodContextImpl context) throws IOException {
        ColumnMetaData metaData = chunk.metaData();
        int length = Math.toIntExact(metaData.totalCompressedSize());
        ByteBuffer bytes = file.readRange(chunk.chunkStartOffset(), length);
        List<PageInfo> pages = new ArrayList<>();
        Dictionary dictionary = null;
        int position = 0;
        while (position < length) {
            ThriftCompactReader reader = new ThriftCompactReader(bytes.slice(position, length - position), 0);
            PageHeader header = PageHeaderReader.read(reader);
            int headerSize = reader.getBytesRead();
            int pageSize = headerSize + header.compressedPageSize();
            ByteBuffer page = bytes.slice(position, pageSize);
            if (header.type() == PageType.DICTIONARY_PAGE) {
                dictionary = DictionaryParser.parsePage(header, page.slice(headerSize, header.compressedPageSize()),
                        column, metaData, context);
            }
            else if (header.type() == PageType.DATA_PAGE || header.type() == PageType.DATA_PAGE_V2) {
                int values = header.type() == PageType.DATA_PAGE ? header.dataPageHeader().numValues()
                        : header.dataPageHeaderV2().numValues();
                pages.add(new PageInfo(new DataPage(header, headerSize, page), column, metaData, dictionary,
                        PageRowMask.ALL, PageInfo.keptRecords(column, PageRowMask.ALL, values)));
            }
            position += pageSize;
        }
        return pages;
    }
}
