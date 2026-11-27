/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.fuzz;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.params.provider.MethodSource;

import com.code_intelligence.jazzer.junit.FuzzTest;

import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.HardwoodContextImpl;
import dev.hardwood.internal.TestDecodeBuffers;
import dev.hardwood.internal.reader.batch.Dictionary;
import dev.hardwood.internal.reader.decode.DataPage;
import dev.hardwood.internal.reader.decode.DataPages;
import dev.hardwood.internal.reader.decode.DictionaryParser;
import dev.hardwood.internal.reader.decode.PageDecoder;
import dev.hardwood.internal.reader.memory.WorkingSet;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;

/// Decodes arbitrary bytes as the pages of one column chunk ([ChunkInput]): a dictionary page,
/// then data pages, each decompressed with the chunk's codec and decoded with the encoding its
/// header declares, every value written out through the readers' fills. Unlike [FileFuzzTest],
/// whose mutations reach a page only where they keep the footer's offsets and the page's sizes
/// consistent, every input here reaches the page decoders. A decode returns, or fails with a
/// [ParquetReadException] raised where the bytes are found wrong, or an
/// [UnsupportedOperationException] for a codec or an encoding this version does not read. Any
/// other throwable is a finding, an [Error] among them, and so is a JDK runtime exception that a
/// read would restate as a [ParquetReadException] ([ExceptionContext#asReadFailure]):
/// it is a check the decoders lack, or a defect of their own.
class PageFuzzTest {

    /// A context of zero threads decodes on the fuzzer's thread; it supplies the decompressors a
    /// read uses, libdeflate's among them where the library is present
    private static final HardwoodContextImpl CONTEXT = HardwoodContextImpl.create(0);
    /// Pages decoded from an input, from its first
    private static final int MAX_PAGES = 4;

    @MethodSource("dev.hardwood.fuzz.Seeds#chunks")
    @FuzzTest
    void chunk(byte[] bytes) {
        ChunkInput input = ChunkInput.of(bytes);
        if (input == null) {
            return;
        }
        try {
            decode(input);
        }
        catch (ParquetReadException | UnsupportedOperationException e) {
            // malformed, or a codec or an encoding this version does not read
        }
    }

    private static void decode(ChunkInput input) {
        ColumnSchema column = input.column();
        ColumnMetaData metaData = metaData(input);
        PageDecoder decoder = new PageDecoder(metaData, column, CONTEXT.decompressorFactory());
        ByteBuffer pages = input.pages();
        Dictionary dictionary = null;
        int at = 0;
        for (int i = 0; i < MAX_PAGES && at < pages.limit(); i++) {
            int remaining = pages.limit() - at;
            DataPage page = DataPage.parse(pages.slice(at, remaining), 0);
            // the page's body lies within the input, or its parse failed
            int length = page.headerSize() + page.header().compressedPageSize();
            ByteBuffer bytes = pages.slice(at, length);
            if (page.header().type() == PageType.DICTIONARY_PAGE) {
                DataPage dictionaryPage = DataPage.parse(bytes, 0);
                dictionary = DictionaryParser.parsePage(dictionaryPage.header(), dictionaryPage.body(), column,
                        metaData, CONTEXT, WorkingSet.standalone(column), TestDecodeBuffers.create());
            }
            else {
                DataPages.read(decoder, column, bytes, dictionary);
            }
            if (length >= remaining) {
                return;
            }
            at += length;
        }
    }

    /// The metadata of a chunk of `input`'s column and codec that declares as many values as any
    /// page may, so that a page is held to its own header and body.
    private static ColumnMetaData metaData(ChunkInput input) {
        return new ColumnMetaData(input.column().type(), List.of(Encoding.PLAIN), input.column().fieldPath(),
                input.codec(), Long.MAX_VALUE, 0, 0, Map.of(), 0, null, null, null, null, null, List.of(), null);
    }
}
