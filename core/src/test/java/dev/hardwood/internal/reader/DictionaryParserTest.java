/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Test;

import dev.hardwood.InputFile;
import dev.hardwood.internal.metadata.DictionaryPageHeader;
import dev.hardwood.internal.metadata.PageHeader;
import dev.hardwood.internal.thrift.PageHeaderReader;
import dev.hardwood.internal.thrift.ThriftCompactReader;
import dev.hardwood.metadata.ColumnMetaData;
import dev.hardwood.metadata.Encoding;
import dev.hardwood.metadata.PageType;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.schema.ColumnSchema;
import dev.hardwood.schema.FileSchema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Covers [DictionaryParser]'s entry point for callers that have already parsed the page header.
class DictionaryParserTest {

    private static final Path FIXTURE = Paths.get("src/test/resources/column_index_pushdown_dict.parquet");

    /// A caller that scans page headers hands its parsed header and the page body to
    /// [DictionaryParser#parsePage]. The body it passes is the body alone — so a parser that
    /// went looking for a header in those bytes would decode the dictionary's first entries as
    /// a page header and fail, rather than agreeing by accident.
    @Test
    void parsesAPageFromAnAlreadyParsedHeaderAndItsBodyAlone() throws Exception {
        DictionaryPage page = firstDictionaryPage();

        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            Dictionary fromHeaderAndBody = DictionaryParser.parsePage(page.header(), page.body(),
                    page.columnSchema(), page.metaData(), context);
            Dictionary fromWholeRegion = DictionaryParser.parse(page.region(),
                    page.columnSchema(), page.metaData(), context);

            assertThat(fromHeaderAndBody.size())
                    .isEqualTo(page.header().dictionaryPageHeader().numValues());
            assertThat(((Dictionary.LongDictionary) fromHeaderAndBody).values())
                    .as("the entries the region-based entry point decodes, value for value")
                    .containsExactly(((Dictionary.LongDictionary) fromWholeRegion).values());
        }
    }

    /// A header describes a body of a given length, so that is the only body it can vouch for.
    /// Handed the whole region instead — the header still in front of the entries — the parser
    /// would decode header bytes as values, and on a file carrying no page CRC nothing further
    /// would catch it.
    @Test
    void rejectsABodyThatIsNotTheOneTheHeaderDescribes() throws Exception {
        DictionaryPage page = firstDictionaryPage();

        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            assertThatThrownBy(() -> DictionaryParser.parsePage(page.header(), page.region(),
                    page.columnSchema(), page.metaData(), context))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("Invalid dictionary page: body of " + page.region().remaining()
                            + " bytes, header claims " + page.header().compressedPageSize());
        }
    }

    /// A page of another type is the caller having handed over the wrong page altogether, which
    /// reads better as that than as a dictionary page missing its dictionary header.
    @Test
    void rejectsAPageThatIsNotADictionaryPage() throws Exception {
        DictionaryPage page = firstDictionaryPage();
        PageHeader dataPage = withType(page.header(), PageType.DATA_PAGE);

        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            assertThatThrownBy(() -> DictionaryParser.parsePage(dataPage, page.body(),
                    page.columnSchema(), page.metaData(), context))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("Invalid dictionary page: page type is DATA_PAGE");
        }
    }

    /// A page header claiming to be a dictionary page but carrying no `dictionary_page_header`
    /// has nothing to say how many values follow. Reading it out would dereference null.
    @Test
    void rejectsADictionaryPageWithoutItsDictionaryHeader() throws Exception {
        DictionaryPage page = firstDictionaryPage();
        PageHeader headerless = withDictionaryHeader(page.header(), null);

        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            assertThatThrownBy(() -> DictionaryParser.parsePage(headerless, page.body(),
                    page.columnSchema(), page.metaData(), context))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("Invalid dictionary page: no dictionary_page_header");
        }
    }

    /// `numValues` sizes the decoded dictionary, so a negative count has to be refused before it
    /// reaches the decoder rather than surfacing there as an array-sizing failure.
    @Test
    void rejectsANegativeValueCount() throws Exception {
        DictionaryPage page = firstDictionaryPage();
        PageHeader negative = withDictionaryHeader(page.header(),
                new DictionaryPageHeader(-1, page.header().dictionaryPageHeader().encoding()));

        try (HardwoodContextImpl context = HardwoodContextImpl.create()) {
            assertThatThrownBy(() -> DictionaryParser.parsePage(negative, page.body(),
                    page.columnSchema(), page.metaData(), context))
                    .isInstanceOf(ParquetReadException.class)
                    .hasMessage("Invalid dictionary page: negative numValues (-1)");
        }
    }

    private static PageHeader withType(PageHeader header, PageType type) {
        return new PageHeader(type, header.uncompressedPageSize(), header.compressedPageSize(),
                header.dataPageHeader(), header.dataPageHeaderV2(), header.dictionaryPageHeader(), header.crc());
    }

    private static PageHeader withDictionaryHeader(PageHeader header, DictionaryPageHeader dictionaryPageHeader) {
        return new PageHeader(header.type(), header.uncompressedPageSize(), header.compressedPageSize(),
                header.dataPageHeader(), header.dataPageHeaderV2(), dictionaryPageHeader, header.crc());
    }

    /// The dictionary page of the fixture's first column chunk, in the three shapes the parser's
    /// entry points take: the whole region, its parsed header, and its body alone.
    private record DictionaryPage(ByteBuffer region, PageHeader header, ByteBuffer body,
            ColumnSchema columnSchema, ColumnMetaData metaData) {}

    private static DictionaryPage firstDictionaryPage() throws IOException {
        ByteBuffer file = ByteBuffer.wrap(Files.readAllBytes(FIXTURE));

        ColumnMetaData metaData;
        ColumnSchema columnSchema;
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(file))) {
            metaData = reader.getFileMetaData().rowGroups().getFirst().columns().getFirst().metaData();
            columnSchema = FileSchema.fromSchemaElements(reader.getFileMetaData().schema()).getColumn(0);
        }

        long dictionaryOffset = metaData.dictionaryPageOffset();
        int regionSize = Math.toIntExact(metaData.dataPageOffset() - dictionaryOffset);
        ByteBuffer region = file.slice(Math.toIntExact(dictionaryOffset), regionSize);

        ThriftCompactReader headerReader = new ThriftCompactReader(region, 0);
        PageHeader header = PageHeaderReader.read(headerReader);
        assertThat(header.type()).isEqualTo(PageType.DICTIONARY_PAGE);
        assertThat(header.dictionaryPageHeader().encoding())
                .isIn(Encoding.PLAIN, Encoding.PLAIN_DICTIONARY);

        ByteBuffer body = region.slice(headerReader.getBytesRead(), header.compressedPageSize());
        return new DictionaryPage(region, header, body, columnSchema, metaData);
    }
}
