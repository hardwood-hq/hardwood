/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.ExceptionContext.ReadContext.Region;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.SchemaIncompatibleException;

import static org.assertj.core.api.Assertions.assertThat;

/// Where a read failure says it happened, and which failures say it at all.
///
/// The rules under test are properties of types and scopes rather than of any
/// raise site: nothing below names a position, and the positions still appear.
class ReadScopeTest {

    @Test
    void aFailureRaisedInAScopeNamesTheFileColumnAndRegion() {
        try (ReadScope.Scope file = ReadScope.file("f.parquet");
             ReadScope.Scope column = ReadScope.column(2, "id");
             ReadScope.Scope region = ReadScope.region(Region.DICTIONARY_PAGE, 4096)) {
            assertThat(new ParquetReadException("the page is not a page"))
                    .hasMessage("[f.parquet] row group 2, column id, dictionary page at byte 4096"
                            + " (0x001000) — the page is not a page");
        }
    }

    /// A region says where a read began, and a read that knows how far it got
    /// says so through the scope rather than by carrying a number out with the
    /// failure. Neither names a byte on its own.
    @Test
    void aParsePositionIsAddedToTheRegionsOwnStart() {
        try (ReadScope.Scope file = ReadScope.file("f.parquet");
             ReadScope.Scope region = ReadScope.region(Region.FOOTER, 1000)) {
            assertThat(ReadScope.stoppedAt("Unknown field type: 13", 24))
                    .hasMessage("[f.parquet] footer at byte 1024 (0x000400)"
                            + " — Unknown field type: 13");
        }
    }

    /// A buffer that is not part of any file has no byte to add to, and the
    /// failure says what went wrong without inventing one.
    @Test
    void aParsePositionOutsideAnyRegionNamesNoByte() {
        assertThat(ReadScope.stoppedAt("Unknown field type: 13", 24))
                .hasMessage("Unknown field type: 13");
    }

    /// A region with no address of its own names none. A decode that failed a
    /// thousand values into a page is not at the page's first byte, and the row
    /// group and column already say which page it was.
    @Test
    void aRegionWithNoAddressNamesNoByte() {
        try (ReadScope.Scope file = ReadScope.file("f.parquet");
             ReadScope.Scope column = ReadScope.column(1, "amount");
             ReadScope.Scope page = ReadScope.region(Region.DATA_PAGE)) {
            assertThat(new ParquetReadException("bad run"))
                    .hasMessage("[f.parquet] row group 1, column amount, data page — bad run");
        }
    }

    /// The type is the rule. A correct file this library will not read is named
    /// and never positioned, because it is not the type that carries a place —
    /// not because any raise site decided so.
    @Test
    void anUnsupportedFileIsNamedButNeverPositioned() {
        try (ReadScope.Scope file = ReadScope.file("f.parquet");
             ReadScope.Scope column = ReadScope.column(0, "amount");
             ReadScope.Scope region = ReadScope.region(Region.DATA_PAGE, 900)) {
            assertThat(new UnsupportedOperationException(
                    ReadScope.fileHere() + "no codec here"))
                    .hasMessage("[f.parquet] no codec here");
        }
    }

    /// The innermost scope is the one the failure was in.
    @Test
    void theInnermostScopeWinsAndTheEnclosingOneComesBack() {
        try (ReadScope.Scope file = ReadScope.file("f.parquet");
             ReadScope.Scope outer = ReadScope.region(Region.CHUNK_FETCH, 10)) {
            try (ReadScope.Scope inner = ReadScope.region(Region.PAGE_HEADER, 20)) {
                assertThat(new ParquetReadException("x").getMessage()).contains("page header");
            }
            assertThat(new ParquetReadException("x").getMessage()).contains("chunk fetch");
        }
    }

    /// A place is taken once, at construction, so a failure travelling out
    /// through further scopes is described once and describes the scope it was
    /// raised in. Nothing inspects the message to find that out.
    @Test
    void aFailureIsDescribedOnceHoweverManyScopesItLeaves() {
        ParquetReadException raised;
        try (ReadScope.Scope file = ReadScope.file("f.parquet");
             ReadScope.Scope region = ReadScope.region(Region.FOOTER, 8)) {
            raised = new ParquetReadException("bad");
        }
        try (ReadScope.Scope other = ReadScope.file("g.parquet")) {
            assertThat(raised).hasMessage("[f.parquet] footer at byte 8 (0x000008) — bad");
        }
        assertThat(raised).hasMessage("[f.parquet] footer at byte 8 (0x000008) — bad");
    }

    /// Describing a failure must not rebuild it: callers catch on the subclass,
    /// and a rebuilt one would arrive as its parent.
    @Test
    void beingDescribedKeepsAFailuresExactType() {
        try (ReadScope.Scope file = ReadScope.file("f.parquet")) {
            assertThat(new SchemaIncompatibleException("mismatch"))
                    .isInstanceOf(SchemaIncompatibleException.class)
                    .hasMessage("[f.parquet] mismatch");
        }
    }

    /// A place resumed on another thread is the one described, not whatever
    /// that thread was doing last. The decode of a page runs on a pool thread
    /// and inherits nothing.
    @Test
    void aResumedPlaceDescribesWhereTheWorkCameFrom() {
        ReadScope.Place page;
        try (ReadScope.Scope file = ReadScope.file("f.parquet");
             ReadScope.Scope column = ReadScope.column(3, "ts");
             ReadScope.Scope region = ReadScope.region(Region.BLOOM_FILTER, 720)) {
            page = ReadScope.current();
        }
        try (ReadScope.Scope elsewhere = ReadScope.file("g.parquet");
             ReadScope.Scope resumed = ReadScope.resume(page)) {
            assertThat(new ParquetReadException("x"))
                    .hasMessage("[f.parquet] row group 3, column ts, bloom filter at byte 720"
                            + " (0x0002d0) — x");
        }
    }

    /// Outside a read there is nothing to say, and the message is the one it
    /// was raised with.
    @Test
    void aFailureRaisedOutsideAnyScopeIsUntouched() {
        assertThat(new ParquetReadException("plain")).hasMessage("plain");
        assertThat(ReadScope.current()).isNull();
    }

    /// A transport failure takes the same place, and stays the [IOException] a caller
    /// catches and the reader declares — it takes its position as text rather than gaining
    /// a subclass to hold one.
    @Test
    void aFailedReadCarriesItsPlaceAndStaysAnIoException() {
        try (ReadScope.Scope file = ReadScope.file("f.parquet");
             ReadScope.Scope region = ReadScope.region(Region.CHUNK_FETCH, 2048)) {
            IOException failure = new IOException(
                    ReadScope.here() + "Failed to fetch 64 bytes");
            assertThat(failure).hasMessage("[f.parquet] chunk fetch at byte 2048"
                    + " (0x000800) — Failed to fetch 64 bytes");
        }
    }

    /// Outside a read both prefixes are empty, so a failure raised there reads exactly as
    /// it was written.
    @Test
    void thePrefixesAreEmptyOutsideAnyRead() {
        assertThat(ReadScope.here()).isEmpty();
        assertThat(ReadScope.fileHere()).isEmpty();
    }

    /// A read is entered all at once and left all at once: the narrowing methods extend the
    /// scope already opened rather than opening more, so one place is restored on close
    /// however many were chained.
    @Test
    void aChainedScopeNarrowsOnceAndRestoresOnce() {
        try (ReadScope.Scope outer = ReadScope.file("outer.parquet")) {
            try (ReadScope.Scope scope = ReadScope.file("f.parquet")
                    .column(2, "id")
                    .region(Region.DICTIONARY_PAGE, 4096)) {
                assertThat(new ParquetReadException("bad"))
                        .hasMessage("[f.parquet] row group 2, column id, dictionary page at byte"
                                + " 4096 (0x001000) — bad");
            }
            assertThat(ReadScope.current().fileName()).isEqualTo("outer.parquet");
        }
        assertThat(ReadScope.current()).isNull();
    }
}
