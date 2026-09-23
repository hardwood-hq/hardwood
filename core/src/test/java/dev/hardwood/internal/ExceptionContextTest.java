/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.thrift.ThriftTruncatedException;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.SchemaIncompatibleException;

import static org.assertj.core.api.Assertions.assertThat;

/// Unit tests for [ExceptionContext].
class ExceptionContextTest {

    @Test
    void readPrefixNamesTheRowGroupAndColumn() {
        assertThat(ExceptionContext.readPrefix("f.parquet", 3, "amount"))
                .isEqualTo("[f.parquet: row group 3, column 'amount'] ");
    }

    /// The two are known independently — the footer parse is in no row group, and a
    /// schema check runs per file rather than per chunk — so each is dropped on its own.
    @Test
    void readPrefixDropsWhicheverPartIsUnknown() {
        assertThat(ExceptionContext.readPrefix("f.parquet", ExceptionContext.UNKNOWN_ROW_GROUP,
                "amount")).isEqualTo("[f.parquet: column 'amount'] ");
        assertThat(ExceptionContext.readPrefix("f.parquet", 3, null))
                .isEqualTo("[f.parquet: row group 3] ");
    }

    /// With no position there is nothing for a separator to divide, so the message follows
    /// the file directly and every caller that names only a file is unaffected.
    @Test
    void readPrefixWithNoPositionIsTheFilePrefix() {
        assertThat(ExceptionContext.readPrefix("f.parquet", ExceptionContext.UNKNOWN_ROW_GROUP,
                null)).isEqualTo(ExceptionContext.filePrefix("f.parquet"));
        assertThat(ExceptionContext.readPrefix(null, 3, "amount")).isEmpty();
    }

    @Test
    void readPrefixNamesThePage() {
        assertThat(ExceptionContext.readPrefix("f.parquet", 3, "amount", 12))
                .isEqualTo("[f.parquet: row group 3, column 'amount', page 12] ");
    }

    /// The dictionary page is named, not numbered: it has no ordinal among the data pages,
    /// and leaving it out would be indistinguishable from a failure met before any page.
    @Test
    void readPrefixNamesTheDictionaryPage() {
        assertThat(ExceptionContext.readPrefix("f.parquet", 3, "amount",
                ExceptionContext.DICTIONARY_PAGE))
                .isEqualTo("[f.parquet: row group 3, column 'amount', dictionary page] ");
    }

    /// Page 0 is a page like any other. An ordinal that happened to be falsy would be
    /// dropped by a looser check, and every chunk has one.
    @Test
    void readPrefixNamesTheFirstPage() {
        assertThat(ExceptionContext.readPrefix("f.parquet", 0, "amount", 0))
                .isEqualTo("[f.parquet: row group 0, column 'amount', page 0] ");
    }

    @Test
    void readPrefixDropsThePageWhenItIsUnknown() {
        assertThat(ExceptionContext.readPrefix("f.parquet", 3, "amount",
                ExceptionContext.UNKNOWN_PAGE))
                .isEqualTo("[f.parquet: row group 3, column 'amount'] ");
        assertThat(ExceptionContext.readPrefix("f.parquet", ExceptionContext.UNKNOWN_ROW_GROUP,
                null, 7)).isEqualTo("[f.parquet: page 7] ");
    }

    @Test
    void addsReadContextKeepingTheExceptionType() {
        IllegalStateException original = new IllegalStateException("bad run");
        RuntimeException wrapped =
                ExceptionContext.addReadContext("f.parquet", 0, "amount", original);

        assertThat(wrapped).isInstanceOf(IllegalStateException.class);
        assertThat(wrapped.getMessage()).isEqualTo("[f.parquet: row group 0, column 'amount'] bad run");
        assertThat(wrapped.getCause()).isSameAs(original);
    }

    @Test
    void addsFileContextToMessage() {
        IllegalStateException original = new IllegalStateException("something broke");
        RuntimeException wrapped = ExceptionContext.addFileContext("test.parquet", original);

        assertThat(wrapped).isInstanceOf(IllegalStateException.class);
        assertThat(wrapped.getMessage()).isEqualTo("[test.parquet] something broke");
        assertThat(wrapped.getCause()).isSameAs(original);
    }

    @Test
    void preservesIllegalArgumentException() {
        IllegalArgumentException original = new IllegalArgumentException("bad arg");
        RuntimeException wrapped = ExceptionContext.addFileContext("file.parquet", original);

        assertThat(wrapped).isInstanceOf(IllegalArgumentException.class);
        assertThat(wrapped.getMessage()).isEqualTo("[file.parquet] bad arg");
    }

    @Test
    void preservesUnsupportedOperationException() {
        UnsupportedOperationException original = new UnsupportedOperationException("nope");
        RuntimeException wrapped = ExceptionContext.addFileContext("file.parquet", original);

        assertThat(wrapped).isInstanceOf(UnsupportedOperationException.class);
        assertThat(wrapped.getMessage()).isEqualTo("[file.parquet] nope");
    }

    @Test
    void idempotentWhenAlreadyWrapped() {
        IllegalStateException original = new IllegalStateException("[test.parquet] already wrapped");
        RuntimeException wrapped = ExceptionContext.addFileContext("test.parquet", original);

        assertThat(wrapped).isSameAs(original);
    }

    @Test
    void nullFileNameReturnsOriginal() {
        IllegalStateException original = new IllegalStateException("error");
        RuntimeException wrapped = ExceptionContext.addFileContext(null, original);

        assertThat(wrapped).isSameAs(original);
    }

    @Test
    void nullMessageHandledGracefully() {
        RuntimeException original = new RuntimeException((String) null);
        RuntimeException wrapped = ExceptionContext.addFileContext("test.parquet", original);

        assertThat(wrapped.getMessage()).isEqualTo("[test.parquet] RuntimeException");
    }

    @Test
    void decoderRuntimeFailuresBecomeReadFailuresKeepingTheirCause() {
        for (RuntimeException raised : new RuntimeException[]{
                new ArrayIndexOutOfBoundsException("Index 7 out of bounds for length 4"),
                new IllegalStateException("Invalid RLE run header"),
                new ArithmeticException("integer overflow"),
                new IllegalArgumentException("negative page length"),
                new NullPointerException()}) {
            RuntimeException typed = ExceptionContext.asReadFailure(raised);

            assertThat(typed)
                    .as("%s from a decoder is the file being wrong", raised.getClass().getSimpleName())
                    .isInstanceOf(ParquetReadException.class)
                    .hasCause(raised);
        }
    }

    @Test
    void readFailureMessageFallsBackToTheTypeWhenTheOriginalHasNone() {
        RuntimeException typed =
                ExceptionContext.asReadFailure(new ArrayIndexOutOfBoundsException());

        assertThat(typed).hasMessage("ArrayIndexOutOfBoundsException");
    }

    @Test
    void transportAndAlreadyTypedFailuresPassThroughUnchanged() {
        UncheckedIOException transport = new UncheckedIOException(new IOException("connection reset"));
        ParquetReadException read = new ParquetReadException("bad magic");
        SchemaIncompatibleException schema = new SchemaIncompatibleException("column type differs");
        UnsupportedOperationException unsupported =
                new UnsupportedOperationException("BROTLI requires com.aayushatharva.brotli4j:brotli4j");

        assertThat(ExceptionContext.asReadFailure(transport)).isSameAs(transport);
        assertThat(ExceptionContext.asReadFailure(read)).isSameAs(read);
        assertThat(ExceptionContext.asReadFailure(schema)).isSameAs(schema);
        assertThat(ExceptionContext.asReadFailure(unsupported)).isSameAs(unsupported);
    }

    @Test
    void fallsBackToRuntimeExceptionForExoticType() {
        // A custom RuntimeException subclass without a (String, Throwable) constructor
        RuntimeException original = new CustomException();
        RuntimeException wrapped = ExceptionContext.addFileContext("test.parquet", original);

        assertThat(wrapped).isInstanceOf(RuntimeException.class);
        assertThat(wrapped.getMessage()).isEqualTo("[test.parquet] custom error");
        assertThat(wrapped.getCause()).isSameAs(original);
    }

    /// A truncation carries the file's name like any other failure and stays a
    /// [ThriftTruncatedException] doing it. The page-header peek widens its guess on
    /// this type and gives up on any other, so a restated one that came back as
    /// something else would turn a header with long statistics into a read error.
    @Test
    void preservesThriftTruncatedException() {
        RuntimeException original = new ThriftTruncatedException("Unexpected EOF while reading varint");
        RuntimeException wrapped = ExceptionContext.addReadContext("f.parquet", 0, "id", original);

        assertThat(wrapped).isInstanceOf(ThriftTruncatedException.class);
        assertThat(wrapped.getMessage())
                .isEqualTo("[f.parquet: row group 0, column 'id'] Unexpected EOF while reading varint");
        assertThat(wrapped.getCause()).isSameAs(original);
    }

    /// What callers catch is the base type. A subclass that cannot be reconstructed still
    /// has to leave as a [ParquetReadException], or restating it takes the failure out of
    /// every `catch (ParquetReadException)` above and the file stops being reported as
    /// what is wrong.
    @Test
    void keepsAnUnreconstructableReadFailureAReadFailure() {
        RuntimeException original = new UnreconstructableReadException();
        RuntimeException wrapped = ExceptionContext.addFileContext("f.parquet", original);

        assertThat(wrapped).isInstanceOf(ParquetReadException.class);
        assertThat(wrapped.getMessage()).isEqualTo("[f.parquet] cannot be rebuilt");
        assertThat(wrapped.getCause()).isSameAs(original);
    }

    private static class CustomException extends RuntimeException {
        CustomException() {
            super("custom error");
        }
    }

    private static class UnreconstructableReadException extends ParquetReadException {
        private static final long serialVersionUID = 1L;

        UnreconstructableReadException() {
            super("cannot be rebuilt");
        }
    }
}
