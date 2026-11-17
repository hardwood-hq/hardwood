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
import java.time.DateTimeException;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import dev.hardwood.internal.thrift.ThriftTruncatedException;
import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.SchemaIncompatibleException;
import dev.hardwood.row.VariantTypeException;

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
    void preservesClassCastException() {
        ClassCastException original = new ClassCastException("class [I cannot be cast to class [J");
        RuntimeException wrapped = ExceptionContext.addFileContext("file.parquet", original);

        assertThat(wrapped).isExactlyInstanceOf(ClassCastException.class);
        assertThat(wrapped.getMessage()).isEqualTo("[file.parquet] class [I cannot be cast to class [J");
        assertThat(wrapped.getCause()).isSameAs(original);
    }

    @Test
    void preservesDateTimeException() {
        DateTimeException original = new DateTimeException(
                "The TIME(MILLIS) value 86400000 lies outside a day, which holds 0 to 86399999");
        RuntimeException wrapped = ExceptionContext.addReadContext("file.parquet",
                ExceptionContext.UNKNOWN_ROW_GROUP, "t", original);

        assertThat(wrapped).isExactlyInstanceOf(DateTimeException.class);
        assertThat(wrapped).hasMessage("[file.parquet: column 't'] The TIME(MILLIS) value 86400000 lies outside"
                + " a day, which holds 0 to 86399999");
        assertThat(wrapped.getCause()).isSameAs(original);
    }

    @Test
    void preservesUnsupportedOperationException() {
        UnsupportedOperationException original = new UnsupportedOperationException("nope");
        RuntimeException wrapped = ExceptionContext.addFileContext("file.parquet", original);

        assertThat(wrapped).isInstanceOf(UnsupportedOperationException.class);
        assertThat(wrapped.getMessage()).isEqualTo("[file.parquet] nope");
    }

    /// Every type the reader raises is wrapped in a new exception of the same type with the
    /// located message.
    @ParameterizedTest
    @MethodSource("typesTheReaderRaises")
    void wrapsATypeTheReaderRaisesInTheSameType(RuntimeException original) {
        RuntimeException wrapped = ExceptionContext.wrapInSameType("[f.parquet] refused", original);

        assertThat(wrapped).isExactlyInstanceOf(original.getClass())
                .hasMessage("[f.parquet] refused")
                .hasCause(original);
    }

    static Stream<RuntimeException> typesTheReaderRaises() {
        return Stream.of(
                new IllegalArgumentException("refused"),
                new IllegalStateException("refused"),
                new UnsupportedOperationException("refused"),
                new NullPointerException("refused"),
                new IndexOutOfBoundsException("refused"),
                new ClassCastException("refused"),
                new DateTimeException("refused"),
                new NoSuchElementException("refused"),
                new VariantTypeException("refused"),
                new ParquetReadException("refused"),
                new SchemaIncompatibleException("refused"),
                new CompletionException("refused", null));
    }

    /// A subclass of a type the reader raises is not wrapped as that type, which would hand the
    /// caller its supertype.
    @Test
    void wrapsASubclassOfATypeTheReaderRaisesAsARuntimeException() {
        NumberFormatException original = new NumberFormatException("refused");

        assertThat(ExceptionContext.wrapInSameType("[f.parquet] refused", original))
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessage("[f.parquet] refused")
                .hasCause(original);
    }

    /// A rejected codec or encoding is the library's limit, and keeps saying so with the place
    /// the read reached.
    @Test
    void placesAnUnsupportedOperationException() {
        UnsupportedOperationException original = new UnsupportedOperationException("LZO compression is not supported");

        RuntimeException placed = ExceptionContext.addReadContext("f.parquet", 0, "name", 0, original);

        assertThat(placed).isExactlyInstanceOf(UnsupportedOperationException.class)
                .hasMessage("[f.parquet: row group 0, column 'name', page 0] LZO compression is not supported")
                .hasCause(original);
    }

    @Test
    void placesASchemaIncompatibleException() {
        SchemaIncompatibleException original = new SchemaIncompatibleException(
                "Column 'price' is FIXED_LEN_BYTE_ARRAY but declares no type_length");

        RuntimeException placed = ExceptionContext.addFileContext("f.parquet", original);

        assertThat(placed).isExactlyInstanceOf(SchemaIncompatibleException.class)
                .hasMessage("[f.parquet] Column 'price' is FIXED_LEN_BYTE_ARRAY but declares no type_length")
                .hasCause(original);
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

    /// A type the reader does not raise becomes a [RuntimeException], whatever constructors it
    /// declares: no constructor is found reflectively, so the JVM and a native image agree.
    @Test
    void wrapsATypeTheReaderDoesNotRaiseAsARuntimeException() {
        RuntimeException original = new CustomException("custom error", null);
        RuntimeException wrapped = ExceptionContext.addFileContext("test.parquet", original);

        assertThat(wrapped).isExactlyInstanceOf(RuntimeException.class)
                .hasMessage("[test.parquet] custom error")
                .hasCause(original);
    }

    /// A truncation carries the file's name like any other failure, and leaves as a plain
    /// [ParquetReadException]: [ThriftTruncatedException] is internal, and a caller must not
    /// be handed a type it cannot name.
    @Test
    void restatesThriftTruncatedExceptionAsParquetReadException() {
        RuntimeException original = new ThriftTruncatedException("Unexpected EOF while reading varint");
        RuntimeException wrapped = ExceptionContext.addReadContext("f.parquet", 0, "id", original);

        assertThat(wrapped).isExactlyInstanceOf(ParquetReadException.class);
        assertThat(wrapped.getMessage())
                .isEqualTo("[f.parquet: row group 0, column 'id'] Unexpected EOF while reading varint");
        assertThat(wrapped.getCause()).isSameAs(original);
    }

    /// A truncation that already names its file is not prefixed again, but is still restated.
    @Test
    void restatesAnAlreadyPlacedThriftTruncatedException() {
        RuntimeException original = new ThriftTruncatedException("[f.parquet] Unexpected EOF while reading varint");
        RuntimeException wrapped = ExceptionContext.addFileContext("f.parquet", original);

        assertThat(wrapped).isExactlyInstanceOf(ParquetReadException.class);
        assertThat(wrapped.getMessage()).isEqualTo("[f.parquet] Unexpected EOF while reading varint");
        assertThat(wrapped.getCause()).isSameAs(original);
    }

    /// Without a file name to add, the type is restated all the same.
    @Test
    void restatesThriftTruncatedExceptionWithoutAFileName() {
        RuntimeException original = new ThriftTruncatedException("Unexpected EOF while reading varint");
        RuntimeException wrapped = ExceptionContext.addFileContext(null, original);

        assertThat(wrapped).isExactlyInstanceOf(ParquetReadException.class);
        assertThat(wrapped.getMessage()).isEqualTo("Unexpected EOF while reading varint");
        assertThat(wrapped.getCause()).isSameAs(original);
    }

    @Test
    void asReadFailureRestatesThriftTruncatedException() {
        RuntimeException original = new ThriftTruncatedException("Unexpected EOF while reading varint");
        RuntimeException typed = ExceptionContext.asReadFailure(original);

        assertThat(typed).isExactlyInstanceOf(ParquetReadException.class);
        assertThat(typed.getMessage()).isEqualTo("Unexpected EOF while reading varint");
        assertThat(typed.getCause()).isSameAs(original);
    }

    /// A public subclass says more than its base type and is kept.
    @Test
    void asReadFailureKeepsAPublicReadFailureSubclass() {
        RuntimeException original = new SchemaIncompatibleException("[f.parquet] incompatible");

        assertThat(ExceptionContext.asReadFailure(original)).isSameAs(original);
    }

    /// A read made at the bytes outside the pipeline classifies before it places, as the
    /// pipeline does: a decoder's out-of-bounds index on a corrupt file leaves as a read failure.
    @Test
    void readFailureAtClassifiesThenPlaces() {
        RuntimeException original = new ArrayIndexOutOfBoundsException("Index 5 out of bounds for length 2");

        RuntimeException placed = ExceptionContext.readFailureAt("f.parquet", 0, "c", original);

        assertThat(placed).isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage("[f.parquet: row group 0, column 'c'] Index 5 out of bounds for length 2");
        assertThat(placed.getCause().getCause()).isSameAs(original);
    }

    @Test
    void placesAnIOException() {
        IOException original = new IOException("connection reset");

        IOException placed = ExceptionContext.addReadContext("f.parquet", 2, "c", 4, original);

        assertThat(placed).isExactlyInstanceOf(IOException.class)
                .hasMessage("[f.parquet: row group 2, column 'c', page 4] connection reset")
                .hasCause(original);
    }

    /// An `IOException` already naming its file is not named again.
    @Test
    void leavesAPlacedIOExceptionAsItIs() {
        IOException original = new IOException("[f.parquet] Failed to fetch metadata for row group 2");

        assertThat(ExceptionContext.addReadContext("f.parquet", 2, "c", 4, original)).isSameAs(original);
    }

    /// What callers catch is the base type. A subclass the reader does not raise still has to
    /// leave as a [ParquetReadException], or wrapping it takes the failure out of every
    /// `catch (ParquetReadException)` above and the file stops being reported as what is wrong.
    @Test
    void keepsAReadFailureOfAnotherSubclassAReadFailure() {
        RuntimeException original = new OtherReadException("not a type the reader raises", null);
        RuntimeException wrapped = ExceptionContext.addFileContext("f.parquet", original);

        assertThat(wrapped).isExactlyInstanceOf(ParquetReadException.class)
                .hasMessage("[f.parquet] not a type the reader raises")
                .hasCause(original);
    }

    /// Declares the (String, Throwable) constructor a reflective wrap would have used.
    public static class CustomException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public CustomException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /// Declares the (String, Throwable) constructor a reflective wrap would have used.
    public static class OtherReadException extends ParquetReadException {
        private static final long serialVersionUID = 1L;

        public OtherReadException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
