/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

import org.junit.jupiter.api.Test;

import dev.hardwood.internal.ExceptionContext.ReadContext;
import dev.hardwood.internal.ExceptionContext.ReadContext.Region;

import static dev.hardwood.internal.ExceptionContext.ReadContext.UNKNOWN_OFFSET;
import static dev.hardwood.internal.ExceptionContext.ReadContext.UNKNOWN_ROW_GROUP;
import static org.assertj.core.api.Assertions.assertThat;

/// Unit tests for [ExceptionContext].
class ExceptionContextTest {

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
    void fallsBackToRuntimeExceptionForExoticType() {
        // A custom RuntimeException subclass without a (String, Throwable) constructor
        RuntimeException original = new CustomException();
        RuntimeException wrapped = ExceptionContext.addFileContext("test.parquet", original);

        assertThat(wrapped).isInstanceOf(RuntimeException.class);
        assertThat(wrapped.getMessage()).isEqualTo("[test.parquet] custom error");
        assertThat(wrapped.getCause()).isSameAs(original);
    }

    private static class CustomException extends RuntimeException {
        CustomException() {
            super("custom error");
        }
    }

    /// A footer belongs to the file rather than to any part of it, so a
    /// context naming a region and an offset and no column at all still reads
    /// as a sentence.
    @Test
    void readContextWithNeitherRowGroupNorColumn() {
        assertThat(ExceptionContext.prefix(new ReadContext("f.parquet", UNKNOWN_ROW_GROUP, null,
                Region.FOOTER, 161340)))
                .isEqualTo("[f.parquet] footer at byte 161340 (0x02763c) — ");
    }

    /// A context that knows the stage but not how far it got must not leave a
    /// dangling "at byte" behind; the clause carries only what it was given.
    @Test
    void readContextWithoutAnOffsetNamesNoByte() {
        assertThat(ExceptionContext.prefix(
                new ReadContext("f.parquet", 3, "id", Region.DATA_PAGE, UNKNOWN_OFFSET)))
                .isEqualTo("[f.parquet] row group 3, column id, data page — ");
    }

    /// Nothing beyond the file leaves the prefix exactly as it has always been,
    /// so a caller can concatenate it without checking.
    @Test
    void readContextWithNothingButAFileNameIsJustThePrefix() {
        assertThat(ExceptionContext.prefix(new ReadContext("f.parquet", UNKNOWN_ROW_GROUP, null,
                null, UNKNOWN_OFFSET)))
                .isEqualTo("[f.parquet] ");
    }
}
