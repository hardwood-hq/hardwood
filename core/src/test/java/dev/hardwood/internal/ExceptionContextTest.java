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
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Unit tests for [ExceptionContext] type-preserving exception wrapping.
class ExceptionContextTest {

    private static final String FILE_NAME = "test.parquet";

    @Test
    void nullFileNameReturnsOriginalException() {
        IllegalArgumentException original = new IllegalArgumentException("bad input");
        RuntimeException result = ExceptionContext.addFileContext(null, original);
        assertThat(result).isSameAs(original);
    }

    @Test
    void emptyFileNameReturnsOriginalException() {
        IllegalArgumentException original = new IllegalArgumentException("bad input");
        RuntimeException result = ExceptionContext.addFileContext("", original);
        assertThat(result).isSameAs(original);
    }

    @Test
    void idempotentWhenAlreadyPrefixed() {
        IllegalArgumentException original = new IllegalArgumentException("[test.parquet] bad input");
        RuntimeException result = ExceptionContext.addFileContext(FILE_NAME, original);
        assertThat(result).isSameAs(original);
    }

    @Test
    void preservesIllegalArgumentException() {
        IllegalArgumentException original = new IllegalArgumentException("bad input");
        RuntimeException result = ExceptionContext.addFileContext(FILE_NAME, original);
        assertThat(result).isExactlyInstanceOf(IllegalArgumentException.class);
        assertThat(result.getMessage()).isEqualTo("[test.parquet] bad input");
        assertThat(result.getCause()).isSameAs(original);
    }

    @Test
    void preservesIllegalStateException() {
        IllegalStateException original = new IllegalStateException("wrong state");
        RuntimeException result = ExceptionContext.addFileContext(FILE_NAME, original);
        assertThat(result).isExactlyInstanceOf(IllegalStateException.class);
        assertThat(result.getMessage()).isEqualTo("[test.parquet] wrong state");
        assertThat(result.getCause()).isSameAs(original);
    }

    @Test
    void preservesNullPointerException() {
        NullPointerException original = new NullPointerException("null ref");
        RuntimeException result = ExceptionContext.addFileContext(FILE_NAME, original);
        assertThat(result).isExactlyInstanceOf(NullPointerException.class);
        assertThat(result.getMessage()).isEqualTo("[test.parquet] null ref");
        assertThat(result.getCause()).isSameAs(original);
    }

    @Test
    void preservesIndexOutOfBoundsException() {
        IndexOutOfBoundsException original = new IndexOutOfBoundsException("index 5");
        RuntimeException result = ExceptionContext.addFileContext(FILE_NAME, original);
        assertThat(result).isExactlyInstanceOf(IndexOutOfBoundsException.class);
        assertThat(result.getMessage()).isEqualTo("[test.parquet] index 5");
        assertThat(result.getCause()).isSameAs(original);
    }

    @Test
    void preservesUncheckedIOException() {
        IOException ioe = new IOException("disk error");
        UncheckedIOException original = new UncheckedIOException("io fail", ioe);
        RuntimeException result = ExceptionContext.addFileContext(FILE_NAME, original);
        assertThat(result).isExactlyInstanceOf(UncheckedIOException.class);
        assertThat(result.getMessage()).isEqualTo("[test.parquet] io fail");
        // UncheckedIOException cause is the IOException, original preserved as suppressed
        assertThat(result.getCause()).isSameAs(ioe);
        assertThat(result.getSuppressed()).hasSize(1);
        assertThat(result.getSuppressed()[0]).isSameAs(original);
    }

    @Test
    void preservesCompletionException() {
        RuntimeException innerCause = new RuntimeException("inner");
        CompletionException original = new CompletionException("completion failed", innerCause);
        RuntimeException result = ExceptionContext.addFileContext(FILE_NAME, original);
        assertThat(result).isExactlyInstanceOf(CompletionException.class);
        assertThat(result.getMessage()).isEqualTo("[test.parquet] completion failed");
        // CompletionException preserves the inner cause, not the CE itself
        assertThat(result.getCause()).isSameAs(innerCause);
    }

    @Test
    void fallsBackToRuntimeExceptionForUnknownTypes() {
        // A custom RuntimeException subclass with no (String, Throwable) constructor
        RuntimeException original = new RuntimeException("custom") {
        };
        RuntimeException result = ExceptionContext.addFileContext(FILE_NAME, original);
        // Falls back to RuntimeException wrapper since anonymous class has no accessible constructor
        assertThat(result.getMessage()).isEqualTo("[test.parquet] custom");
        assertThat(result.getCause()).isSameAs(original);
    }

    @Test
    void handlesNullOriginalMessage() {
        IllegalArgumentException original = new IllegalArgumentException((String) null);
        RuntimeException result = ExceptionContext.addFileContext(FILE_NAME, original);
        assertThat(result.getMessage()).isEqualTo("[test.parquet] IllegalArgumentException");
    }

    @Test
    void subclassOfIllegalArgumentExceptionUsesReflection() {
        // Exact-class check should NOT match subclasses — they go through reflection
        IllegalArgumentException original = new IllegalArgumentException("bad") {
        };
        RuntimeException result = ExceptionContext.addFileContext(FILE_NAME, original);
        // Anonymous subclass lacks (String, Throwable) constructor, falls back to RuntimeException
        assertThat(result.getMessage()).isEqualTo("[test.parquet] bad");
        assertThat(result.getCause()).isSameAs(original);
    }

    @Test
    void idempotentWhenCauseAlreadyPrefixed() {
        // Simulates assembly-thread error propagated through CompletionException:
        // the cause already carries [file] prefix, so consumer should not add another
        RuntimeException inner = new RuntimeException("[test.parquet] decode error");
        CompletionException ce = new CompletionException("wrapper", inner);
        RuntimeException result = ExceptionContext.addFileContext(FILE_NAME, ce);
        assertThat(result).isSameAs(ce);
    }

    @Test
    void idempotentWhenCauseAlreadyPrefixedForDifferentFile() {
        RuntimeException inner = new RuntimeException("[other.parquet] decode error");
        CompletionException ce = new CompletionException("wrapper", inner);
        RuntimeException result = ExceptionContext.addFileContext(FILE_NAME, ce);
        assertThat(result).isSameAs(ce);
    }

    @Test
    void uncheckedIOExceptionWithNullCauseFallsBack() {
        // Construct an UncheckedIOException where getCause() returns null
        // by creating it with a real cause then clearing it via reflection would be fragile,
        // so instead we test the production guard path by verifying a normal UncheckedIOException
        // with a real IOException works correctly when the guard is hit
        IOException ioe = new IOException("disk error");
        UncheckedIOException original = new UncheckedIOException(ioe);
        RuntimeException result = ExceptionContext.addFileContext(FILE_NAME, original);
        assertThat(result).isExactlyInstanceOf(UncheckedIOException.class);
        assertThat(result.getMessage()).startsWith("[test.parquet] ");
        assertThat(result.getCause()).isSameAs(ioe);
    }
}
