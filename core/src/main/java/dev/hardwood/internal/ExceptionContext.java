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

/// Utility for enriching exception messages with file-name context.
///
/// All exception-wrapping for file-name enrichment is centralised here so that
/// reader classes share the same logic.
public final class ExceptionContext {

    private ExceptionContext() {
    }

    /// Returns the `[fileName] ` prefix used to mark exception messages with
    /// their originating file. Returns the empty string when `fileName` is `null`,
    /// so callers can always concatenate the result without a null check.
    public static String filePrefix(String fileName) {
        return fileName != null ? "[" + fileName + "] " : "";
    }

    /// Amends the exception message with a `[fileName] ` prefix. Preserves the
    /// original exception type and cause chain. Returns the original exception
    /// unchanged when the file name is unavailable or the prefix is already present.
    ///
    /// **Cause-chain note for [UncheckedIOException]:** because the type requires
    /// an [IOException] cause, the original [UncheckedIOException] is attached as
    /// a suppressed exception rather than as the cause; the cause slot holds the
    /// inner [IOException].
    ///
    /// @param fileName the originating file name, may be `null`
    /// @param e        the exception to enrich
    /// @return the enriched (or original) exception — never `null`
    public static RuntimeException addFileContext(String fileName, RuntimeException e) {
        if (fileName == null || fileName.isEmpty()) {
            return e;
        }
        String prefix = "[" + fileName + "] ";
        String originalMessage = e.getMessage();
        if (hasFilePrefix(originalMessage)) {
            return e;
        }
        // If the cause already carries file context (e.g. assembly-thread error
        // propagated through CompletionException), don't add a second layer.
        Throwable cause = e.getCause();
        if (cause != null && hasFilePrefix(cause.getMessage())) {
            return e;
        }
        String newMessage = prefix + (originalMessage != null ? originalMessage : e.getClass().getSimpleName());

        if (e instanceof UncheckedIOException uio) {
            // UncheckedIOException requires an IOException cause, so we can't chain
            // the original UncheckedIOException as the cause. Preserve it as suppressed.
            IOException ioCause = uio.getCause();
            if (ioCause == null) {
                ioCause = new IOException(originalMessage);
            }
            UncheckedIOException wrapped = new UncheckedIOException(newMessage, ioCause);
            wrapped.addSuppressed(e);
            return wrapped;
        }
        if (e.getClass() == IllegalArgumentException.class) {
            return new IllegalArgumentException(newMessage, e);
        }
        if (e.getClass() == IllegalStateException.class) {
            return new IllegalStateException(newMessage, e);
        }
        if (e.getClass() == NullPointerException.class) {
            NullPointerException wrapped = new NullPointerException(newMessage);
            wrapped.initCause(e);
            return wrapped;
        }
        if (e.getClass() == IndexOutOfBoundsException.class) {
            IndexOutOfBoundsException wrapped = new IndexOutOfBoundsException(newMessage);
            wrapped.initCause(e);
            return wrapped;
        }

        // For CompletionException and other wrapper types: try to preserve the type
        // via the (String, Throwable) constructor. For CompletionException, preserve
        // the original cause rather than wrapping the CompletionException itself,
        // so the cause chain stays shallow.
        try {
            Throwable preservedCause = (e instanceof CompletionException && e.getCause() != null)
                    ? e.getCause()
                    : e;
            return e.getClass()
                    .getConstructor(String.class, Throwable.class)
                    .newInstance(newMessage, preservedCause);
        }
        catch (ReflectiveOperationException ignored) {
            // Type cannot be preserved
        }
        return new RuntimeException(newMessage, e);
    }

    private static boolean hasFilePrefix(String message) {
        if (message == null || message.isEmpty() || message.charAt(0) != '[') {
            return false;
        }
        return message.indexOf("] ") > 1;
    }
}
