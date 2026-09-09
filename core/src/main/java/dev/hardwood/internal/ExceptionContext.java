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

import dev.hardwood.reader.ParquetReadException;

/// Utility for enriching exception messages with the file, and where in it a read failed.
///
/// A failure the file caused names the row group, column and page the read stopped on,
/// because that varies between failures; one the calling code caused names only the file,
/// because the fix is the same wherever the reader had got to.
public final class ExceptionContext {

    /// The row group of a read that is in none: the footer parse, the per-file schema
    /// check, or a failure the pipeline could not attribute.
    ///
    /// Plain ints, because they travel through per-page arrays and per-call arguments where
    /// an [java.util.OptionalInt] would allocate. [ParquetReadException] is where they
    /// become optional, once, at the boundary a caller sees.
    public static final int UNKNOWN_ROW_GROUP = -1;

    /// The page of a read that is on none.
    public static final int UNKNOWN_PAGE = -1;

    /// The chunk's dictionary page, which has no ordinal among the data pages.
    public static final int DICTIONARY_PAGE = -2;

    private ExceptionContext() {
    }

    /// Returns the `[fileName] ` prefix used to mark exception messages with
    /// their originating file. Returns the empty string when `fileName` is `null`,
    /// so callers can always concatenate the result without a null check.
    public static String filePrefix(String fileName) {
        return fileName != null ? "[" + fileName + "] " : "";
    }

    /// Returns the `[fileName: row group N, column 'X'] ` prefix that marks a message with
    /// where in a file the read failed, dropping whichever part is unknown.
    ///
    /// The bracket already says "this is where", so the position goes inside it: one marker
    /// rather than two, and no separator to render. With no position this is [#filePrefix].
    ///
    /// @param fileName the originating file name, may be `null`
    /// @param rowGroup the row group being read, or [#UNKNOWN_ROW_GROUP]
    /// @param column   the column being read, may be `null`
    /// @return the prefix, or the empty string when `fileName` is `null`
    public static String readPrefix(String fileName, int rowGroup, String column) {
        return readPrefix(fileName, rowGroup, column, UNKNOWN_PAGE);
    }

    /// The same, naming the page the read was on.
    ///
    /// @param fileName the originating file name, may be `null`
    /// @param rowGroup the row group being read, or [#UNKNOWN_ROW_GROUP]
    /// @param column   the column being read, may be `null`
    /// @param page     the page's ordinal in its column chunk, [#DICTIONARY_PAGE], or
    ///                 [#UNKNOWN_PAGE]
    /// @return the prefix, or the empty string when `fileName` is `null`
    public static String readPrefix(String fileName, int rowGroup, String column, int page) {
        return readPrefix(fileName, rowGroup, column,
                page == DICTIONARY_PAGE ? UNKNOWN_PAGE : page, page == DICTIONARY_PAGE);
    }

    /// The same, with the dictionary page named rather than encoded in `page`. Any negative
    /// `rowGroup` or `page` is a part the reader could not determine and is left out.
    ///
    /// @param fileName       the originating file name, may be `null`
    /// @param rowGroup       the row group being read, negative if not known
    /// @param column         the column being read, may be `null`
    /// @param page           the page's ordinal in its column chunk, negative if not known
    /// @param dictionaryPage whether the read was on the chunk's dictionary page
    /// @return the prefix, or the empty string when `fileName` is `null`
    public static String readPrefix(String fileName, int rowGroup, String column, int page,
            boolean dictionaryPage) {
        if (fileName == null) {
            return "";
        }
        if (rowGroup < 0 && column == null && page < 0 && !dictionaryPage) {
            return filePrefix(fileName);
        }
        StringBuilder prefix = new StringBuilder("[").append(fileName).append(": ");
        String separator = "";
        if (rowGroup >= 0) {
            prefix.append("row group ").append(rowGroup);
            separator = ", ";
        }
        if (column != null) {
            prefix.append(separator).append("column '").append(column).append('\'');
            separator = ", ";
        }
        if (dictionaryPage) {
            prefix.append(separator).append("dictionary page");
        }
        else if (page >= 0) {
            prefix.append(separator).append("page ").append(page);
        }
        return prefix.append("] ").toString();
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
        return addReadContext(fileName, UNKNOWN_ROW_GROUP, null, e);
    }

    /// The same, with the row group and column the read failed in — what the read pipeline
    /// knows at the three boundaries between its threads.
    ///
    /// @param fileName the originating file name, may be `null`
    /// @param rowGroup the row group being read, or [#UNKNOWN_ROW_GROUP]
    /// @param column   the column being read, may be `null`
    /// @param e        the exception to enrich
    /// @return the enriched (or original) exception — never `null`
    public static RuntimeException addReadContext(String fileName, int rowGroup, String column,
            RuntimeException e) {
        return addReadContext(fileName, rowGroup, column, UNKNOWN_PAGE, e);
    }

    /// The same, naming the page the read was on.
    ///
    /// @param fileName the originating file name, may be `null`
    /// @param rowGroup the row group being read, or [#UNKNOWN_ROW_GROUP]
    /// @param column   the column being read, may be `null`
    /// @param page     the page's ordinal, [#DICTIONARY_PAGE], or [#UNKNOWN_PAGE]
    /// @param e        the exception to enrich
    /// @return the enriched (or original) exception — never `null`
    public static RuntimeException addReadContext(String fileName, int rowGroup, String column,
            int page, RuntimeException e) {
        if (fileName == null || fileName.isEmpty()) {
            return e;
        }
        String prefix = readPrefix(fileName, rowGroup, column, page);
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
            // Not a case the reader produces: every wrap it makes to leave a lambda is
            // undone by the method enclosing that lambda, and `InputFile.readRange`
            // declares `IOException`, so an implementation has a checked channel and no
            // reason to reach for the unchecked one. The arm is here because this method
            // takes an arbitrary `RuntimeException` and must not silently change its type
            // — `UncheckedIOException` declares (String, IOException) and not
            // (String, Throwable), so the reflective path below cannot construct one and
            // would hand the caller a plain `RuntimeException` instead.
            //
            // Its cause must be an IOException, so the original cannot be chained; it
            // is kept as suppressed.
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

    /// Restates an [UncheckedIOException] as the [IOException] a method that can declare one
    /// should raise.
    ///
    /// The wrapper's message is kept, because that is where the file name is: the read path
    /// wraps a transport failure precisely so [#addFileContext] can name the file on it, and
    /// unwrapping to the bare cause would hand the caller an unattributable failure. Where
    /// nothing was added — the `UncheckedIOException(IOException)` constructor sets the message
    /// to the cause's `toString()` — the cause itself is returned rather than a wrapper
    /// repeating it.
    ///
    /// @param e the wrapper to restate
    /// @return the [IOException] to throw, with `e`'s cause as its own
    public static IOException unwrap(UncheckedIOException e) {
        IOException cause = e.getCause();
        String message = e.getMessage();
        return message == null || message.equals(cause.toString())
                ? cause
                : new IOException(message, cause);
    }

    private static boolean hasFilePrefix(String message) {
        if (message == null || message.isEmpty() || message.charAt(0) != '[') {
            return false;
        }
        return message.indexOf("] ") > 1;
    }
}
