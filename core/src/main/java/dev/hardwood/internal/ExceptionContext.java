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

    /// Where a read was when it failed: which file, which row group and column,
    /// which region of the file was being read, and the byte to go and look at.
    ///
    /// Every part except the file name is optional. A place supplies what it
    /// has — `column` and `region` may be `null`, `rowGroup` may be
    /// [#UNKNOWN_ROW_GROUP] and `offset` may be [#UNKNOWN_OFFSET] — and the
    /// formatted message omits what is missing, so a context carrying only a
    /// file name reads exactly as [#addFileContext] always did.
    ///
    /// The row group earns its place on the failures that have no byte to
    /// name. Where the offset is exact it is largely redundant, since a file
    /// offset resolves to a row group through the footer; where there is no
    /// offset — a fetch that failed before reading, a page that would not
    /// assemble — it is the only thing narrowing the column down to a part of
    /// it, and it is the coordinate `hardwood dive` navigates by.
    ///
    ///
    /// @param fileName the file being read, may be `null`
    /// @param rowGroup the row group index, or [#UNKNOWN_ROW_GROUP]
    /// @param column   the column path, may be `null`
    /// @param region   what was being parsed, may be `null`
    /// @param offset   the byte to go and look at, or [#UNKNOWN_OFFSET]
    public record ReadContext(String fileName, int rowGroup, String column, Region region,
            long offset) {

        /// A region of a Parquet file, as a message names it.
        ///
        /// A closed set rather than free text: the words end up in a message a
        /// reader compares against another message, so they have to be the same
        /// words every time, and a typo in a string literal is not something a
        /// failure path will tell you about.
        ///
        /// The list is of a file's regions, not of one module's reads — the
        /// column and offset indexes are reached by the CLI rather than by the
        /// read path, but a reader should not have to know which of them
        /// produced the words.
        public enum Region {

            /// Reading or parsing a column chunk's dictionary.
            DICTIONARY_PAGE("dictionary page"),

            /// Parsing the Thrift header in front of a page.
            PAGE_HEADER("page header"),

            /// Fetching a page's bytes, before any decode.
            PAGE_FETCH("page fetch"),

            /// Fetching a column chunk's bytes, before any page is located in them.
            CHUNK_FETCH("chunk fetch"),

            /// Decompressing or decoding a page's values.
            DATA_PAGE("data page"),

            /// Assembling decoded pages into a batch.
            BATCH_ASSEMBLY("batch assembly"),

            /// Parsing a chunk's column index.
            COLUMN_INDEX("column index"),

            /// Parsing a chunk's offset index.
            OFFSET_INDEX("offset index"),

            /// Reading or parsing a chunk's bloom filter.
            BLOOM_FILTER("bloom filter"),

            /// Fetching the contiguous span that holds a row group's indexes.
            ROW_GROUP_INDEX("row-group index"),

            /// Parsing the file's Thrift footer.
            FOOTER("footer"),

            /// The four bytes in front of the closing magic that say how long
            /// the footer is. Its own region because it is read, and can be
            /// wrong, before the footer it describes has a position at all.
            FOOTER_LENGTH("footer length"),

            /// The `PAR1` markers that open and close the file.
            MAGIC("magic bytes");

            private final String text;

            Region(String text) {
                this.text = text;
            }

            /// Whether this region is the file's own bookkeeping rather than
            /// any part of its data.
            ///
            /// The three regions a file is opened through, and the ones a
            /// caller counting reads means when it asks how many were spent
            /// before the first row.
            public boolean isFileMetadata() {
                return this == MAGIC || this == FOOTER_LENGTH || this == FOOTER;
            }

            /// How a message names this region.
            @Override
            public String toString() {
                return text;
            }
        }

        /// Offset value meaning "nothing knows which byte to go and look at".
        public static final long UNKNOWN_OFFSET = -1;

        /// Row group value meaning "nothing knows which one".
        ///
        /// A batch assembled across a file boundary is the one read that
        /// genuinely spans row groups; every other scope knows its own.
        public static final int UNKNOWN_ROW_GROUP = -1;

    }

    /// The context clause a message carries after its `[file] ` prefix, or the
    /// empty string when the context names nothing beyond the file.
    static String contextClause(ReadContext context) {
        StringBuilder sb = new StringBuilder();
        if (context.rowGroup() != ReadContext.UNKNOWN_ROW_GROUP) {
            sb.append("row group ").append(context.rowGroup());
        }
        if (context.column() != null && !context.column().isEmpty()) {
            append(sb, "column " + context.column());
        }
        if (context.region() != null) {
            append(sb, context.region().toString());
        }
        if (context.offset() != ReadContext.UNKNOWN_OFFSET) {
            String where = "at byte " + context.offset()
                    + String.format(" (0x%06x)", context.offset());
            if (sb.isEmpty()) {
                sb.append(where);
            }
            else {
                sb.append(' ').append(where);
            }
        }
        return sb.toString();
    }

    private static void append(StringBuilder sb, String part) {
        if (!sb.isEmpty()) {
            sb.append(", ");
        }
        sb.append(part);
    }


    /// The `[file] clause — ` prefix a message carries in front of what went
    /// wrong. Reached through [ReadScope.Place#describe], which is the only
    /// thing that builds a [ReadContext].
    static String prefix(ReadContext context) {
        String clause = contextClause(context);
        return filePrefix(context.fileName()) + (clause.isEmpty() ? "" : clause + " — ");
    }



    /// Amends the exception message with a `[fileName] ` prefix. Preserves the
    /// original exception type and cause chain. Returns the original exception
    /// unchanged when the file name is unavailable or the prefix is already present.
    ///
    /// @param fileName the originating file name, may be `null`
    /// @param e        the exception to enrich
    /// @return the enriched (or original) exception — never `null`
    public static RuntimeException addFileContext(String fileName, RuntimeException e) {
        return addFileContext(fileName, e, null);
    }

    private static RuntimeException addFileContext(String fileName, RuntimeException e,
            ReadContext context) {
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
        return rewrap(e, prefix
                + (originalMessage != null ? originalMessage : e.getClass().getSimpleName()),
                context);
    }

    /// Re-throws `e`'s content under `newMessage`, preserving its type and cause
    /// chain. Shared by the file-name prefix and the read-context clause so a
    /// message gains context without changing what a caller can catch.
    ///
    /// **Cause-chain note for [UncheckedIOException]:** the type requires an
    /// [IOException] cause, so the original is attached as suppressed and the
    /// cause slot holds the inner [IOException].
    private static RuntimeException rewrap(RuntimeException e, String newMessage,
            ReadContext context) {
        String originalMessage = e.getMessage();
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
        // `instanceof`, not an exact class match: indexing an array throws
        // ArrayIndexOutOfBoundsException, and an exact match sent it to the
        // reflective path below, where it has no (String, Throwable)
        // constructor and came out a bare RuntimeException. A corrupt
        // dictionary reference reaches a caller this way, and one that catches
        // on the type — dive does — stopped seeing it.
        if (e instanceof IndexOutOfBoundsException) {
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
