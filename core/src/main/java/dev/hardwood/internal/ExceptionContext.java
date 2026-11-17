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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletionException;

import dev.hardwood.reader.ParquetReadException;
import dev.hardwood.reader.SchemaIncompatibleException;
import dev.hardwood.row.VariantTypeException;

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
    /// an [java.util.OptionalInt] would allocate. A caller sees them only in the message
    /// prefix; no exception carries them as fields.
    public static final int UNKNOWN_ROW_GROUP = -1;

    /// The page of a read that is on none.
    public static final int UNKNOWN_PAGE = -1;

    /// The chunk's dictionary page, which has no ordinal among the data pages.
    public static final int DICTIONARY_PAGE = -2;

    private static final String INTERNAL_PACKAGE = "dev.hardwood.internal";

    /// How an exception of each type the reader raises is wrapped in a new one of the same type,
    /// keyed by its exact class; see [#wrapInSameType].
    private static final Map<Class<? extends RuntimeException>, SameTypeWrapper> SAME_TYPE_WRAPPERS =
            Map.ofEntries(
                    Map.entry(IllegalArgumentException.class, IllegalArgumentException::new),
                    Map.entry(IllegalStateException.class, IllegalStateException::new),
                    Map.entry(UnsupportedOperationException.class, UnsupportedOperationException::new),
                    Map.entry(NullPointerException.class,
                            (message, e) -> caused(new NullPointerException(message), e)),
                    Map.entry(IndexOutOfBoundsException.class,
                            (message, e) -> caused(new IndexOutOfBoundsException(message), e)),
                    // What a typed accessor raises when asked for a type its column does not
                    // hold: the cast the decode makes, which the caller is told about as that cast.
                    Map.entry(ClassCastException.class,
                            (message, e) -> caused(new ClassCastException(message), e)),
                    // What a `TIME` outside a day raises on its way to a `LocalTime`, which the
                    // CLI's value markers catch by type.
                    Map.entry(DateTimeException.class, DateTimeException::new),
                    Map.entry(NoSuchElementException.class, NoSuchElementException::new),
                    Map.entry(VariantTypeException.class,
                            (message, e) -> caused(new VariantTypeException(message), e)),
                    Map.entry(ParquetReadException.class, ParquetReadException::new),
                    Map.entry(SchemaIncompatibleException.class, SchemaIncompatibleException::new),
                    Map.entry(CompletionException.class, CompletionException::new));

    private static final String SUPPLIED_FOOTER_HINT =
            " (footer supplied by the MetadataSource; the file may have changed since the footer was read)";

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

    /// What a runtime failure raised while interpreting file bytes means.
    ///
    /// A corrupt dictionary index reaches a decoder as an
    /// [ArrayIndexOutOfBoundsException], an impossible RLE run header as an
    /// [IllegalStateException], a length that will not fit as an
    /// [ArithmeticException]; a malformed footer value or Variant buffer reaches
    /// the boundaries that call this as whatever its guard raised. Every one of
    /// them is the file being wrong, and every one of them reads to a user as a
    /// defect in this library. They become a [ParquetReadException] keeping the
    /// original as their cause.
    ///
    /// A [ParquetReadException] subclass under `dev.hardwood.internal` — a truncated Thrift
    /// structure raises one — is restated as a plain [ParquetReadException] keeping it as the
    /// cause: callers catch the public type, and must not be handed one they cannot name.
    ///
    /// Three kinds otherwise pass through unchanged. An [UncheckedIOException] is the
    /// transport: an [dev.hardwood.InputFile] is implementable from outside, and
    /// one that answers a failed `readRange` with the unchecked form is still
    /// describing the transport, so it must not be relabelled as the file being
    /// wrong. A [ParquetReadException] already says what it is — including a
    /// [dev.hardwood.reader.SchemaIncompatibleException]. And an
    /// [UnsupportedOperationException] is a codec library that is absent or an
    /// encoding not implemented, which is this library's limit rather than a
    /// fault in the file.
    ///
    /// The cost is that a defect of ours reaching a decoder is reported as a
    /// problem with the file. That is the rarer mistake: without this, every
    /// corrupt file is reported as a defect of ours.
    ///
    /// This mapping is the judgement the reader's exception model rests on, and
    /// it is asserted directly for every arm of it rather than only through a
    /// corrupt file. A checked [IOException] never reaches it — the decoded
    /// column routes transport failures around it — and an [Error] propagates
    /// as it was raised.
    ///
    /// @param e the runtime failure raised while reading file content
    /// @return the failure classified as a read error, or the original pass-through exception
    public static RuntimeException asReadFailure(RuntimeException e) {
        if (isInternalReadFailure(e)) {
            return new ParquetReadException(e.getMessage(), e);
        }
        if (e instanceof UncheckedIOException
                || e instanceof ParquetReadException
                || e instanceof UnsupportedOperationException) {
            return e;
        }
        return new ParquetReadException(
                e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName(), e);
    }

    /// Amends the exception message with a `[fileName] ` prefix. Preserves the
    /// original exception type and cause chain. Returns the original exception
    /// unchanged when the file name is unavailable or the prefix is already present.
    ///
    /// Only the types the reader raises itself are preserved ([#wrapInSameType]). Any other
    /// [ParquetReadException] subclass leaves as a plain [ParquetReadException] and any other
    /// type as a [RuntimeException], with the original as its cause; an internal
    /// [ParquetReadException] subclass is restated that way even when the prefix is already
    /// present (see [#asReadFailure]).
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
        String originalMessage = e.getMessage();
        Throwable cause = e.getCause();
        // If the cause already carries file context (e.g. assembly-thread error
        // propagated through CompletionException), don't add a second layer.
        if (fileName == null || fileName.isEmpty()
                || hasFilePrefix(originalMessage)
                || (cause != null && hasFilePrefix(cause.getMessage()))) {
            return isInternalReadFailure(e) ? new ParquetReadException(originalMessage, e) : e;
        }
        String prefix = readPrefix(fileName, rowGroup, column, page);
        String newMessage = prefix + (originalMessage != null ? originalMessage : e.getClass().getSimpleName());
        // an internal ParquetReadException subclass is not among the types kept, so it leaves as
        // a plain ParquetReadException
        return wrapInSameType(newMessage, e);
    }

    /// Appends to a read failure of a file whose footer a [dev.hardwood.MetadataSource] supplied
    /// that the footer was supplied, and that the file may have changed since the footer was read.
    ///
    /// Only a plain [ParquetReadException] gains it, a failure of the file's bytes to say what the
    /// footer locates in them. A [dev.hardwood.reader.SchemaIncompatibleException] compares
    /// footers, which a changed file does not explain; any other type is not the file's bytes at
    /// all. The result is a [ParquetReadException] with `e` as its cause.
    ///
    /// @param e the read failure, already classified and placed
    /// @return the failure with the hint, or `e` unchanged when it is not a plain
    ///         [ParquetReadException] or already carries the hint
    public static RuntimeException addSuppliedFooterHint(RuntimeException e) {
        String message = e.getMessage();
        if (e.getClass() != ParquetReadException.class || message == null
                || message.endsWith(SUPPLIED_FOOTER_HINT)) {
            return e;
        }
        return new ParquetReadException(message + SUPPLIED_FOOTER_HINT, e);
    }

    /// Classifies a runtime failure with [#asReadFailure], then names where the read was with
    /// [#addReadContext] — what the read pipeline does at its boundaries, for a read made at the
    /// bytes outside it.
    ///
    /// @param fileName the originating file name, may be `null`
    /// @param rowGroup the row group being read, or [#UNKNOWN_ROW_GROUP]
    /// @param column   the column being read, may be `null`
    /// @param e        the failure to classify and place
    /// @return the classified, placed failure — never `null`
    public static RuntimeException readFailureAt(String fileName, int rowGroup, String column,
            RuntimeException e) {
        return addReadContext(fileName, rowGroup, column, asReadFailure(e));
    }

    /// Names where a read was on an [IOException], as a fresh `IOException` with `e` as its
    /// cause. Returns `e` unchanged when the file name is unavailable or `e`, or its cause,
    /// already names a file.
    ///
    /// @param fileName the originating file name, may be `null`
    /// @param rowGroup the row group being read, or [#UNKNOWN_ROW_GROUP]
    /// @param column   the column being read, may be `null`
    /// @param page     the page's ordinal, [#DICTIONARY_PAGE], or [#UNKNOWN_PAGE]
    /// @param e        the transport failure to place
    /// @return the placed (or original) exception — never `null`
    public static IOException addReadContext(String fileName, int rowGroup, String column, int page,
            IOException e) {
        Throwable cause = e.getCause();
        if (fileName == null || fileName.isEmpty()
                || hasFilePrefix(e.getMessage())
                || (cause != null && hasFilePrefix(cause.getMessage()))) {
            return e;
        }
        return new IOException(readPrefix(fileName, rowGroup, column, page)
                + (e.getMessage() != null ? e.getMessage() : "I/O failure"), e);
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

    /// `e` wrapped in a new exception of its own type with `message` and `e` as its cause. An
    /// [UncheckedIOException] keeps its [IOException] as its cause instead, with `e` kept as
    /// suppressed.
    ///
    /// The same type is kept for every type the reader raises, keyed by exact class, since a
    /// subclass wrapped as its supertype would leave as that supertype; an
    /// [UncheckedIOException] of any class leaves as one, the type its declared cause allows. Any
    /// other type leaves as a [ParquetReadException] if it is one, since what callers catch is the
    /// base type and the file is still what is wrong, and as a [RuntimeException] otherwise.
    ///
    /// No constructor is found reflectively: a native image keeps none it was not told of, so a
    /// reflective wrap would keep a type on the JVM that a native image loses, out of reach there
    /// of the `catch` clauses callers, the CLI's among them, write for it.
    static RuntimeException wrapInSameType(String message, RuntimeException e) {
        if (e instanceof UncheckedIOException unchecked) {
            return uncheckedIo(message, unchecked);
        }
        SameTypeWrapper wrapper = SAME_TYPE_WRAPPERS.get(e.getClass());
        if (wrapper != null) {
            return wrapper.wrap(message, e);
        }
        return e instanceof ParquetReadException
                ? new ParquetReadException(message, e)
                : new RuntimeException(message, e);
    }

    /// An [UncheckedIOException] with `message`. Not a case the reader produces: every wrap it
    /// makes to leave a lambda is undone by the method enclosing that lambda, and
    /// `InputFile.readRange` declares `IOException`, so an implementation has a checked channel
    /// and no reason to reach for the unchecked one. It is wrapped because [#addReadContext]
    /// takes an arbitrary `RuntimeException` and must not silently change its type.
    ///
    /// Its cause must be an [IOException], so the original cannot be chained; it is kept as
    /// suppressed.
    private static UncheckedIOException uncheckedIo(String message, UncheckedIOException e) {
        IOException ioCause = e.getCause();
        if (ioCause == null) {
            ioCause = new IOException(e.getMessage());
        }
        UncheckedIOException wrapped = new UncheckedIOException(message, ioCause);
        wrapped.addSuppressed(e);
        return wrapped;
    }

    /// Wraps an exception in a new one of its own type with a message.
    @FunctionalInterface
    private interface SameTypeWrapper {
        RuntimeException wrap(String message, RuntimeException original);
    }

    private static <T extends RuntimeException> T caused(T wrapper, Throwable cause) {
        wrapper.initCause(cause);
        return wrapper;
    }

    /// Whether `e` is a [ParquetReadException] of a type callers cannot name, one declared
    /// under `dev.hardwood.internal`.
    private static boolean isInternalReadFailure(RuntimeException e) {
        if (!(e instanceof ParquetReadException)) {
            return false;
        }
        String packageName = e.getClass().getPackageName();
        return packageName.equals(INTERNAL_PACKAGE) || packageName.startsWith(INTERNAL_PACKAGE + ".");
    }

    private static boolean hasFilePrefix(String message) {
        if (message == null || message.isEmpty() || message.charAt(0) != '[') {
            return false;
        }
        return message.indexOf("] ") > 1;
    }
}
