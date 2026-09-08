/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal;

import dev.hardwood.internal.ExceptionContext.ReadContext;
import dev.hardwood.internal.ExceptionContext.ReadContext.Region;
import dev.hardwood.metadata.FieldPath;
import dev.hardwood.reader.ParquetReadException;

/// Where the read currently is, as a scope rather than as an argument.
///
/// A failure's position is a property of the work in progress, not of the
/// exception: the code that knows it is reading a bloom filter is the code
/// running, and by the time a failure has travelled to a frame that can report
/// it, that knowledge is gone. Every catch site that names a region today is
/// restating what its own call stack already said.
///
/// So the region is entered, not passed:
///
/// ```java
/// try (ReadScope.Scope ignored = ReadScope.region(Region.FOOTER, footerStart)) {
///     return FileMetaDataReader.read(reader);
/// }
/// ```
///
/// and one enricher reads it back. Scopes nest and the innermost wins, which is
/// the rule the reader already wanted: a page header inside a chunk fetch is a
/// page-header failure.
///
/// **The region's own first byte comes with it.** A raise site states a message
/// and nothing else; the byte a reader is sent to is the region's address. The
/// one exception is a parse that knows how far it got, which says so through
/// [#stoppedAt].
public final class ReadScope {

    private static final ThreadLocal<Place> CURRENT = new ThreadLocal<>();

    private ReadScope() {
    }

    /// Where the read is, or `null` outside any scope.
    ///
    /// Public so that a fetch log can name the same place an exception would,
    /// rather than composing a second description of it from a string.
    public static Place current() {
        return CURRENT.get();
    }

    /// Opens the file being read. Narrows nothing else: a file is entered
    /// before there is a row group or a column to speak of.
    public static Scope file(String fileName) {
        return set(new Place(fileName, ReadContext.UNKNOWN_ROW_GROUP, null, null,
                ReadContext.UNKNOWN_OFFSET));
    }

    /// Narrows to one column chunk, keeping the file.
    public static Scope column(int rowGroup, FieldPath column) {
        return column(rowGroup, String.valueOf(column));
    }

    /// The same, for a chunk named by ordinal because it carries no `meta_data`
    /// to take a path from.
    public static Scope column(int rowGroup, String column) {
        return set(columnPlace(rowGroup, column));
    }

    private static Place columnPlace(int rowGroup, String column) {
        Place outer = CURRENT.get();
        return new Place(fileNameOf(outer), rowGroup, column, null, ReadContext.UNKNOWN_OFFSET);
    }

    /// An offset the footer need not have given, as a region start.
    public static long orUnknown(Long offset) {
        return offset == null ? ReadContext.UNKNOWN_OFFSET : offset;
    }

    /// Narrows to a region that has no address of its own, keeping the file,
    /// the row group and the column.
    ///
    /// For work that covers a whole region rather than a position in it: a
    /// decode that failed a thousand values into a page is not at the page's
    /// first byte, and naming that byte would send a reader to bytes that are
    /// intact. The row group and column already say which page it was.
    public static Scope region(Region region) {
        return region(region, ReadContext.UNKNOWN_OFFSET);
    }

    /// Narrows to a region whose first byte is known, keeping the file, the row
    /// group and the column.
    public static Scope region(Region region, long regionStart) {
        return set(regionPlace(region, regionStart));
    }

    private static Place regionPlace(Region region, long regionStart) {
        Place outer = CURRENT.get();
        return outer == null
                ? new Place(null, ReadContext.UNKNOWN_ROW_GROUP, null, region, regionStart)
                : new Place(outer.fileName(), outer.rowGroup(), outer.column(), region,
                        regionStart);
    }

    /// Re-establishes a captured place on this thread, for work that crossed a
    /// thread boundary. The page decoded on a pool thread belongs to the row
    /// group the retriever read it for, not to whatever that thread did last.
    public static Scope resume(Place place) {
        return set(place);
    }

    /// Where the read is, as a message prefix: `[file] row group N, column X, region at
    /// byte B — `, or the empty string outside any read.
    ///
    /// For the failures that cannot carry a place of their own. An [java.io.IOException] is
    /// the type a caller catches and the type the reader declares, so it stays that type and
    /// takes its position as text rather than gaining a subclass to hold one.
    public static String here() {
        Place place = CURRENT.get();
        return place == null ? "" : place.describe();
    }

    /// The file being read, as a message prefix: `[file] `, or the empty string outside any
    /// read.
    ///
    /// For a correct file this library will not read. The file is not at fault, so no byte
    /// of it is worth naming and the remedy is the same wherever the limit was met.
    public static String fileHere() {
        Place place = CURRENT.get();
        return place == null ? "" : ExceptionContext.filePrefix(place.fileName());
    }

    /// A failure `bytesIntoRegion` bytes into the region being read, positioned
    /// at that byte rather than at the region's first.
    ///
    /// The one thing a raise site knows that the scope cannot: a region says
    /// where a read began, and only the read itself says where it stopped. Used
    /// by the Thrift reader, whose buffer is a region and whose position in it
    /// is the byte to go and look at.
    ///
    /// A buffer that is not part of any file — a hand-built struct in a test,
    /// a footer the CLI read for itself — has no address to add to, and the
    /// failure then says what went wrong without inventing a byte.
    public static ParquetReadException stoppedAt(String message, int bytesIntoRegion) {
        Place here = CURRENT.get();
        if (here == null || here.regionStart() == ReadContext.UNKNOWN_OFFSET) {
            return new ParquetReadException(message);
        }
        try (Scope stopped = set(here.at(here.regionStart() + bytesIntoRegion))) {
            return new ParquetReadException(message);
        }
    }

    /// Narrows to a region that begins where the enclosing one does.
    ///
    /// A page's header starts where the page starts, so the step that parses it
    /// names what it is doing without being told an offset it would only be
    /// copying from its caller.
    public static Scope narrow(Region region) {
        Place outer = CURRENT.get();
        return region(region, outer == null ? ReadContext.UNKNOWN_OFFSET : outer.regionStart());
    }

    private static Scope set(Place place) {
        Place previous = CURRENT.get();
        CURRENT.set(place);
        return new Scope(previous);
    }

    private static String fileNameOf(Place place) {
        return place == null ? null : place.fileName();
    }

    /// Where a read is: the file, and as much of the way into it as has been
    /// entered.
    ///
    /// `regionStart` is the byte a failure raised here sends a reader to. Where
    /// a read knows how far it got, [#stoppedAt] narrows it to that byte before
    /// the failure is built, so nothing downstream has to compose a position
    /// out of two halves.
    public record Place(String fileName, int rowGroup, String column, Region region,
            long regionStart) {

        /// This place as a message names it, ending in the separator that
        /// divides where a failure was from what it was, or the empty string
        /// where it names nothing.
        public String describe() {
            return ExceptionContext.prefix(
                    new ReadContext(fileName, rowGroup, column, region, regionStart));
        }

        /// The same place, in a region the caller knows and this one does not.
        public Place withRegion(Region region) {
            return new Place(fileName, rowGroup, column, region, regionStart);
        }

        /// The same place, at a byte inside the region rather than at its first.
        Place at(long offset) {
            return new Place(fileName, rowGroup, column, region, offset);
        }
    }

    /// Restores the enclosing place on close. Held in a try-with-resources.
    ///
    /// A read is usually entered all at once — a file, then the column in it, then the
    /// region of that column — and the narrowing methods below chain so that reads as one
    /// statement holding one resource:
    ///
    /// ```java
    /// try (ReadScope.Scope scope = ReadScope.file(name)
    ///         .column(rowGroupIndex, columnName)
    ///         .region(Region.DICTIONARY_PAGE, dictionaryOffset)) {
    /// ```
    ///
    /// They narrow the scope already opened rather than opening another, so one place is
    /// restored on close however many of them were called. Where the parts have different
    /// lifetimes — a file entered once with several regions read inside it in turn — the
    /// static entry points still nest, each restoring its own.
    public static final class Scope implements AutoCloseable {

        private final Place previous;

        private Scope(Place previous) {
            this.previous = previous;
        }

        /// Narrows this scope to one column chunk, keeping the file.
        public Scope column(int rowGroup, FieldPath column) {
            return column(rowGroup, String.valueOf(column));
        }

        /// The same, for a chunk named by ordinal because it carries no `meta_data`.
        public Scope column(int rowGroup, String column) {
            CURRENT.set(columnPlace(rowGroup, column));
            return this;
        }

        /// Narrows this scope to a region whose first byte is known.
        public Scope region(Region region, long regionStart) {
            CURRENT.set(regionPlace(region, regionStart));
            return this;
        }

        /// Narrows this scope to a region that has no address of its own.
        public Scope region(Region region) {
            return region(region, ReadContext.UNKNOWN_OFFSET);
        }

        /// Narrows this scope to a region beginning where the enclosing one does.
        public Scope narrow(Region region) {
            Place outer = CURRENT.get();
            return region(region,
                    outer == null ? ReadContext.UNKNOWN_OFFSET : outer.regionStart());
        }

        @Override
        public void close() {
            if (previous == null) {
                CURRENT.remove();
            }
            else {
                CURRENT.set(previous);
            }
        }
    }
}
