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
/// **Each part of a place is entered on its own,** by whichever code owns it and for as
/// long as it holds. A worker that reads one column for its whole life enters that column
/// once, and the file and row group it moves between are entered inside it.
///
/// A place has two independent axes, and entering a point on one leaves the other alone:
/// which column is being read, and where in a file the read is. On the position axis the
/// parts nest — a region lies in a row group, which lies in a file — so entering a coarser
/// one discards the finer ones that described somewhere else. Entering a file therefore
/// leaves no row group behind from the file before it.
///
/// **The region's own first byte comes with it.** A raise site states a message
/// and nothing else; the byte a reader is sent to is the region's address. The
/// one exception is a parse that knows how far it got, which says so through
/// [#stoppedAt].
public final class ReadScope {

    private static final ThreadLocal<Place> CURRENT = new ThreadLocal<>();

    /// The place of a thread that has entered no scope: a read that names nothing.
    private static final Place UNPLACED = new Place(null, ReadContext.UNKNOWN_ROW_GROUP, null,
            null, ReadContext.UNKNOWN_OFFSET);

    private ReadScope() {
    }

    /// Where the read is, or `null` outside any scope.
    ///
    /// Public so that a fetch log can name the same place an exception would,
    /// rather than composing a second description of it from a string.
    public static Place current() {
        return CURRENT.get();
    }

    /// Enters the file being read, keeping the column and discarding where in the
    /// previous file the read was.
    public static Scope file(String fileName) {
        return set(currentPlace().inFile(fileName));
    }

    /// Enters a row group, keeping the file and the column and discarding the region
    /// of the row group before it.
    public static Scope rowGroup(int rowGroup) {
        return set(currentPlace().inRowGroup(rowGroup));
    }

    /// Enters a column, keeping where in the file the read is.
    public static Scope column(FieldPath column) {
        return column(String.valueOf(column));
    }

    /// The same, for a chunk named by ordinal because it carries no `meta_data`
    /// to take a path from.
    public static Scope column(String column) {
        return set(currentPlace().inColumn(column));
    }

    /// An offset the footer need not have given, as a region start.
    public static long orUnknown(Long offset) {
        return offset == null ? ReadContext.UNKNOWN_OFFSET : offset;
    }

    /// Enters a region that has no address of its own, keeping the file, the
    /// row group and the column.
    ///
    /// For work that covers a whole region rather than a position in it: a
    /// decode that failed a thousand values into a page is not at the page's
    /// first byte, and naming that byte would send a reader to bytes that are
    /// intact. The row group and column already say which page it was.
    public static Scope region(Region region) {
        return region(region, ReadContext.UNKNOWN_OFFSET);
    }

    /// Enters a region whose first byte is known, keeping the file, the row
    /// group and the column.
    public static Scope region(Region region, long regionStart) {
        return set(currentPlace().inRegion(region, regionStart));
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

    /// Enters a region that begins where the enclosing one does.
    ///
    /// A page's header starts where the page starts, so the step that parses it
    /// names what it is doing without being told an offset it would only be
    /// copying from its caller.
    public static Scope narrow(Region region) {
        return set(currentPlace().withRegion(region));
    }

    private static Scope set(Place place) {
        Place previous = CURRENT.get();
        CURRENT.set(place);
        return new Scope(previous);
    }

    /// The place to narrow from: the enclosing one, or the one that names
    /// nothing where no scope has been entered.
    private static Place currentPlace() {
        Place place = CURRENT.get();
        return place == null ? UNPLACED : place;
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
            return inRegion(region, regionStart);
        }

        Place inFile(String fileName) {
            return new Place(fileName, ReadContext.UNKNOWN_ROW_GROUP, column, null,
                    ReadContext.UNKNOWN_OFFSET);
        }

        Place inRowGroup(int rowGroup) {
            return new Place(fileName, rowGroup, column, null, ReadContext.UNKNOWN_OFFSET);
        }

        Place inColumn(String column) {
            return new Place(fileName, rowGroup, column, region, regionStart);
        }

        Place inRegion(Region region, long regionStart) {
            return new Place(fileName, rowGroup, column, region, regionStart);
        }

        /// The same place, at a byte inside the region rather than at its first.
        public Place at(long offset) {
            return new Place(fileName, rowGroup, column, region, offset);
        }
    }

    /// Restores the enclosing place on close. Held in a try-with-resources.
    ///
    /// Where several parts of a place are entered together — a file, the row group in it,
    /// the region of that row group — the narrowing methods below chain, so that reads as
    /// one statement holding one resource:
    ///
    /// ```java
    /// try (ReadScope.Scope scope = ReadScope.file(name)
    ///         .rowGroup(rowGroupIndex)
    ///         .region(Region.DICTIONARY_PAGE, dictionaryOffset)) {
    /// ```
    ///
    /// They narrow the scope already opened rather than opening another, so one place is
    /// restored on close however many of them were called. Where the parts have different
    /// lifetimes — a column entered once with several row groups read inside it in turn —
    /// the static entry points still nest, each restoring its own.
    public static final class Scope implements AutoCloseable {

        private final Place previous;

        private Scope(Place previous) {
            this.previous = previous;
        }

        /// Narrows this scope to a file, keeping the column and discarding where in the
        /// previous file the read was.
        public Scope file(String fileName) {
            return to(currentPlace().inFile(fileName));
        }

        /// Narrows this scope to a row group, keeping the file and the column.
        public Scope rowGroup(int rowGroup) {
            return to(currentPlace().inRowGroup(rowGroup));
        }

        /// Narrows this scope to a column, keeping where in the file the read is.
        public Scope column(FieldPath column) {
            return column(String.valueOf(column));
        }

        /// The same, for a chunk named by ordinal because it carries no `meta_data`.
        public Scope column(String column) {
            return to(currentPlace().inColumn(column));
        }

        /// Narrows this scope to a region whose first byte is known.
        public Scope region(Region region, long regionStart) {
            return to(currentPlace().inRegion(region, regionStart));
        }

        /// Narrows this scope to a region that has no address of its own.
        public Scope region(Region region) {
            return region(region, ReadContext.UNKNOWN_OFFSET);
        }

        /// Narrows this scope to a region beginning where the enclosing one does.
        public Scope narrow(Region region) {
            return to(currentPlace().withRegion(region));
        }

        private Scope to(Place place) {
            CURRENT.set(place);
            return this;
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
