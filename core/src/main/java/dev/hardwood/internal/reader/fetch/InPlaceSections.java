/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import dev.hardwood.InputFile;
import dev.hardwood.internal.ExceptionContext;
import dev.hardwood.internal.FetchReason;
import dev.hardwood.internal.io.HeldBytes;
import dev.hardwood.internal.io.ReadRanges;

/// The sections of a fetch plan or an index window built outside a read
/// ([SequentialFetchPlan#build]): bytes the file holds (a file in memory, or its tail) are sliced in
/// place as a read slices them, so a walk over a file in memory never stops for bytes; any other
/// section is read on the thread that demands it ([Section#demand], [Section#whenMet]).
public final class InPlaceSections implements Sections {

    @Override
    public List<? extends Section> sections(int workItemIndex) {
        return null;
    }

    @Override
    public List<? extends Section> insert(int fileIndex, int workItemIndex, InputFile inputFile, List<Span> spans) {
        List<Section> inserted = new ArrayList<>(spans.size());
        for (Span span : spans) {
            inserted.add(new InPlaceSection(inputFile, span));
        }
        return inserted;
    }

    @Override
    public Section insertDemanded(int workItemIndex, InputFile inputFile, long start, int length, String reason) {
        return new InPlaceSection(inputFile, new Span(start, length, reason, 1));
    }

    @Override
    public Section planned(InputFile inputFile, long start, int length, String reason, int holders) {
        return new InPlaceSection(inputFile, new Span(start, length, reason, holders));
    }

    /// The section of `span` where the file holds its bytes, `null` where reading it takes a read.
    @Override
    public Section heldInPlace(InputFile inputFile, Span span) {
        return HeldBytes.inMemory(inputFile) || HeldBytes.slice(inputFile, span.start(), span.length()) != null
                ? new InPlaceSection(inputFile, span)
                : null;
    }

    /// The bytes of `span` of `inputFile`: sliced where the file holds them, read once demanded
    /// otherwise, and dropped once every holder has released them.
    private static final class InPlaceSection implements Section {

        private final InputFile inputFile;
        private final Span span;
        private int holders;
        private ByteBuffer bytes;
        private IOException failure;
        private boolean released;

        InPlaceSection(InputFile inputFile, Span span) {
            this.inputFile = inputFile;
            this.span = span;
            this.holders = span.holders();
        }

        @Override
        public long offset() {
            return span.start();
        }

        @Override
        public long end() {
            return span.start() + span.length();
        }

        @Override
        public ByteBuffer poll() throws IOException {
            if (bytes == null && failure == null && !released) {
                bytes = HeldBytes.inMemory(inputFile) ? read() : HeldBytes.slice(inputFile, span.start(), span.length());
            }
            if (bytes == null && failure != null) {
                throw failure;
            }
            return bytes;
        }

        @Override
        public ByteBuffer slice(ByteBuffer polled, long fileOffset, int length) {
            if (fileOffset < offset() || length < 0 || fileOffset + length > end()) {
                throw new IllegalArgumentException("Range [" + fileOffset + ", " + (fileOffset + length)
                        + ") lies outside the section [" + offset() + ", " + end() + ") (" + span.reason() + ")");
            }
            return polled.slice(Math.toIntExact(fileOffset - offset()), length);
        }

        @Override
        public void release() {
            if (--holders <= 0) {
                released = true;
                bytes = null;
            }
        }

        /// Reads the bytes on the calling thread, unless they are present, failed or released.
        @Override
        public void demand() {
            if (bytes != null || failure != null || released) {
                return;
            }
            try {
                bytes = read();
            }
            catch (IOException e) {
                failure = e;
            }
        }

        @Override
        public void discard() {
            released = true;
            bytes = null;
        }

        /// Demands the bytes, and runs `resume` once they are read or failed: at once.
        @Override
        public void whenMet(Runnable resume) {
            demand();
            resume.run();
        }

        private ByteBuffer read() throws IOException {
            try (FetchReason.Scope ignored = FetchReason.set(span.reason())) {
                return ReadRanges.read(inputFile, span.start(), span.length());
            }
            catch (IOException e) {
                throw new IOException(ExceptionContext.filePrefix(inputFile.name()) + "Failed to fetch data at offset "
                        + span.start() + " (length " + span.length() + ")", e);
            }
        }
    }
}
