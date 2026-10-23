/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader;

import java.io.IOException;
import java.util.NoSuchElementException;

/// A [PageWalk] seen as an iterator, for a thread off the pool: advancing waits for the reads
/// the walk needs.
///
/// [java.util.Iterator] in every respect but one: both methods declare [IOException], because
/// advancing is what reaches the file.
public final class PageIterator {

    private final PageWalk walk;
    private PageInfo next;
    private boolean ended;

    /// @param walk the walk to wait on
    public PageIterator(PageWalk walk) {
        this.walk = walk;
    }

    /// Whether another page is available, waiting for whatever is needed to know.
    ///
    /// @throws IOException if reading the file failed
    public boolean hasNext() throws IOException {
        while (next == null && !ended) {
            next = walk.advance();
            if (next == null) {
                Need pending = walk.pending();
                if (pending == null) {
                    ended = true;
                }
                else if (pending instanceof ByteNeed bytes) {
                    bytes.issue().await();
                }
                else {
                    // Only a read's tasks wait for one another to end ([WorkingSet#admitTask])
                    throw new IllegalStateException("A walk outside a read waits for bytes only, not " + pending);
                }
            }
        }
        return next != null;
    }

    /// The next page.
    ///
    /// @throws IOException if reading the file failed
    /// @throws NoSuchElementException if no page is available
    public PageInfo next() throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        PageInfo result = next;
        next = null;
        return result;
    }

    /// Which page of the column chunk the walk is on; see [PageWalk#currentPage].
    public int currentPage() {
        return walk.currentPage();
    }
}
