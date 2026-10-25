/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.fetch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import dev.hardwood.internal.reader.decode.PageInfo;
import dev.hardwood.internal.reader.step.Need;

/// Drives a [PageWalk] for tests that walk a plan outside a read: each section the walk stops on is
/// demanded, which reads it on the calling thread ([InPlaceSections]).
public final class PageWalks {

    private PageWalks() {
    }

    /// The pages of `plan`'s walk.
    public static Pages of(FetchPlan plan) {
        return new Pages(plan.walk());
    }

    /// Every page of `plan`'s walk, in file order.
    public static List<PageInfo> all(FetchPlan plan) throws IOException {
        Pages pages = of(plan);
        List<PageInfo> all = new ArrayList<>();
        while (pages.hasNext()) {
            all.add(pages.next());
        }
        return all;
    }

    /// The pages of one walk.
    public static final class Pages {

        private final PageWalk walk;
        private PageInfo next;
        private boolean ended;

        Pages(PageWalk walk) {
            this.walk = walk;
        }

        /// Whether another page is available.
        ///
        /// @throws IOException if reading the file failed
        /// @throws IllegalStateException if the walk stopped on something other than bytes
        public boolean hasNext() throws IOException {
            while (next == null && !ended) {
                next = walk.advance();
                if (next == null) {
                    Need pending = walk.pending();
                    if (pending == null) {
                        ended = true;
                    }
                    else if (pending instanceof Section section) {
                        section.demand();
                    }
                    else {
                        throw new IllegalStateException("A walk outside a read waits for bytes only, not " + pending);
                    }
                }
            }
            return next != null;
        }

        /// The next page.
        ///
        /// @throws IOException if reading the file failed
        public PageInfo next() throws IOException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            PageInfo result = next;
            next = null;
            return result;
        }
    }
}
