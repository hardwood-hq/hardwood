/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

import java.util.List;

/// Where one column's walk stands: its next page. Answers and records; decides nothing.
final class PageCursor {

    private final List<Page> pages;
    private int next;

    PageCursor(List<Page> pages) {
        this.pages = pages;
    }

    /// The next page, `null` once every page is walked.
    Page next() {
        return next < pages.size() ? pages.get(next) : null;
    }

    /// `page`, the next, is walked.
    void walked(Page page) {
        if (page != next()) {
            throw new IllegalStateException(page + " is not the column's next page, " + next());
        }
        next++;
    }
}
