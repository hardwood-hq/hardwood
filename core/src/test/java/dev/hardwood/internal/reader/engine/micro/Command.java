/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

/// Work the [Engine] asks for, as data: the driver runs it through [ReadInterpreter], outside the
/// lock, and posts its result as a [Fact]. The engine never runs work or knows what running it
/// takes.
sealed interface Command {

    /// Fetch `request`'s bytes: I/O, run on a virtual thread; results in [Fact.Fetched].
    record Fetch(Request request) implements Command {
        @Override
        public String toString() {
            return "fetch " + request;
        }
    }

    /// Fetch row group `rowGroup`'s index: I/O; results in [Fact.IndexFetched].
    record FetchIndex(int rowGroup) implements Command {
        @Override
        public String toString() {
            return "fetch index " + rowGroup;
        }
    }

    /// Parse row group `rowGroup`'s index: CPU; results in [Fact.IndexParsed], after which the row
    /// group's pages may be fetched.
    record ParseIndex(int rowGroup) implements Command {
        @Override
        public String toString() {
            return "parse index " + rowGroup;
        }
    }

    /// Decode `page`: CPU, run on the pool; results in [Fact.Decoded].
    record Decode(Page page) implements Command {
        @Override
        public String toString() {
            return "decode " + page;
        }
    }
}
