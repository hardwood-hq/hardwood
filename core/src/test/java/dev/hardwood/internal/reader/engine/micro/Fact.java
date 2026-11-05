/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine.micro;

/// What can happen to a read: the result of a [Command], which the driver posts to the [Engine]
/// ([Engine#post]). A fact is data; posting it records it, and only the poll after it decides.
sealed interface Fact {

    /// `request`'s bytes have arrived.
    record Fetched(Request request) implements Fact {
        @Override
        public String toString() {
            return "fetched " + request;
        }
    }

    /// Row group `rowGroup`'s index has arrived.
    record IndexFetched(int rowGroup) implements Fact {
        @Override
        public String toString() {
            return "fetched index " + rowGroup;
        }
    }

    /// Row group `rowGroup`'s index is parsed: the row group is planned.
    record IndexParsed(int rowGroup) implements Fact {
        @Override
        public String toString() {
            return "parsed index " + rowGroup;
        }
    }

    /// `page` is decoded into `values`.
    record Decoded(Page page, long[] values) implements Fact {
        @Override
        public String toString() {
            return "decoded " + page;
        }
    }

    /// `command` failed with `cause`.
    record Failed(Command command, Throwable cause) implements Fact {
        @Override
        public String toString() {
            return "failed " + command + ": " + cause;
        }
    }
}
