/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import dev.hardwood.internal.reader.batch.Dictionary;

/// A column chunk's dictionary in a read: its values, the [Output] of its dictionary page's decode
/// or ended from the start where the unit's probe decoded them, which the chunk's data pages wait
/// on; and its readers, the last of which lets go of the values. The working set charges the
/// values, so a dictionary holds no charge of R. Used under the read's lock.
final class ChunkDictionary {

    private final Output<Dictionary> values;
    private int readers;

    ChunkDictionary() {
        this(new Output<>());
    }

    private ChunkDictionary(Output<Dictionary> values) {
        this.values = values;
    }

    /// A dictionary the unit's probe decoded, whose values are there from the start.
    static ChunkDictionary preloaded(Dictionary values) {
        return new ChunkDictionary(Output.of(values));
    }

    /// Its dictionary page's decode's output: its values, or the decode's failure.
    Output<Dictionary> output() {
        return values;
    }

    /// Its values, `null` until they have arrived and once the last reader is done.
    Dictionary values() {
        return values.value();
    }

    /// Whether its values have arrived.
    boolean hasArrived() {
        return values.hasValue();
    }

    void addReader() {
        readers++;
    }

    /// A reader is done; the last lets go of the values.
    void readerDone() {
        if (--readers == 0) {
            values.drop();
        }
    }
}
