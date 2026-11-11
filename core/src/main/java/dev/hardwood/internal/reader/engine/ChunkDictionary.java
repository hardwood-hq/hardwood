/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import java.util.ArrayList;
import java.util.List;

import dev.hardwood.internal.reader.batch.Dictionary;

/// A column chunk's dictionary in a read: its values once its dictionary page has decoded, or from
/// the start where the unit's probe decoded them, the data pages that wait for them, and its
/// readers, the last of which lets go of the values. The working set charges the values, so a
/// dictionary holds no charge of R. Used under the read's lock.
final class ChunkDictionary {

    final List<TrackedPage> waiting = new ArrayList<>();
    Dictionary values;
    boolean arrived;
    private int readers;

    /// A dictionary whose values have arrived.
    static ChunkDictionary arrived(Dictionary values) {
        ChunkDictionary d = new ChunkDictionary();
        d.values = values;
        d.arrived = true;
        return d;
    }

    void reader() {
        readers++;
    }

    /// A reader is done; the last lets go of the values.
    void readerDone() {
        if (--readers == 0) {
            values = null;
        }
    }
}
