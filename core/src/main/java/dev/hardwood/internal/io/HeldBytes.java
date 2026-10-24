/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.io;

import java.nio.ByteBuffer;

import dev.hardwood.InputFile;

/// An [InputFile] some of whose bytes the reader slices without a request: a file in memory all
/// of them, for as long as it is open, and a file whose tail the reader took the tail, until the
/// read is done with the file. The reader asks a file what it holds through here, never by its
/// class; a file that is no `HeldBytes` holds nothing.
public interface HeldBytes {

    /// A slice of `[offset, offset + length)` where the file holds all of it and slicing it takes
    /// no I/O, `null` otherwise: never a request, also where the held bytes are dropped meanwhile.
    /// A file mapped by region holds every byte but maps each range as it is read, so it slices
    /// none here.
    ByteBuffer sliceHeld(long offset, int length);

    /// The offset from which the file holds every byte to its end, [Long#MAX_VALUE] once it holds
    /// none; `0` for a file in memory.
    default long heldFrom() {
        return 0;
    }

    /// Whether the file holds every byte for as long as it is open, so that a range of it can be
    /// sliced as late as it is read, as a file in memory does; bytes held otherwise may be
    /// dropped, so a range of them is sliced while they are held.
    default boolean holdsAll() {
        return true;
    }

    /// Drops the held bytes once the read is done with the file, so a read of them is a request
    /// again; nothing for a file in memory.
    default void dropHeld() {
    }

    /// Whether `inputFile` holds every byte for as long as it is open ([#holdsAll]).
    static boolean inMemory(InputFile inputFile) {
        return inputFile instanceof HeldBytes held && held.holdsAll();
    }

    /// The offset from which `inputFile` holds every byte to its end ([#heldFrom]), [Long#MAX_VALUE]
    /// for a file that holds none.
    static long heldFrom(InputFile inputFile) {
        return inputFile instanceof HeldBytes held ? held.heldFrom() : Long.MAX_VALUE;
    }

    /// A slice of `[offset, offset + length)` of `inputFile` where it holds all of it
    /// ([#sliceHeld]), `null` otherwise.
    static ByteBuffer slice(InputFile inputFile, long offset, int length) {
        return inputFile instanceof HeldBytes held ? held.sliceHeld(offset, length) : null;
    }

    /// Drops what `inputFile` holds ([#dropHeld]); nothing for a file that holds nothing.
    static void drop(InputFile inputFile) {
        if (inputFile instanceof HeldBytes held) {
            held.dropHeld();
        }
    }
}
