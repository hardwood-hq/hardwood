/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.io;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/// Explicitly releases the address space of a memory-mapped file, instead of leaving the
/// mapping for the garbage collector: on Windows a live mapping keeps the file locked, and
/// in a long scan the address space of closed files is exhausted before the collector runs.
///
/// The release goes through `sun.misc.Unsafe.invokeCleaner`, looked up by name so this class
/// references no `sun.*` type directly. Where the runtime does not expose it, the mapping is
/// left for the garbage collector, as before, and [#unmap] reports `false` instead of failing.
final class MappedBuffers {

    /// The `Unsafe` instance, or `null` where the runtime does not expose it.
    private static final Object UNSAFE;

    /// `Unsafe.invokeCleaner(ByteBuffer)` bound to [#UNSAFE], or `null` where the runtime
    /// does not expose it.
    private static final Method INVOKE_CLEANER;

    static {
        Object unsafe = null;
        Method invokeCleaner = null;
        try {
            Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
            Field theUnsafe = unsafeClass.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = theUnsafe.get(null);
            invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
        }
        catch (ReflectiveOperationException | RuntimeException e) {
            // No explicit unmap on this runtime; mappings are left for the collector.
        }
        UNSAFE = unsafe;
        INVOKE_CLEANER = invokeCleaner;
    }

    private MappedBuffers() {
    }

    /// Unmaps `buffer`, releasing its address space now. Returns `true` when the mapping was
    /// released, `false` where the runtime does not expose an unmap and the buffer is left for
    /// the garbage collector. Never throws: a failed release falls back to the collector
    /// rather than failing the close.
    static boolean unmap(MappedByteBuffer buffer) {
        if (INVOKE_CLEANER == null) {
            return false;
        }
        try {
            INVOKE_CLEANER.invoke(UNSAFE, buffer);
            return true;
        }
        catch (ReflectiveOperationException | RuntimeException e) {
            return false;
        }
    }
}
