/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

import java.util.Objects;
import java.util.function.Function;

import dev.hardwood.reader.BinaryDictionary;

/// Internal bridge giving first-party bindings (such as `hardwood-arrow`) read-only bulk access
/// to a [BinaryDictionary]'s backing [Dictionary.ByteArrayDictionary] without exposing mutable
/// backing arrays or internal types on `BinaryDictionary`'s public API.
///
/// Callers must never mutate the returned `chunkBytes(int)` or `chunkOffsets(int)` arrays.
public final class BinaryDictionaryLayout {

    private static volatile Function<BinaryDictionary, Dictionary.ByteArrayDictionary> unwrapper;

    private BinaryDictionaryLayout() {
    }

    /// Registers `BinaryDictionary`'s package-private extractor when `BinaryDictionary` initializes.
    public static void register(Function<BinaryDictionary, Dictionary.ByteArrayDictionary> extractor) {
        Objects.requireNonNull(extractor, "extractor");
        if (unwrapper != null) {
            throw new IllegalStateException("BinaryDictionaryLayout extractor is already registered");
        }
        unwrapper = extractor;
    }

    /// Returns the backing [Dictionary.ByteArrayDictionary] of `dictionary`.
    public static Dictionary.ByteArrayDictionary unwrap(BinaryDictionary dictionary) {
        Objects.requireNonNull(dictionary, "dictionary");
        return unwrapper.apply(dictionary);
    }
}
