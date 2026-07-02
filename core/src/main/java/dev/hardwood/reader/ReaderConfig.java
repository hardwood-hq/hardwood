/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.reader;

import java.util.HashMap;
import java.util.Map;

/// Read-time behaviour knobs for [ParquetFileReader].
///
/// A `ReaderConfig` is an immutable value carrying per-read behaviour, as opposed
/// to [dev.hardwood.HardwoodContext], which holds the shared, stateful runtime
/// resources (the decode executor, the native decompression pools). A single
/// context can back many reads with different configurations, so a behaviour knob
/// never forces a fresh thread pool.
///
/// Knobs are **string-keyed** (see [Builder#option]) so an experimental or
/// transitional flag can be added and later retired without changing this type.
/// The reader resolves and validates the keys it recognises; an unrecognised key
/// is ignored, but the reader logs it at `WARNING` when the config is used, so a
/// typo surfaces rather than silently taking the default.
///
/// Obtain the defaults with [#defaults] or set options through [#builder].
public final class ReaderConfig {

    private final Map<String, String> options;

    private ReaderConfig(Map<String, String> options) {
        this.options = options;
    }

    /// The default configuration (no options set).
    public static ReaderConfig defaults() {
        return builder().build();
    }

    /// A builder for setting reader options.
    public static Builder builder() {
        return new Builder();
    }

    /// The options set on this config, as an immutable map. Keys are matched
    /// case-sensitively; the reader interprets and validates them.
    public Map<String, String> options() {
        return options;
    }

    /// Builder for [ReaderConfig].
    public static final class Builder {

        private final Map<String, String> options = new HashMap<>();

        private Builder() {
        }

        /// Sets a reader option. An unknown key is ignored (so a transitional flag
        /// can be retired without breaking callers) but logged at `WARNING` when
        /// the config is used for a read, so a typo in a live key surfaces rather
        /// than silently taking the default.
        public Builder option(String key, String value) {
            options.put(key, value);
            return this;
        }

        /// Builds the immutable configuration.
        public ReaderConfig build() {
            return new ReaderConfig(Map.copyOf(options));
        }
    }
}
