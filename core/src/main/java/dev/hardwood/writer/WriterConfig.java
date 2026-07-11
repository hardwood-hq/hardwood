/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

import dev.hardwood.metadata.CompressionCodec;

/// Tuning knobs for [ParquetFileWriter].
///
/// The two size targets bound the writer's output granularity and peak memory:
///
/// - **Page target** — the writer splits a column chunk into data pages of at most this
///   many uncompressed bytes.
/// - **Row-group target** — the writer flushes a row group once its buffered
///   uncompressed data reaches this many bytes, bounding how much it holds in memory.
///
/// Obtain the defaults with [#defaults] or override individual knobs through [#builder].
public final class WriterConfig {

    /// Default page target: 1 MiB of uncompressed values per data page.
    public static final int DEFAULT_PAGE_TARGET_BYTES = 1 << 20;

    /// Default row-group target: 128 MiB of uncompressed data per row group.
    public static final long DEFAULT_ROW_GROUP_TARGET_BYTES = 128L << 20;

    /// Default `created_by` identifier written into the file footer.
    public static final String DEFAULT_CREATED_BY = "hardwood";

    /// Default dictionary page-size limit: 1 MiB of dictionary values before a column
    /// chunk falls back to `PLAIN`.
    public static final int DEFAULT_DICTIONARY_PAGE_LIMIT_BYTES = 1 << 20;

    /// Default page compression codec: `ZSTD` when the zstd-jni library is on the classpath,
    /// otherwise `UNCOMPRESSED`. Choosing a codec explicitly through [Builder#codec] still
    /// requires that codec's library and fails at writer creation when it is missing; this
    /// default only avoids imposing the ZSTD dependency on callers who did not ask to compress.
    public static final CompressionCodec DEFAULT_CODEC = defaultCodec();

    private final int pageTargetBytes;
    private final long rowGroupTargetBytes;
    private final String createdBy;
    private final boolean enableDictionary;
    private final int dictionaryPageLimitBytes;
    private final CompressionCodec codec;

    private WriterConfig(Builder builder) {
        this.pageTargetBytes = builder.pageTargetBytes;
        this.rowGroupTargetBytes = builder.rowGroupTargetBytes;
        this.createdBy = builder.createdBy;
        this.enableDictionary = builder.enableDictionary;
        this.dictionaryPageLimitBytes = builder.dictionaryPageLimitBytes;
        this.codec = builder.codec;
    }

    /// The default configuration.
    public static WriterConfig defaults() {
        return builder().build();
    }

    /// A builder pre-populated with the defaults.
    public static Builder builder() {
        return new Builder();
    }

    /// Maximum uncompressed bytes of values per data page.
    public int pageTargetBytes() {
        return pageTargetBytes;
    }

    /// Uncompressed byte threshold at which a row group is flushed.
    public long rowGroupTargetBytes() {
        return rowGroupTargetBytes;
    }

    /// The `created_by` identifier written into the file footer.
    public String createdBy() {
        return createdBy;
    }

    /// Whether eligible columns are dictionary-encoded (with `PLAIN` fallback on overflow).
    public boolean enableDictionary() {
        return enableDictionary;
    }

    /// The dictionary size in bytes at which a column chunk falls back to `PLAIN`.
    public int dictionaryPageLimitBytes() {
        return dictionaryPageLimitBytes;
    }

    /// The codec each page body is compressed with.
    public CompressionCodec codec() {
        return codec;
    }

    /// `ZSTD` when its library is loadable, otherwise `UNCOMPRESSED`. The class is only probed
    /// for presence, not initialized, so picking the default never triggers the native load.
    private static CompressionCodec defaultCodec() {
        try {
            Class.forName("com.github.luben.zstd.Zstd", false, WriterConfig.class.getClassLoader());
            return CompressionCodec.ZSTD;
        }
        catch (ClassNotFoundException e) {
            return CompressionCodec.UNCOMPRESSED;
        }
    }

    /// Builder for [WriterConfig].
    public static final class Builder {

        private int pageTargetBytes = DEFAULT_PAGE_TARGET_BYTES;
        private long rowGroupTargetBytes = DEFAULT_ROW_GROUP_TARGET_BYTES;
        private String createdBy = DEFAULT_CREATED_BY;
        private boolean enableDictionary = true;
        private int dictionaryPageLimitBytes = DEFAULT_DICTIONARY_PAGE_LIMIT_BYTES;
        private CompressionCodec codec = DEFAULT_CODEC;

        private Builder() {
        }

        /// Sets the page target; must be at least one `INT32` (4 bytes).
        public Builder pageTargetBytes(int pageTargetBytes) {
            if (pageTargetBytes < Integer.BYTES) {
                throw new IllegalArgumentException(
                        "pageTargetBytes must be at least " + Integer.BYTES + " but was " + pageTargetBytes);
            }
            this.pageTargetBytes = pageTargetBytes;
            return this;
        }

        /// Sets the row-group target; must be positive.
        public Builder rowGroupTargetBytes(long rowGroupTargetBytes) {
            if (rowGroupTargetBytes <= 0) {
                throw new IllegalArgumentException(
                        "rowGroupTargetBytes must be positive but was " + rowGroupTargetBytes);
            }
            this.rowGroupTargetBytes = rowGroupTargetBytes;
            return this;
        }

        /// Sets the `created_by` footer identifier; must be non-null.
        public Builder createdBy(String createdBy) {
            if (createdBy == null) {
                throw new IllegalArgumentException("createdBy must not be null");
            }
            this.createdBy = createdBy;
            return this;
        }

        /// Enables or disables dictionary encoding. When disabled, every column chunk is
        /// written as `PLAIN` with no dictionary page.
        public Builder enableDictionary(boolean enableDictionary) {
            this.enableDictionary = enableDictionary;
            return this;
        }

        /// Sets the dictionary size in bytes past which a column chunk falls back to `PLAIN`;
        /// must be at least one `INT32` (4 bytes) so at least one value always fits.
        public Builder dictionaryPageLimitBytes(int dictionaryPageLimitBytes) {
            if (dictionaryPageLimitBytes < Integer.BYTES) {
                throw new IllegalArgumentException("dictionaryPageLimitBytes must be at least "
                        + Integer.BYTES + " but was " + dictionaryPageLimitBytes);
            }
            this.dictionaryPageLimitBytes = dictionaryPageLimitBytes;
            return this;
        }

        /// Sets the codec each page body is compressed with; must be non-null. Only
        /// `UNCOMPRESSED` and `ZSTD` are currently supported on the write path, and a
        /// non-`UNCOMPRESSED` codec requires its library on the classpath.
        public Builder codec(CompressionCodec codec) {
            if (codec == null) {
                throw new IllegalArgumentException("codec must not be null");
            }
            this.codec = codec;
            return this;
        }

        /// Builds the immutable configuration.
        public WriterConfig build() {
            return new WriterConfig(this);
        }
    }
}
