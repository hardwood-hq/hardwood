/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.writer;

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

    private final int pageTargetBytes;
    private final long rowGroupTargetBytes;
    private final String createdBy;

    private WriterConfig(Builder builder) {
        this.pageTargetBytes = builder.pageTargetBytes;
        this.rowGroupTargetBytes = builder.rowGroupTargetBytes;
        this.createdBy = builder.createdBy;
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

    /// Builder for [WriterConfig].
    public static final class Builder {

        private int pageTargetBytes = DEFAULT_PAGE_TARGET_BYTES;
        private long rowGroupTargetBytes = DEFAULT_ROW_GROUP_TARGET_BYTES;
        private String createdBy = DEFAULT_CREATED_BY;

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

        /// Builds the immutable configuration.
        public WriterConfig build() {
            return new WriterConfig(this);
        }
    }
}
