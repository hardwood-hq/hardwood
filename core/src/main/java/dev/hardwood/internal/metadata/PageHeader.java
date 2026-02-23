/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.metadata;

/**
 * Header for a page in Parquet.
 */
public record PageHeader(
        PageType type,
        int uncompressedPageSize,
        int compressedPageSize,
        DataPageHeader dataPageHeader,
        DataPageHeaderV2 dataPageHeaderV2,
        DictionaryPageHeader dictionaryPageHeader) {

    public enum PageType {
        DATA_PAGE(0),
        INDEX_PAGE(1),
        DICTIONARY_PAGE(2),
        DATA_PAGE_V2(3);

        private final int thriftValue;

        PageType(int thriftValue) {
            this.thriftValue = thriftValue;
        }

        public static PageType fromThriftValue(int value) {
            for (PageType type : values()) {
                if (type.thriftValue == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown page type: " + value);
        }
    }
}
