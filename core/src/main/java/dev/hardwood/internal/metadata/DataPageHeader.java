/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.metadata;

import dev.hardwood.metadata.Encoding;

/**
 * Header for DataPage (v1).
 */
public record DataPageHeader(
        int numValues,
        Encoding encoding,
        Encoding definitionLevelEncoding,
        Encoding repetitionLevelEncoding) {
}
