/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

/// [PredicatePathAgreementTest] through the `RowReader` with `hardwood.metadata-filtering=false`, which
/// decodes every row group and page and answers from values alone.
class PredicatePathAgreementRowReaderNoMetadataTest extends PredicatePathAgreementTest {

    PredicatePathAgreementRowReaderNoMetadataTest() {
        super(false, false, false);
    }
}
