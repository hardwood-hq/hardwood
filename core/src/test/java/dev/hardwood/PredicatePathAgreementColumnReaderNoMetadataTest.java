/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

/// [PredicatePathAgreementTest] through the `ColumnReader` with `hardwood.metadata-filtering=false`.
class PredicatePathAgreementColumnReaderNoMetadataTest extends PredicatePathAgreementTest {

    PredicatePathAgreementColumnReaderNoMetadataTest() {
        super(true, false, false);
    }
}
