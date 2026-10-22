/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

/// [PredicatePathAgreementTest] through the `RowReader` forced onto its record-level path.
class PredicatePathAgreementRecordPathTest extends PredicatePathAgreementTest {

    PredicatePathAgreementRecordPathTest() {
        super(false, true, true);
    }
}
