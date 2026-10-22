/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

/// [PredicatePathAgreementTest] through the `RowReader` by default, which compiles the filter into batch
/// matchers where it can.
class PredicatePathAgreementRowReaderTest extends PredicatePathAgreementTest {

    PredicatePathAgreementRowReaderTest() {
        super(false, true, false);
    }
}
