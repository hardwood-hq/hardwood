/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChargeTest {

    /// A reader done with a charge every reader is done with is a defect of its holder: the charge
    /// was returned already, and its bytes may be read by no one.
    @Test
    void aReaderPastTheLastIsRejected() {
        Charge charge = new Charge();
        charge.reader();

        assertThat(charge.readerDone()).isTrue();
        assertThatThrownBy(charge::readerDone)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("A reader is done with a charge every reader is done with");
    }
}
