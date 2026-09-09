/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.thrift;

import java.util.EnumSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import dev.hardwood.metadata.Encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [ThriftEnumLookup]'s encoding table, which is the one place a Thrift encoding value becomes an
/// [Encoding] and back.
///
/// The write path exercises the table for every encoding it emits — `ColumnMetaDataWriter` and
/// `PageHeaderWriter` both resolve one through [ThriftEnumLookup#thriftValue] — so a writable
/// encoding missing from it fails there. What that leaves uncovered is an encoding Hardwood only
/// reads: nothing in production asks for its Thrift value, so a missing or mis-indexed entry does
/// not fail, it *degrades*, and the encoding reads back as [Encoding#UNKNOWN].
///
/// The round trip below closes that. It is driven by [Encoding#values()], so adding an encoding
/// never means editing this file.
class EncodingThriftMappingTest {

    /// The one member that is not an encoding but a stand-in for the ones this release cannot
    /// name. It has no Thrift value by construction, so it is excluded from the round trip and
    /// pinned separately by [#unknownHasNoThriftValue].
    private static final Set<Encoding> NOT_A_WIRE_VALUE = EnumSet.of(Encoding.UNKNOWN);

    /// Every named encoding survives a trip through its Thrift value and back.
    ///
    /// A member missing from the table, or sitting at the wrong index, fails here rather than
    /// silently reading back as [Encoding#UNKNOWN] or displacing its neighbour.
    @ParameterizedTest
    @EnumSource(Encoding.class)
    void roundTripsThroughItsThriftValue(Encoding encoding) {
        if (NOT_A_WIRE_VALUE.contains(encoding)) {
            return;
        }
        int thriftValue = ThriftEnumLookup.thriftValue(encoding);
        assertThat(thriftValue).as("Thrift value of %s", encoding).isNotNegative();
        assertThat(ThriftEnumLookup.encoding(thriftValue))
                .as("%s read back from Thrift value %d", encoding, thriftValue)
                .isEqualTo(encoding);
    }

    /// [Encoding#UNKNOWN] stands for a value this release cannot name, so writing it out would
    /// mean inventing one. Asking for its Thrift value is a defect, not a fallback.
    @Test
    void unknownHasNoThriftValue() {
        assertThatThrownBy(() -> ThriftEnumLookup.thriftValue(Encoding.UNKNOWN))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No Thrift value for encoding: UNKNOWN");
    }

    /// Thrift value 1 is `GROUP_VAR_INT`, withdrawn by the format and never written. The table
    /// carries it as an explicit hole, which must read as unrecognized rather than as whatever
    /// member happens to sit beside it. A value past the table's end is the same question one step
    /// further out, and `UnknownEncodingReadTest` settles that one on a real file.
    @Test
    void theWithdrawnGroupVarIntValueIsUnrecognized() {
        assertThat(ThriftEnumLookup.encoding(1)).isEqualTo(Encoding.UNKNOWN);
    }
}
