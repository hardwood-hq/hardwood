/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.writer;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// [ByteArrayBuilder#reserve], which is how a page's sections are produced into the body that
/// will carry them rather than into arrays the caller copies in.
///
/// The contract has one sharp edge — reserving may replace the backing array — so the offset it
/// returns is only usable against an [ByteArrayBuilder#array()] fetched afterwards. These pin
/// both halves of that.
class ByteArrayBuilderTest {

    @Test
    void reserveReturnsTheOffsetPastWhatWasWritten() {
        ByteArrayBuilder builder = new ByteArrayBuilder();
        builder.write(1);
        builder.write(2);

        int at = builder.reserve(3);

        assertThat(at).isEqualTo(2);
        assertThat(builder.length()).as("the reservation counts as written").isEqualTo(5);
    }

    @Test
    void reservedBytesAreFilledThroughTheArrayAndSurviveLaterWrites() {
        ByteArrayBuilder builder = new ByteArrayBuilder();
        builder.write(9);

        int at = builder.reserve(2);
        builder.array()[at] = 7;
        builder.array()[at + 1] = 8;
        builder.write(6);

        assertThat(Arrays.copyOf(builder.array(), builder.length()))
                .containsExactly((byte) 9, (byte) 7, (byte) 8, (byte) 6);
    }

    @Test
    void reserveGrowsBeyondTheInitialCapacity() {
        // The growth is the reason the offset must be taken first: it replaces the array, so a
        // reference fetched before reserving would point at the old one.
        ByteArrayBuilder builder = new ByteArrayBuilder(4);
        byte[] before = builder.array();

        int at = builder.reserve(64);
        byte[] after = builder.array();

        assertThat(after.length).isGreaterThanOrEqualTo(64);
        assertThat(after).as("the buffer was replaced by the growth").isNotSameAs(before);
        assertThat(at).isZero();
        assertThat(builder.length()).isEqualTo(64);
    }

    @Test
    void reserveKeepsWhatWasAlreadyWrittenWhenItGrows() {
        ByteArrayBuilder builder = new ByteArrayBuilder(2);
        builder.write(4);
        builder.write(5);

        int at = builder.reserve(100);
        Arrays.fill(builder.array(), at, at + 100, (byte) 1);

        assertThat(builder.array()[0]).isEqualTo((byte) 4);
        assertThat(builder.array()[1]).isEqualTo((byte) 5);
        assertThat(builder.length()).isEqualTo(102);
    }

    @Test
    void resetReturnsTheBuilderToEmptyAndReservesFromZeroAgain() {
        ByteArrayBuilder builder = new ByteArrayBuilder();
        builder.reserve(10);
        builder.reset();

        assertThat(builder.length()).isZero();
        assertThat(builder.reserve(1)).isZero();
    }

    @Test
    void reserveRejectsANegativeLength() {
        assertThatThrownBy(() -> new ByteArrayBuilder().reserve(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Cannot reserve a negative length: -1");
    }

    @Test
    void reserveOfNothingIsANoOp() {
        ByteArrayBuilder builder = new ByteArrayBuilder();
        builder.write(3);

        assertThat(builder.reserve(0)).isEqualTo(1);
        assertThat(builder.length()).isEqualTo(1);
    }
}
