/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PqFileTest {

    @Test
    void factoriesRefuseWhatNoReferenceHolds() {
        assertThatThrownBy(() -> PqFile.inline(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("bytes");
        assertThatThrownBy(() -> PqFile.external(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("uri");
        assertThatThrownBy(() -> PqFile.external(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("uri must not be empty");
        assertThatThrownBy(() -> PqFile.external("s3://b/k", -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("size must not be negative: -1");
        assertThatThrownBy(() -> PqFile.external("s3://b/k", -2, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("offset must not be negative: -2");
        assertThatThrownBy(() -> PqFile.external("s3://b/k").withContentType(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("mediaType must not be empty");
        assertThatThrownBy(() -> PqFile.external("s3://b/k").withChecksum(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("token must not be empty");
    }

    @Test
    void aFieldIsSetOnce() {
        assertThatThrownBy(() -> PqFile.inline(new byte[1]).withInline(new byte[1]))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("This FILE reference already has inline");
        assertThatThrownBy(() -> PqFile.external("a").withContentType("x/y").withContentType("x/z"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("This FILE reference already has content_type");
        assertThatThrownBy(() -> PqFile.external("a").withChecksum("MD5:00").withChecksum("MD5:01"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("This FILE reference already has checksum");
    }

    /// The token is checked when it is written, not when the reference is built, so a reference read
    /// from a file holding a malformed one still reads.
    @Test
    void aChecksumIsStoredAsGiven() {
        assertThat(PqFile.external("s3://b/k").withChecksum("not a token").checksum()).isEqualTo("not a token");
    }

    @Test
    void aWholeObjectReferenceStoresNoRange() {
        PqFile file = PqFile.external("s3://b/k");

        assertThat(file.hasOffset()).isFalse();
        assertThat(file.offset()).isZero();
        assertThat(file.hasSize()).isFalse();
        assertThatThrownBy(file::size)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("This FILE reference stores no size");
        assertThat(file.contentType()).isNull();
        assertThat(file.checksum()).isNull();
        assertThat(file.inline()).isNull();
    }

    @Test
    void inlineBytesAreCopiedInAndOut() {
        byte[] bytes = {1, 2, 3};
        PqFile file = PqFile.inline(bytes);
        bytes[0] = 9;
        file.inline()[1] = 9;

        assertThat(file.inline()).containsExactly(1, 2, 3);
    }

    @Test
    void equalityComparesFieldsAndInlineContent() {
        PqFile a = PqFile.external("s3://b/k", 4, 5).withInline(new byte[] {1}).withContentType("x/y");
        PqFile b = PqFile.external("s3://b/k", 4, 5).withInline(new byte[] {1}).withContentType("x/y");

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(PqFile.external("s3://b/k", 4, 5).withInline(new byte[] {2}).withContentType("x/y"));
        assertThat(PqFile.external("s3://b/k", 5)).isNotEqualTo(PqFile.external("s3://b/k", 0, 5));
    }

    @Test
    void rendersTheFieldsItHas() {
        assertThat(PqFile.external("s3://b/k", 4, 5).withInline(new byte[3]).withChecksum("MD5:00"))
                .hasToString("PqFile{uri=s3://b/k, offset=4, size=5, checksum=MD5:00, inline=3 bytes}");
        assertThat(PqFile.inline(new byte[0]).withContentType("text/plain"))
                .hasToString("PqFile{contentType=text/plain, inline=0 bytes}");
    }
}
