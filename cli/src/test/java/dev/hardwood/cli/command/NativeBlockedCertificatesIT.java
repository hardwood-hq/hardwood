/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import dev.hardwood.cli.internal.CheckBlockedCertificates;

import static org.assertj.core.api.Assertions.assertThat;

/// native-image bakes the build JDK's `blocked.certs` into the binary, where each entry appears
/// as its fingerprint string. A binary built with a JDK that lacked an entry of
/// `src/main/trust/blocked.certs` trusts that certificate, which the binary itself shows here.
class NativeBlockedCertificatesIT {

    private final String nativeBinary = System.getProperty("native.image.path");

    @Test
    void embedsEveryBlockedCertificate() throws IOException {
        String binary = new String(Files.readAllBytes(Path.of(nativeBinary)), StandardCharsets.ISO_8859_1);

        assertThat(CheckBlockedCertificates.fingerprints(Path.of("src/main/trust/blocked.certs")))
                .allSatisfy(fingerprint -> assertThat(binary).withFailMessage("The binary does not block %s", fingerprint)
                        .contains(fingerprint));
    }
}
