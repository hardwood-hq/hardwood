/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.internal;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CheckBlockedCertificatesTest {

    private static final Path REQUIRED = Path.of("src/main/trust/blocked.certs");

    private static final String FIRST = "42C4ACCD12C4F59597925E1A08A7A90102608543809308D50EC427E118BA1493";
    private static final String SECOND = "45A883F349AEDFD5F3A649B5498FFB121712BA8B448F3F2A91DBC5A766FCDDBE";

    @TempDir
    Path dir;

    @Test
    void readsTheFingerprintsOfTheCheckedInList() throws IOException {
        List<String> fingerprints = CheckBlockedCertificates.fingerprints(REQUIRED);

        assertThat(fingerprints).hasSize(16).startsWith(FIRST, SECOND).allMatch(f -> f.matches("[0-9A-F]{64}"));
    }

    @Test
    void passesWhenTheJdkBlocksEveryRequiredFingerprint() throws IOException {
        Path jdk = jdkList("Algorithm=SHA-256", "03DB9E5E79FE6117177F81C11595AF598CB176AF766290DBCEB2C318B32E39A2",
                "# compromised", FIRST.toLowerCase(), SECOND);
        Path required = Files.writeString(dir.resolve("required"), "# two\n" + FIRST + "\n\n" + SECOND + "\n");

        assertThatCode(() -> CheckBlockedCertificates.check(jdk, required)).doesNotThrowAnyException();
    }

    @Test
    void failsWhenTheJdkLacksARequiredFingerprint() throws IOException {
        Path jdk = jdkList("Algorithm=SHA-256", SECOND);
        Path required = Files.writeString(dir.resolve("required"), FIRST + "\n" + SECOND + "\n");

        assertThatThrownBy(() -> CheckBlockedCertificates.check(jdk, required))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(jdk + " does not block " + FIRST + "; run tools/block-certificates.sh " + dir.resolve("jdk"));
    }

    @Test
    void locatesTheJdkLikeNativeMavenPlugin() throws IOException {
        Path onPath = Files.createDirectories(dir.resolve("graal/bin"));
        Files.createFile(onPath.resolve("native-image"));
        String path = dir.resolve("empty") + File.pathSeparator + onPath;

        assertThat(CheckBlockedCertificates.nativeImageJdk("/opt/graal", "/opt/java", path)).isEqualTo(Path.of("/opt/graal"));
        assertThat(CheckBlockedCertificates.nativeImageJdk(" ", "/opt/java", path)).isEqualTo(Path.of("/opt/java"));
        assertThat(CheckBlockedCertificates.nativeImageJdk(null, null, path)).isEqualTo(dir.resolve("graal").toRealPath());
    }

    @Test
    void failsWithoutAJdkToCheck() {
        assertThatThrownBy(() -> CheckBlockedCertificates.nativeImageJdk(null, "", dir.toString()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot locate the JDK that builds the native image: set GRAALVM_HOME or JAVA_HOME");
    }

    /// Writes `lines` as the `blocked.certs` of a JDK at `<dir>/jdk`.
    private Path jdkList(String... lines) throws IOException {
        Path security = Files.createDirectories(dir.resolve("jdk/lib/security"));
        return Files.write(security.resolve("blocked.certs"), List.of(lines));
    }
}
