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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/// Build-time check that the JDK building the native binary rejects every certificate in
/// `cli/src/main/trust/blocked.certs`.
///
/// native-image bakes the build JDK's `lib/security/blocked.certs` into the binary, and the
/// binary rejects a chain containing a listed certificate even with a trust store passed at run
/// time. A JDK whose list lacks an entry would produce a binary that trusts it, so the native
/// build fails instead.
///
/// The JDK is located the way native-maven-plugin locates `native-image`: `GRAALVM_HOME`, else
/// `JAVA_HOME`, else the `native-image` on the `PATH`.
///
/// Usage: `java -cp ... dev.hardwood.cli.internal.CheckBlockedCertificates <required>`
public final class CheckBlockedCertificates {

    private static final List<String> NATIVE_IMAGE = List.of("native-image", "native-image.cmd", "native-image.exe");

    private CheckBlockedCertificates() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: CheckBlockedCertificates <required>");
            System.exit(1);
        }
        Path jdk = nativeImageJdk(System.getenv("GRAALVM_HOME"), System.getenv("JAVA_HOME"), System.getenv("PATH"));
        Path jdkList = jdk.resolve("lib").resolve("security").resolve("blocked.certs");
        check(jdkList, Path.of(args[0]));
        System.out.println(jdkList + " blocks every certificate in " + args[0]);
    }

    /// The JDK whose `native-image` native-maven-plugin runs.
    ///
    /// @throws IllegalStateException if neither variable is set and no `native-image` is on `path`
    static Path nativeImageJdk(String graalvmHome, String javaHome, String path) throws IOException {
        if (graalvmHome != null && !graalvmHome.isBlank()) {
            return Path.of(graalvmHome);
        }
        if (javaHome != null && !javaHome.isBlank()) {
            return Path.of(javaHome);
        }
        if (path != null) {
            for (String directory : path.split(File.pathSeparator)) {
                for (String name : NATIVE_IMAGE) {
                    Path candidate = Path.of(directory, name);
                    if (!directory.isEmpty() && Files.isRegularFile(candidate)) {
                        return candidate.toRealPath().getParent().getParent();
                    }
                }
            }
        }
        throw new IllegalStateException("Cannot locate the JDK that builds the native image: set GRAALVM_HOME or JAVA_HOME");
    }

    /// @throws IllegalStateException if `jdkList` lacks a fingerprint `required` lists
    static void check(Path jdkList, Path required) throws IOException {
        Set<String> blocked = Set.copyOf(fingerprints(jdkList));
        List<String> missing = new ArrayList<>();
        for (String fingerprint : fingerprints(required)) {
            if (!blocked.contains(fingerprint)) {
                missing.add(fingerprint);
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException(jdkList + " does not block " + String.join(", ", missing)
                    + "; run tools/block-certificates.sh " + jdkList.getParent().getParent().getParent());
        }
    }

    /// The SHA-256 fingerprints a `blocked.certs` file lists: every line other than comments,
    /// blank lines and the `Algorithm=` header.
    public static List<String> fingerprints(Path file) throws IOException {
        return Files.readAllLines(file).stream()
                .map(String::strip)
                .filter(line -> !line.isEmpty() && !line.startsWith("#") && !line.startsWith("Algorithm="))
                .map(line -> line.toUpperCase(Locale.ROOT))
                .collect(Collectors.toList());
    }
}
