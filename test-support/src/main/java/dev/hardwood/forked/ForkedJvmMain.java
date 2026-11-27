/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.forked;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;

/// The child JVM of [ForkedJvm]: runs the tests a selector selects, a test class or one test, and
/// writes each test's start, and each test's and container's end or skip, to a results file as it
/// happens, a line each and flushed at once, so that the file names the test that was running if
/// the JVM exits.
///
/// Lines are tab-separated: `S` and the unique id for a start; `F`, the unique id, the status and
/// the Base64 of the serialized throwable, or nothing, for an end; `K`, the unique id and the
/// Base64 of the reason for a skip.
public final class ForkedJvmMain {

    private static final int PARENT_GONE_STATUS = 2;

    private ForkedJvmMain() {
    }

    /// @param args the results file and the selector, in the form `DiscoverySelectors.parse(String)`
    ///     reads
    public static void main(String[] args) {
        // a child whose launching JVM is gone has no one to report to and no timeout over it
        ProcessHandle.current().parent().ifPresent(parent -> parent.onExit()
                .thenRun(() -> Runtime.getRuntime().halt(PARENT_GONE_STATUS)));
        try {
            run(Path.of(args[0]), args[1]);
        }
        catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        // the tests may leave non-daemon threads behind
        System.exit(0);
    }

    private static void run(Path results, String selector) throws IOException {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.parse(selector)
                        .orElseThrow(() -> new IllegalArgumentException("Not a discovery selector: " + selector)))
                .build();
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(results), true)) {
            LauncherFactory.create().execute(request, new ResultWriter(out));
        }
    }

    private static final class ResultWriter implements TestExecutionListener {

        private final PrintWriter out;

        ResultWriter(PrintWriter out) {
            this.out = out;
        }

        @Override
        public void executionStarted(TestIdentifier test) {
            if (test.isTest()) {
                out.println("S\t" + test.getUniqueId());
            }
        }

        @Override
        public void executionSkipped(TestIdentifier test, String reason) {
            out.println("K\t" + test.getUniqueId() + "\t"
                    + Base64.getEncoder().encodeToString(reason.getBytes(StandardCharsets.UTF_8)));
        }

        @Override
        public void executionFinished(TestIdentifier test, TestExecutionResult result) {
            String throwable = result.getThrowable().map(ResultWriter::encode).orElse("");
            out.println("F\t" + test.getUniqueId() + "\t" + result.getStatus() + "\t" + throwable);
        }

        private static String encode(Throwable throwable) {
            try {
                return Base64.getEncoder().encodeToString(serialize(throwable));
            }
            catch (NotSerializableException e) {
                StringWriter trace = new StringWriter();
                throwable.printStackTrace(new PrintWriter(trace));
                return encode(new AssertionError("The forked JVM's test threw " + trace));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static byte[] serialize(Throwable throwable) throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(throwable);
            }
            return bytes.toByteArray();
        }
    }
}
