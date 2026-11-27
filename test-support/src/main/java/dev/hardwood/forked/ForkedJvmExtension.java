/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.forked;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;
import org.junit.platform.commons.support.AnnotationSupport;
import org.opentest4j.TestAbortedException;

/// Runs the tests of a class annotated with [ForkedJvm] in a child JVM ([ForkedJvmMain]), once for
/// the class at its first test, and reports the child's outcome for each test in the launching JVM.
/// A test that a child which exited abnormally did not reach runs in a child of its own, so that
/// one test's crash fails no other; after a child that timed out it fails instead, so that a hang
/// costs one timeout. After the class's tests, a failure of the class's container in the child
/// fails the class. In the child, where [#CHILD_PROPERTY] is set, every invocation proceeds.
public final class ForkedJvmExtension implements InvocationInterceptor, AfterAllCallback {

    static final String CHILD_PROPERTY = "hardwood.forkedJvm.child";

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(ForkedJvmExtension.class);
    /// the store's key, with the class name, of a class's child that timed out running one of its
    /// tests on its own, whose outcome each test not yet reached takes instead of another child
    private static final String TIMED_OUT_KEY = "timed out: ";
    private static final List<String> INHERITED_PREFIXES = List.of("--add-modules=", "--add-opens=",
            "--add-exports=", "--add-reads=", "--enable-native-access=", "-D");

    @Override
    public void interceptTestMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext,
                                    ExtensionContext extensionContext) throws Throwable {
        runInChild(invocation, extensionContext);
    }

    @Override
    public void interceptTestTemplateMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext,
                                            ExtensionContext extensionContext) throws Throwable {
        runInChild(invocation, extensionContext);
    }

    @Override
    public <T> T interceptTestFactoryMethod(Invocation<T> invocation, ReflectiveInvocationContext<Method> invocationContext,
                                            ExtensionContext extensionContext) throws Throwable {
        if (inChild()) {
            return invocation.proceed();
        }
        throw new ExtensionConfigurationException("@ForkedJvm does not run @TestFactory methods: "
                + extensionContext.getUniqueId());
    }

    @Override
    public void interceptBeforeAllMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext,
                                         ExtensionContext extensionContext) throws Throwable {
        proceedInChildOnly(invocation);
    }

    @Override
    public void interceptBeforeEachMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext,
                                          ExtensionContext extensionContext) throws Throwable {
        proceedInChildOnly(invocation);
    }

    @Override
    public void interceptAfterEachMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext,
                                         ExtensionContext extensionContext) throws Throwable {
        proceedInChildOnly(invocation);
    }

    @Override
    public void interceptAfterAllMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext,
                                        ExtensionContext extensionContext) throws Throwable {
        proceedInChildOnly(invocation);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        if (inChild()) {
            return;
        }
        ChildRun run = context.getRoot().getStore(NAMESPACE).get(context.getRequiredTestClass(), ChildRun.class);
        Throwable failure = run == null ? null : run.containerFailure(context.getUniqueId());
        if (failure instanceof Exception exception) {
            throw exception;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException("The forked JVM's class container failed", failure);
        }
    }

    private static boolean inChild() {
        return Boolean.getBoolean(CHILD_PROPERTY);
    }

    private static void proceedInChildOnly(Invocation<Void> invocation) throws Throwable {
        if (inChild()) {
            invocation.proceed();
        }
        else {
            invocation.skip();
        }
    }

    private static void runInChild(Invocation<Void> invocation, ExtensionContext context) throws Throwable {
        if (inChild()) {
            invocation.proceed();
            return;
        }
        invocation.skip();
        Class<?> testClass = context.getRequiredTestClass();
        if (testClass.getEnclosingClass() != null) {
            throw new ExtensionConfigurationException("@ForkedJvm does not run nested test classes: " + testClass.getName());
        }
        String uniqueId = context.getUniqueId();
        ExtensionContext.Store store = context.getRoot().getStore(NAMESPACE);
        ChildRun run = store.computeIfAbsent(testClass, c -> run(c, "class:" + c.getName()), ChildRun.class);
        if (!run.reached(uniqueId)) {
            String timedOutKey = TIMED_OUT_KEY + testClass.getName();
            ChildRun timedOut = store.get(timedOutKey, ChildRun.class);
            if (timedOut != null) {
                run = timedOut;
            }
            else {
                run = run(testClass, "uid:" + uniqueId);
                if (run.timedOut) {
                    store.put(timedOutKey, run);
                }
            }
        }
        run.report(uniqueId);
    }

    /// Runs the tests `selector` selects in a child JVM, in the form
    /// `DiscoverySelectors.parse(String)` reads.
    private static ChildRun run(Class<?> testClass, String selector) {
        ForkedJvm forked = AnnotationSupport.findAnnotation(testClass, ForkedJvm.class)
                .orElseThrow(() -> new ExtensionConfigurationException(testClass.getName() + " is not annotated with @ForkedJvm"));
        Path directory = null;
        Process process = null;
        try {
            directory = Files.createTempDirectory("forked-jvm");
            Path results = directory.resolve("results.txt");
            Path output = directory.resolve("output.txt");
            process = new ProcessBuilder(command(forked, results, selector))
                    .redirectErrorStream(true)
                    .redirectOutput(output.toFile())
                    .start();
            boolean exited = process.waitFor(forked.timeoutSeconds(), TimeUnit.SECONDS);
            if (!exited) {
                process.destroyForcibly().waitFor();
            }
            ChildRun run = new ChildRun(exited ? "exited with status " + process.exitValue()
                    : "did not exit within " + forked.timeoutSeconds() + " seconds",
                    exited && process.exitValue() == 0, !exited, readOutput(output));
            if (Files.exists(results)) {
                run.read(Files.readAllLines(results));
            }
            return run;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted waiting for the forked JVM of " + testClass.getName(), e);
        }
        finally {
            if (process != null) {
                process.destroyForcibly();
            }
            deleteDirectory(directory);
        }
    }

    /// The child's output, in the platform's encoding, which its standard streams use when
    /// redirected to a file; bytes that encoding cannot decode are replaced.
    private static String readOutput(Path output) throws IOException {
        Charset encoding = Charset.forName(System.getProperty("native.encoding"), Charset.defaultCharset());
        return new String(Files.readAllBytes(output), encoding);
    }

    private static void deleteDirectory(Path directory) {
        if (directory == null) {
            return;
        }
        try {
            Files.deleteIfExists(directory.resolve("results.txt"));
            Files.deleteIfExists(directory.resolve("output.txt"));
            Files.deleteIfExists(directory);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<String> command(ForkedJvm forked, Path results, String selector) {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
        for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            if (inherited(argument)) {
                command.add(argument);
            }
        }
        for (String name : System.getProperties().stringPropertyNames()) {
            if (name.startsWith("hardwood.")) {
                command.add("-D" + name + "=" + System.getProperty(name));
            }
        }
        command.addAll(List.of(forked.jvmArgs()));
        command.add("-D" + CHILD_PROPERTY + "=true");
        command.addAll(List.of("-cp", System.getProperty("java.class.path"), ForkedJvmMain.class.getName(),
                results.toString(), selector));
        return command;
    }

    private static boolean inherited(String argument) {
        for (String prefix : INHERITED_PREFIXES) {
            if (argument.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /// What the child JVM did: how it ended, its output, and the end or skip of each test and
    /// container it finished, by unique id.
    private static final class ChildRun {

        private final String ending;
        private final boolean exitedNormally;
        private final boolean timedOut;
        private final String output;
        private final Map<String, Outcome> finished = new HashMap<>();
        private String running;
        private boolean containerReported;
        private boolean endReported;

        ChildRun(String ending, boolean exitedNormally, boolean timedOut, String output) {
            this.ending = ending;
            this.exitedNormally = exitedNormally;
            this.timedOut = timedOut;
            this.output = output;
        }

        void read(List<String> lines) {
            for (String line : lines) {
                String[] fields = line.split("\t", -1);
                if (fields[0].equals("S")) {
                    running = fields[1];
                }
                else if (fields[0].equals("K")) {
                    finished.put(fields[1], new Outcome("SKIPPED", fields[2]));
                }
                else {
                    finished.put(fields[1], new Outcome(fields[2], fields[3]));
                    if (fields[1].equals(running)) {
                        running = null;
                    }
                }
            }
        }

        /// Whether the child ran or skipped the test, was running it when it exited, exited
        /// normally or timed out, so that [#report] tells the test's outcome.
        boolean reached(String uniqueId) {
            return exitedNormally || timedOut || finished.containsKey(uniqueId) || uniqueId.equals(running);
        }

        /// Passes where the child's run of the test passed, aborts where it was skipped, throws
        /// what it threw otherwise, or, where the test has no outcome, what ended a container
        /// around it, as an error JUnit rethrows does, or how the child ended.
        void report(String uniqueId) throws Throwable {
            Outcome outcome = finished.get(uniqueId);
            if (outcome != null) {
                outcome.report();
                return;
            }
            for (Map.Entry<String, Outcome> container : finished.entrySet()) {
                if (uniqueId.startsWith(container.getKey() + "/")) {
                    Throwable failure = container.getValue().failure();
                    if (failure != null) {
                        containerReported = true;
                        throw failure;
                    }
                }
            }
            if (uniqueId.equals(running)) {
                endReported = true;
                throw new AssertionError("The forked JVM " + ending + " while running this test", new ChildOutput(output));
            }
            if (!exitedNormally) {
                endReported = true;
                throw new AssertionError("The forked JVM " + ending + " before running this test", new ChildOutput(output));
            }
            throw new IllegalStateException("The forked JVM did not run " + uniqueId, new ChildOutput(output));
        }

        /// What ended the container in the child where it failed or aborted, or how the child
        /// ended where it did not exit normally after the container's tests, unless a test
        /// reported either; `null` otherwise.
        Throwable containerFailure(String uniqueId) throws IOException, ClassNotFoundException {
            Outcome outcome = finished.get(uniqueId);
            if (outcome == null) {
                return exitedNormally || endReported ? null
                        : new AssertionError("The forked JVM " + ending + " after running the class's tests",
                                new ChildOutput(output));
            }
            if (containerReported || outcome.status().equals("SKIPPED")) {
                return null;
            }
            return outcome.failure();
        }
    }

    /// What the child JVM wrote to its standard streams, as the cause of a failure it left
    /// without an outcome of its own.
    private static final class ChildOutput extends Exception {

        ChildOutput(String output) {
            super("The forked JVM's output:\n" + output, null, false, false);
        }
    }

    /// A test's or container's end: its status and the Base64 of the serialized throwable, or of
    /// the reason where it was skipped.
    private record Outcome(String status, String data) {

        void report() throws Throwable {
            Throwable failure = failure();
            if (failure != null) {
                throw failure;
            }
        }

        /// What [#report] throws, or `null` where the run passed.
        Throwable failure() throws IOException, ClassNotFoundException {
            if (status.equals("SKIPPED")) {
                return new TestAbortedException("The forked JVM skipped this test: "
                        + new String(Base64.getDecoder().decode(data), StandardCharsets.UTF_8));
            }
            if (!data.isEmpty()) {
                try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(data)))) {
                    return (Throwable) in.readObject();
                }
            }
            if (!status.equals("SUCCESSFUL")) {
                return new IllegalStateException("The forked JVM's run ended " + status + " without a throwable");
            }
            return null;
        }
    }
}
