/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.forked;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.platform.commons.JUnitException;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.TestExecutionResult.Status;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.opentest4j.TestAbortedException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

/// Runs fixture classes annotated with [ForkedJvm] through the JUnit launcher and checks the
/// outcome the launching JVM reports for each of their tests and for the class. The fixtures run
/// only where [#FIXTURE_PROPERTY] is set, as it is here, so that a run of every test, an IDE's,
/// leaves out what fails by design. The tests run concurrently, since each mostly waits for child
/// JVMs.
@Execution(ExecutionMode.CONCURRENT)
class ForkedJvmExtensionTest {

    /// set while this class runs: enables the fixtures, in the child JVMs too, which take each
    /// `hardwood.` property
    static final String FIXTURE_PROPERTY = "hardwood.forkedJvm.fixture";

    @BeforeAll
    static void enableFixtures() {
        System.setProperty(FIXTURE_PROPERTY, "true");
    }

    @AfterAll
    static void disableFixtures() {
        System.clearProperty(FIXTURE_PROPERTY);
    }

    @Test
    void eachOutcomeOfTheChildIsReported() {
        Map<String, TestExecutionResult> results = run(OutcomesFixture.class);

        assertThat(results.get("passes()").getStatus()).isEqualTo(Status.SUCCESSFUL);
        assertOutcome(results.get("fails()"), Status.FAILED, AssertionError.class, "failed in the child");
        assertOutcome(results.get("aborts()"), Status.ABORTED, TestAbortedException.class, "aborted in the child");
        assertOutcome(results.get("skippedInChild()"), Status.ABORTED, TestAbortedException.class,
                "The forked JVM skipped this test: System property [" + ForkedJvmExtension.CHILD_PROPERTY
                        + "] with value [true] matches regular expression [true] ==> skipped in the child");
        assertThat(results.get("OutcomesFixture").getStatus()).isEqualTo(Status.SUCCESSFUL);
    }

    @Test
    void beforeAllFailureInTheChildFailsEachTestOnce() {
        Map<String, TestExecutionResult> results = run(BeforeAllFailureFixture.class);

        assertOutcome(results.get("passes()"), Status.FAILED, IllegalStateException.class,
                "before all failed in the child");
        assertThat(results.get("BeforeAllFailureFixture").getStatus()).isEqualTo(Status.SUCCESSFUL);
    }

    @Test
    void afterAllFailureInTheChildFailsTheClass() {
        Map<String, TestExecutionResult> results = run(AfterAllFailureFixture.class);

        assertThat(results.get("passes()").getStatus()).isEqualTo(Status.SUCCESSFUL);
        assertOutcome(results.get("AfterAllFailureFixture"), Status.FAILED, IllegalStateException.class,
                "after all failed in the child");
    }

    /// The first invocation ends the child; the second, which it did not reach, runs in a child
    /// of its own, selected by its unique id.
    @Test
    void crashFailsTheRunningTestAndForksForTheRest() {
        Map<String, TestExecutionResult> results = run(CrashFixture.class);

        assertOutcome(results.get("[1] invocation = 1"), Status.FAILED, AssertionError.class,
                "The forked JVM exited with status 3 while running this test");
        assertThat(results.get("[2] invocation = 2").getStatus()).isEqualTo(Status.SUCCESSFUL);
    }

    /// The tests the child did not reach fail rather than run in children of their own, where
    /// they would pass.
    @Test
    void timeoutFailsEachUnreachedTest() {
        Map<String, TestExecutionResult> results = run(TimeoutFixture.class);

        assertOutcome(results.get("passes()"), Status.FAILED, AssertionError.class,
                "The forked JVM did not exit within 1 seconds before running this test");
        assertOutcome(results.get("passesToo()"), Status.FAILED, AssertionError.class,
                "The forked JVM did not exit within 1 seconds before running this test");
    }

    /// A crash, then a timeout of the child the next test ran in: the test after that fails
    /// without a child of its own, where it would pass. On a machine too slow for the first child
    /// to crash within the timeout, that child times out instead and nothing is forked again, so
    /// the test has nothing to check and is skipped.
    @Test
    void timeoutAfterACrashFailsEachTestNotYetReached() {
        Map<String, TestExecutionResult> results = run(TimeoutAfterCrashFixture.class);

        String crashed = "The forked JVM exited with status 3 while running this test";
        Assumptions.assumeTrue(results.get("[1] invocation = 1").getThrowable()
                .map(Throwable::getMessage).filter(crashed::equals).isPresent(),
                "the first child did not crash within the timeout");
        String notReached = "The forked JVM did not exit within 3 seconds before running this test";
        assertOutcome(results.get("[1] invocation = 1"), Status.FAILED, AssertionError.class, crashed);
        assertOutcomeIn(results.get("[2] invocation = 2"), Status.FAILED, AssertionError.class,
                "The forked JVM did not exit within 3 seconds while running this test", notReached);
        assertOutcome(results.get("[3] invocation = 3"), Status.FAILED, AssertionError.class, notReached);
        assertThat(results.get("TimeoutAfterCrashFixture").getStatus()).isEqualTo(Status.SUCCESSFUL);
    }

    @Test
    void exitAfterTheLastTestFailsTheClass() {
        Map<String, TestExecutionResult> results = run(AfterAllExitFixture.class);

        assertThat(results.get("passes()").getStatus()).isEqualTo(Status.SUCCESSFUL);
        assertOutcome(results.get("AfterAllExitFixture"), Status.FAILED, AssertionError.class,
                "The forked JVM exited with status 3 after running the class's tests");
    }

    /// JUnit ends the engine's run with the error rather than end the test with it, so the test
    /// fails with the engine's failure; the child exits although the test left a non-daemon thread
    /// running.
    @Test
    void errorJUnitRethrowsFailsTheTestWithIt() {
        Map<String, TestExecutionResult> results = run(RethrownErrorFixture.class);

        assertOutcome(results.get("throwsOutOfMemoryError()"), Status.FAILED, JUnitException.class,
                "TestEngine with ID 'junit-jupiter' failed to execute tests");
        assertThat(results.get("throwsOutOfMemoryError()").getThrowable()).hasValueSatisfying(throwable -> assertThat(throwable)
                .rootCause()
                .isInstanceOf(OutOfMemoryError.class)
                .hasMessage("thrown by the fixture"));
    }

    /// Asserts the outcome ended with one of `messages`, each in full.
    private static void assertOutcomeIn(TestExecutionResult result, Status status, Class<? extends Throwable> type,
                                        String... messages) {
        assertThat(result.getStatus()).isEqualTo(status);
        assertThat(result.getThrowable()).hasValueSatisfying(throwable -> {
            assertThat(throwable).isInstanceOf(type);
            assertThat(throwable.getMessage()).isIn((Object[]) messages);
        });
    }

    private static void assertOutcome(TestExecutionResult result, Status status, Class<? extends Throwable> type,
                                      String message) {
        assertThat(result.getStatus()).isEqualTo(status);
        assertThat(result.getThrowable()).hasValueSatisfying(throwable -> assertThat(throwable)
                .isInstanceOf(type)
                .hasMessage(message));
    }

    /// The result of each test and container of `fixture`, by display name.
    private static Map<String, TestExecutionResult> run(Class<?> fixture) {
        Map<String, TestExecutionResult> results = new ConcurrentHashMap<>();
        LauncherFactory.create().execute(LauncherDiscoveryRequestBuilder.request().selectors(selectClass(fixture)).build(),
                new TestExecutionListener() {
                    @Override
                    public void executionFinished(TestIdentifier test, TestExecutionResult result) {
                        results.put(test.getDisplayName(), result);
                    }
                });
        return results;
    }
}
