/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.forked;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

/// Runs a test class's tests in one child JVM started with [#jvmArgs], such as a heap of a set
/// size, whatever JVM runs the class: the build's test fork or an IDE's.
///
/// The class's tests and lifecycle methods run only in the child. Its static initialisers, its
/// instances' construction and the resolution of its tests' parameters, `@TempDir` among them, run
/// in both JVMs, so a heap-sized fixture belongs in a lifecycle method or a test. Each test of the
/// launching JVM reports what the child's run of it did: it passes, is skipped or aborted, or
/// throws what the child's run threw; a failure of the class's container in the child, such as an `@AfterAll`
/// method's, fails the class. A test the child was running when it exited, as an
/// `OutOfMemoryError` makes it, fails with the child's exit status and output; each test it did not
/// reach then runs in a child JVM of its own. A child still running after [#timeoutSeconds] is
/// killed: the test it was running and each test not yet reached fail, without another child. A
/// child that exits abnormally, or times out, after its last test fails the class. A child exits
/// when the JVM that launched it does.
///
/// The child runs every test of the class, even where only some were selected. It takes the
/// launching JVM's class path, its module flags (`--add-modules`, `--add-opens`, `--add-exports`,
/// `--add-reads`, `--enable-native-access`), its `-D` arguments and each `hardwood.` system
/// property, then [#jvmArgs]. `@TestFactory` methods and `@Nested` classes are not supported.
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(ForkedJvmExtension.class)
public @interface ForkedJvm {

    /// The child JVM's own arguments, such as `-Xmx256m`.
    String[] jvmArgs();

    /// How long the child JVM may run, in seconds, before it is killed.
    long timeoutSeconds() default 300;
}
