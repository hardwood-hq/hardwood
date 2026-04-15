/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.cli.command;

import java.util.Map;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

/// Disables Quarkus console logging for native integration tests.
///
/// The Quarkus integration-test extension adds
/// `-Dquarkus.log.category."io.quarkus".level=INFO` to the native binary
/// subprocess command, which causes startup/shutdown messages to appear in
/// captured stdout and break exact-string assertions. Returning
/// `quarkus.log.console.enable=false` here overrides the console handler at the
/// handler level, suppressing all console output regardless of category levels.
public class QuietLoggingTestResource implements QuarkusTestResourceLifecycleManager {

    @Override
    public Map<String, String> start() {
        return Map.of("quarkus.log.console.enable", "false");
    }

    @Override
    public void stop() {
    }
}
