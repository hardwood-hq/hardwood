/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.predicate;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/// Collects the `WARNING` lines a test provokes, for the cases that assert what the filter
/// layer says about a file rather than what it decides about one.
///
/// Register it with `@RegisterExtension`; it attaches to the root logger for the duration of
/// each test and detaches afterwards, so the messages of one case never reach another.
final class CapturedWarnings implements BeforeEachCallback, AfterEachCallback {

    private final CapturingAppender appender = new CapturingAppender();

    private LoggerConfig loggerConfig;

    /// The formatted text of every warning logged since the current test started.
    List<String> messages() {
        return appender.messages;
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
        loggerConfig = loggerContext.getConfiguration().getRootLogger();
        appender.messages.clear();
        appender.start();
        loggerConfig.addAppender(appender, Level.WARN, null);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        loggerConfig.removeAppender(appender.getName());
        appender.stop();
    }

    private static final class CapturingAppender extends AbstractAppender {

        private final List<String> messages = new ArrayList<>();

        CapturingAppender() {
            super("CapturedWarnings", null, null, true, Property.EMPTY_ARRAY);
        }

        @Override
        public void append(LogEvent event) {
            messages.add(event.getMessage().getFormattedMessage());
        }
    }
}
