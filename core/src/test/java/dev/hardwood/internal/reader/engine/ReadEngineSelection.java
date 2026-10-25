/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.engine;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/// Selects, for the reads of one test class, whether they go to the read engine or to the
/// pipeline, whatever the build selects for the others; registered as a static field with
/// `@RegisterExtension`. Restores the selection the class found once its tests have run.
public final class ReadEngineSelection implements BeforeAllCallback, AfterAllCallback {

    private final String value;
    private String previous;

    private ReadEngineSelection(String value) {
        this.value = value;
    }

    /// The class's reads go to the read engine.
    public static ReadEngineSelection engine() {
        return new ReadEngineSelection("auto");
    }

    /// The class's reads go to the pipeline.
    public static ReadEngineSelection pipeline() {
        return new ReadEngineSelection("off");
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        previous = System.getProperty(ReadEngine.PROPERTY);
        System.setProperty(ReadEngine.PROPERTY, value);
    }

    @Override
    public void afterAll(ExtensionContext context) {
        if (previous == null) {
            System.clearProperty(ReadEngine.PROPERTY);
        }
        else {
            System.setProperty(ReadEngine.PROPERTY, previous);
        }
    }
}
