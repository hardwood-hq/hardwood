/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for constructing FieldPath objects during schema traversal.
 */
class PathBuilder {

    private final List<FieldPath.PathStep> steps = new ArrayList<>();
    private final List<FieldPath> results = new ArrayList<>();

    void push(FieldPath.PathStep step) {
        steps.add(step);
    }

    void pop() {
        steps.remove(steps.size() - 1);
    }

    void materialize(int leafFieldIndex, int maxDefLevel) {
        results.add(new FieldPath(steps.toArray(new FieldPath.PathStep[0]), leafFieldIndex, maxDefLevel));
    }

    List<FieldPath> results() {
        return results;
    }
}
