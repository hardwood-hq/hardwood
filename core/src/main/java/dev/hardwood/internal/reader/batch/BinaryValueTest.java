/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.batch;

/// A test of one present binary value, read where it sits as `bytes[from, to)`.
@FunctionalInterface
public interface BinaryValueTest {

    boolean testValue(byte[] bytes, int from, int to);
}
