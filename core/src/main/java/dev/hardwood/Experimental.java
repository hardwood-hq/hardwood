/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/// Marks a public API as experimental.
///
/// Experimental APIs may change incompatibly, or be removed altogether, in any future release —
/// including patch releases — without going through the usual deprecation cycle. They are
/// published so that users can try them and provide feedback, but should not be relied upon
/// in code where source or binary stability matters.
///
/// When applied to a type, every member of that type is implicitly experimental.
@Documented
@Retention(RetentionPolicy.CLASS)
@Target({ ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.FIELD })
public @interface Experimental {
}
