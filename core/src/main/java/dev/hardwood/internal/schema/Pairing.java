/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

import java.util.List;

import dev.hardwood.metadata.PhysicalType;

/// Whether parquet-format defines an annotation over a column's physical type and width, as
/// [AnnotationPairings#check] answers it.
///
/// A [Fault] carries the facts of a refusal and no sentence. The writer refuses such a column and
/// the reader drops its annotation and reads the column as its physical type, which the format
/// requires of a reader, so the two address different audiences and each formats its own message
/// from these facts.
public sealed interface Pairing {

    /// A pairing the format defines.
    record Legal() implements Pairing {
    }

    /// A pairing it does not.
    record Illegal(Fault fault) implements Pairing {
    }

    /// Why a pairing is not defined.
    sealed interface Fault {

        /// The annotation is defined over `allowed` and the column is none of them.
        record WrongPhysicalType(List<PhysicalType> allowed) implements Fault {
        }

        /// The annotation is defined over a `FIXED_LEN_BYTE_ARRAY` of exactly `expected` bytes and
        /// the column declares another width.
        record WrongWidth(int expected) implements Fault {
        }

        /// An annotation declaring `precision` digits over a column holding at most
        /// `maxPrecision`. Both are carried, so a caller formats the refusal without recovering
        /// the precision from the annotation's own type.
        record PrecisionTooLarge(int precision, long maxPrecision) implements Fault {
        }

        /// An annotation of a group — `LIST`, `MAP` or `VARIANT` — on a primitive column.
        record GroupAnnotation() implements Fault {
        }
    }
}
