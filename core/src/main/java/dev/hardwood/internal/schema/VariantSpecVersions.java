/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.schema;

/// The version of the Variant specification the reader implements.
///
/// A `VARIANT` annotation names the specification version its group's bytes are encoded in. A
/// version the reader does not implement cannot be decoded, nor read as the unannotated group,
/// whose leaves would surface the encoded bytes as though they were plain binaries. The
/// annotation stays in the schema, so the file opens and reports the group as the footer states
/// it, and [UnreadableGroups] refuses a read that touches one of the group's columns.
public final class VariantSpecVersions {

    /// The one Variant specification version the reader decodes.
    public static final int SUPPORTED = 1;

    private VariantSpecVersions() {
    }
}
