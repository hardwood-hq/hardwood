/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.internal.reader.row;

import dev.hardwood.row.StructAccessor;
import dev.hardwood.schema.SchemaNode;

/// A [StructAccessor] that names the schema node behind each of its fields. Two sibling fields
/// may share a name, so only a field's position identifies it, and a projected struct's
/// positions follow the projection rather than the file's group.
public interface FieldSchemaAccessor extends StructAccessor {

    /// The schema node of the field at `fieldIndex`, in the index space of [#getFieldName].
    ///
    /// @throws IndexOutOfBoundsException if `fieldIndex` is not a field of this struct
    SchemaNode getFieldSchema(int fieldIndex);
}
