/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.row;

/// Type-safe struct interface for reading nested Parquet data.
///
/// Provides dedicated accessor methods for each type, similar to JDBC ResultSet.
/// This interface is used for nested struct access, not for top-level row iteration.
/// For top-level row access, use [dev.hardwood.reader.RowReader] directly.
///
/// A `PqStruct` is an immutable, self-contained object representing a nested
/// struct value, which can be freely passed around in an application.
///
/// ```java
/// while (rowReader.hasNext()) {
///     rowReader.next();
///     int id = rowReader.getInt("id");
///
///     // Nested struct
///     PqStruct address = rowReader.getStruct("address");
///     String city = address.getString("city");
///
///     // List of structs
///     PqList items = rowReader.getList("items");
///     for (PqStruct item : items.structs()) { ... }
/// }
/// ```
public interface PqStruct extends StructAccessor {
}
