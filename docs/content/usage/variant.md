<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# Variant Columns

A Parquet column annotated with the [`VARIANT`](https://parquet.apache.org/docs/file-format/types/variantencoding/) logical type carries semi-structured, JSON-like data in a self-describing binary encoding. Physically it is a group of two required `BYTE_ARRAY` children, `metadata` and `value`, whose bytes together define a Variant value with its own type tag (object, array, string, int, etc.). `getVariant` reads both children and surfaces them through the `PqVariant` API.

```java
try (RowReader rowReader = fileReader.rowReader()) {
    while (rowReader.hasNext()) {
        rowReader.next();
        PqVariant v = rowReader.getVariant("event");
        if (v == null) {
            continue;   // SQL NULL
        }

        // Type introspection
        VariantType tag = v.type();         // OBJECT, ARRAY, STRING, INT32, ...
        if (tag == VariantType.OBJECT) {
            PqVariantObject obj = v.asObject();
            String userId  = obj.getString("user_id");
            int    age     = obj.getInt("age");
            Instant ts     = obj.getTimestamp("ts");

            // Nested Variant OBJECT / ARRAY — same vocabulary all the way down
            PqVariantObject addr = obj.getObject("address");
            PqVariantArray  tags = obj.getArray("tags");
        }

        // Raw canonical bytes (for round-tripping or hashing)
        byte[] metadata = v.metadata();
        byte[] value    = v.value();
    }
}
```

The `PqVariantObject` view exposes the same primitive getters as a Parquet struct (`getInt`, `getString`, `getTimestamp`, …), but its complex navigation uses `getObject` and `getArray` (Variant-spec terminology) rather than `getStruct` / `getList` / `getMap`. A `PqVariantArray` is iterable and indexed; elements are heterogeneous `PqVariant`s — inspect each element's `type()` and unwrap appropriately.

**Primitive extraction on `PqVariant`:** When you already hold a `PqVariant` (e.g. an array element) use the `as*()` methods — `asInt`, `asString`, `asTimestamp`, and so on. Each throws `VariantTypeException` if the variant's type tag doesn't match.

**Shredded Variants:** Some writers store part of the payload in a typed sibling column (`typed_value`) alongside `value` for better compression and pushdown. Reassembly is transparent: `metadata()` and `value()` return canonical bytes regardless of whether the file was shredded, so `PqVariant` consumers see a single consistent representation.

## Current limitations

- **No Variant-aware predicate pushdown.** Filter predicates against a Variant sub-path (e.g. `WHERE v.age > 30`) aren't yet understood by the pushdown pipeline. Filtering still works against the file's physical shredded columns if you know the layout — a `FilterPredicate.gt("v.typed_value.age", 30)` gets row-group and page skipping via ordinary column statistics — but that ties query code to the writer's shredding strategy and misses any rows where the payload sits in the opaque `value` blob instead. Tracked as [#309](https://github.com/hardwood-hq/hardwood/issues/309).
- **No path projection optimization.** Reading only `v.age` from a Variant column still reassembles the whole Variant for each row rather than reading just the shredded `typed_value.age` column. Requires the same variant-aware planning as #309; no separate issue filed yet.

