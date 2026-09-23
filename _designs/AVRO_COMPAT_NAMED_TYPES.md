# Avro-Compatible Named Types

**Status: Implemented.** Tracking issue: #925.

## Scope

`AvroSchemaConverter.planForParquetAvroCompatibility(FileSchema, ColumnProjection)` produces a decode plan whose generated Avro record and fixed names match parquet-avro 1.17.1. The existing two-argument `plan` method remains native Hardwood conversion.

parquet-avro converts `INT96` only when `READ_INT96_AS_FIXED` is enabled and otherwise rejects it as deprecated. The two-argument compatibility method mirrors the default and rejects `INT96` with parquet-avro's own `IllegalArgumentException`; the `planForParquetAvroCompatibility(FileSchema, ColumnProjection, boolean readInt96AsFixed)` overload reads it as a 12-byte `fixed` when the flag is set. Native `plan` always reads `INT96` as a `fixed`.

## Naming

One conversion owns one `Map<String, Integer>` counter. Every named type increments the counter for its unchanged Parquet source name before converting children. The first occurrence has no namespace. Occurrence `N > 1` has namespace `<sourceName><N>`. Avro receives the unchanged source name and applies its normal qualified-name parsing.

The counter applies to the message root, ordinary records, Variant groups, fixed-mode INT96, and non-UUID `FIXED_LEN_BYTE_ARRAY` values, including decimal, INTERVAL, and FLOAT16. UUID converts to Avro `string` and does not consume an occurrence.

Records consume before their fields. LIST conversion follows the logical element selected by the Parquet LIST rules. MAP conversion follows the logical value. Variant consumes the annotated group, emits a record with `metadata` and `value` byte fields, and does not traverse the physical Variant children.

## Measured Cases

The executable oracle golden at `parquet-testing-runner/src/test/resources/avro-named-types/parquet-avro-1.17.1-avro-1.11.5.txt` records these parquet-avro 1.17.1 dispositions on Avro 1.11.5.

| Fixture category | Disposition |
| --- | --- |
| Different-shape and identical nested records | Supported; each record occurrence uses the shared source-name counter. |
| Records in LIST elements and MAP values | Supported; logical element/value traversal contributes named records. |
| Canonical and shredded Variant | Supported; each Variant group contributes one record and shredded `typed_value` descendants contribute none. |
| Plain fixed values and DECIMAL fixed values | Supported; fixed values use the shared source-name counter and retain their widths and decimal precision/scale. |
| INTERVAL and FLOAT16 fixed values | Supported; each uses its source column name and consumes an occurrence. |
| UUID fixed value | Supported as Avro `string`; it consumes no occurrence. |
| Qualified root name | Supported; `acme.row` has Avro name `row`, namespace `acme`, and full name `acme.row`. |
| Reordered and preceding `address` records | Supported; occurrences are order-sensitive: `address`, `address2.address`, then `address3.address` after an inserted preceding occurrence. |
| INT96 with `READ_INT96_AS_FIXED` | Supported as a fixed value; repeated source names consume the shared counter and use the later namespace. |
| INT96 without `READ_INT96_AS_FIXED` | Unsupported with `java.lang.IllegalArgumentException`: `INT96 is deprecated. As interim enable READ_INT96_AS_FIXED flag to read as byte array.` |

## Projection

Conversion excludes ordinary record, list-element, Variant, and fixed subtrees with no projected leaf. A retained MAP keeps its physical value schema even for a key-only projection because Avro maps require a value schema. The core row reader still reads only the key and produces null entry values. The equivalent parquet-avro oracle schema retains the complete two-field `key_value` group and value subtree.
