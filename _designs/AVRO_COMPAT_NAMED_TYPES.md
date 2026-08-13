# Avro-Compatible Named Types

**Status: Proposed.** Tracking issue: #925.

## Scope

`AvroSchemaConverter.planForParquetAvroCompatibility(FileSchema, ColumnProjection)` produces a decode plan whose generated Avro record and fixed names match parquet-avro 1.17.1. The existing two-argument `plan` method remains native Hardwood conversion.

## Naming

One conversion owns one `Map<String, Integer>` counter. Every named type increments the counter for its unchanged Parquet source name before converting children. The first occurrence has no namespace. Occurrence `N > 1` has namespace `<sourceName><N>`. Avro receives the unchanged source name and applies its normal qualified-name parsing.

The counter applies to the message root, ordinary records, Variant groups, fixed-mode INT96, and non-UUID `FIXED_LEN_BYTE_ARRAY` values, including decimal, INTERVAL, and FLOAT16. UUID converts to Avro `string` and does not consume an occurrence.

Records consume before their fields. LIST conversion follows the logical element selected by the Parquet LIST rules. MAP conversion follows the logical value. Variant consumes the annotated group, emits a record with `metadata` and `value` byte fields, and does not traverse the physical Variant children.

## Measured Cases

The executable oracle golden at `parquet-testing-runner/src/test/resources/avro-named-types/parquet-avro-1.17.1-avro-1.11.5.txt` records the complete supported matrix.

- Repeated `address` records receive `address` then `address2.address`; inserting an earlier `address` changes later occurrences to `address2.address` and `address3.address`.
- Repeated fixed names use the same counter and namespace rule even when widths differ.
- INTERVAL and FLOAT16 use the source column name and consume occurrences. UUID does not.
- A qualified root `acme.row` remains Avro name `row`, namespace `acme`, full name `acme.row`.
- Canonical and shredded Variant groups contribute only the Variant record occurrence; named nodes under `typed_value` contribute none.

## Projection

Conversion excludes ordinary record, list-element, Variant, and fixed subtrees with no projected leaf. A retained MAP keeps its physical value schema even for a key-only projection because Avro maps require a value schema. The core row reader still reads only the key and produces null entry values. The equivalent parquet-avro oracle schema retains the complete two-field `key_value` group and value subtree.
