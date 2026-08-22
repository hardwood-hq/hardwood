<!--

     SPDX-License-Identifier: CC-BY-SA-4.0

     Copyright The original authors

     Licensed under the Creative Commons Attribution-ShareAlike 4.0 International License;
     you may not use this file except in compliance with the License.
     You may obtain a copy of the License at https://creativecommons.org/licenses/by-sa/4.0/

-->
# SchemaElement

`dev.hardwood.metadata.SchemaElement` represents one element in the flat schema list in a Parquet footer. The list uses depth-first order.

`FileSchema.fromSchemaElements(List<SchemaElement>)` builds a schema tree from this list. `FileSchema.toSchemaElements()` creates the list again.

!!! note

    Use [`FileSchema.builder(String)`](../how-to/write-row-by-row.md#writing-a-file) to build a schema for writing. The builder checks the complete schema and calculates child counts. Use `SchemaElement` when you work with footer metadata.

## Factories

The static factories build the common element kinds. Each factory sets the fields for its kind.

| Factory | Builds |
|---|---|
| `root(name, numChildren)` | the root group, with no repetition |
| `group(name, repetition, numChildren)` | a group |
| `group(name, repetition, numChildren, logicalType)` | a group with a logical type |
| `primitive(name, type, repetition)` | a primitive column |
| `primitive(name, type, repetition, logicalType)` | a primitive column with a logical type |
| `fixedLengthPrimitive(name, typeLength, repetition)` | a `FIXED_LEN_BYTE_ARRAY` column |
| `fixedLengthPrimitive(name, typeLength, repetition, logicalType)` | a `FIXED_LEN_BYTE_ARRAY` column with a logical type |

```java
import static dev.hardwood.metadata.SchemaElement.fixedLengthPrimitive;
import static dev.hardwood.metadata.SchemaElement.primitive;
import static dev.hardwood.metadata.SchemaElement.root;

List<SchemaElement> elements = List.of(
        root("schema", 3),
        primitive("id", PhysicalType.INT64, RepetitionType.REQUIRED),
        primitive("name", PhysicalType.BYTE_ARRAY, RepetitionType.OPTIONAL, new LogicalType.StringType()),
        fixedLengthPrimitive("uuid", 16, RepetitionType.REQUIRED, new LogicalType.UuidType()));

FileSchema schema = FileSchema.fromSchemaElements(elements);
```

## Repetition

The Parquet format leaves `repetition_type` unset on the root element and requires it on every other element.

`root` builds the root element with no repetition. The group and primitive factories require a non-null repetition value and reject a null one.

`fromSchemaElements` accepts an element with no repetition value, as a footer may carry one. It uses `REQUIRED` for the root and `OPTIONAL` for other elements. `FileSchema.toSchemaElements()` writes `REQUIRED` for the root of a schema built with `FileSchema.builder(String)`.

## Name

A valid Parquet footer has a name for each schema element. The Thrift field is required.

A malformed or truncated footer can omit the name. The reader then creates an element with `name == null`. The factories pass a null name through without a check.

## Type length

`typeLength` has a different meaning for each physical type:

| Physical type | Meaning |
|---|---|
| `FIXED_LEN_BYTE_ARRAY` | the byte length of each value |
| any other physical type | the maximum bit length needed to store any value |

`fixedLengthPrimitive` sets the physical type to `FIXED_LEN_BYTE_ARRAY` and takes the byte length as its second argument. `primitive` rejects `FIXED_LEN_BYTE_ARRAY`.

The maximum bit length has no factory. Use the canonical constructor for it:

```java
// An INT32 column with a maximum bit length of 3 - all values of this column can be stored with 3 bits.
SchemaElement tag = new SchemaElement("tag", PhysicalType.INT32, 3, RepetitionType.REQUIRED,
        null, null, null, null, null, null);
```

`typeLength` can be `null` in raw footer metadata. A writer must provide a positive length for a `FIXED_LEN_BYTE_ARRAY` column. A data reader also needs this length to decode its values.

## Errors

The factories throw `IllegalArgumentException` for these inputs:

| Condition | Factory |
|---|---|
| `repetition` is `null` | `group`, `primitive`, `fixedLengthPrimitive` |
| `type` is `null` | `primitive` |
| `type` is `FIXED_LEN_BYTE_ARRAY` | `primitive` |
| `numChildren` is negative | `group`, `root` |
| `typeLength` is zero or less | `fixedLengthPrimitive` |

A factory checks one element. Rules for the complete list stay in `fromSchemaElements`. The method consumes the list in depth-first order and builds the schema tree.

## Canonical constructor

The record constructor takes the components in Hardwood record-component order:

```java
new SchemaElement(name, type, typeLength, repetitionType, 
                  numChildren, convertedType, scale, precision, 
                  fieldId, logicalType);
```

Use the constructor when the factories do not cover the metadata. The factories do not set `convertedType`, `scale`, `precision`, or `fieldId`.

Use the constructor for:

- a legacy `ConvertedType` annotation, such as `ConvertedType.LIST` or `ConvertedType.MAP`;
- legacy decimal metadata with `scale` and `precision`;
- a Thrift `fieldId`;
- a `typeLength` on a column that is not `FIXED_LEN_BYTE_ARRAY`;
- an element with no repetition value that is not the root;
- a complete element decoded from a footer.

## Node kind

| Method | Returns `true` when |
|---|---|
| `isGroup()` | `type` is `null` |
| `isPrimitive()` | `type` is not `null` |

A null physical type marks a group. `primitive` rejects a null type because it would create a group.
