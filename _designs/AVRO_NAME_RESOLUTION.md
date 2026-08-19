# Avro Name Resolution

Avro named types emitted from a Parquet schema use grammar-legal, deterministic names.

## Sanitization

A name is legal when it matches `[A-Za-z_][A-Za-z0-9_]*`. Hardwood preserves legal names. For other names, each character outside `[A-Za-z0-9_]` becomes `_`; an initial digit receives a leading `_`; an empty name becomes `_`.

## Scope-local names

Named fields are resolved within each sibling scope. Sanitization collisions use the legal raw member as the unsuffixed name when one exists. Otherwise, the member with the smallest natural `String` order receives the unsuffixed name. Other colliding members receive `_2`, `_3`, and later suffixes, skipping names already reserved in the scope.

Two siblings with the same raw name are rejected. Resolution uses schema-node identity and the complete unprojected schema tree.

## Root names

The root name is split at the last dot. The final segment is the local name. Earlier segments form the namespace. Each segment is sanitized independently. A rewritten root stores its original Parquet name in the `hardwood.parquetName` schema property.

## Descendant namespaces

A descendant named type is placed in the namespace formed by the root full name followed by the resolved local names of its preceding value-path segments. LIST and MAP value positions contribute their container names. Synthetic `list` and `key_value` encoding wrappers do not contribute names.

The root is not appended to its own namespace. Therefore descendants of `schema` include `schema.home.address`, and a child named `root` below root `root` becomes `root.root`.

## Recovering Parquet names

When a record, fixed type, or field is rewritten, Hardwood attaches the exact raw Parquet segment under `hardwood.parquetName`. The property is omitted when the emitted local name equals the raw name. The property survives Avro schema serialization and parser round-trips.

## Invariants

- Every emitted record and fixed full name is legal and unique.
- A legal Parquet name is never rewritten.
- Descendant names are stable under projection and unrelated sibling changes.
- A descendant cannot collide with the root or the canonical `interval` and `float16` fixed types.
- Repeated `interval` and `float16` logical types retain their canonical names and sizes.
