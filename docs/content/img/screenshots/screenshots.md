# Dive screenshots

Use the `screenshots` Maven profile in the `cli` module to run `hardwood dive`
headlessly via TamboUI recording and overwrite the fixed docs image files under
`docs/content/img/screenshots/`.

Scripted UI navigation tape:
`docs/content/img/screenshots/dive-screenshots.tape`

The tape drives key presses through the `dive` screens and captures:
- `docs/content/img/screenshots/01-landing-overview.svg`
- `docs/content/img/screenshots/02-schema-tree.svg`
- `docs/content/img/screenshots/03-1-rg.svg`
- `docs/content/img/screenshots/03-2-rg-detail.svg`
- `docs/content/img/screenshots/03-3-rg-column-chunks.svg`
- `docs/content/img/screenshots/03-4-rg-column-chunk-detail.svg`
- `docs/content/img/screenshots/04-pages-header-modal.svg`
- `docs/content/img/screenshots/05-dict-search.svg`
- `docs/content/img/screenshots/06-data-scrolled-right.svg`

Default fixture file:
`cli/src/test/resources/dive_screenshots_fixture.parquet`

Regenerate/refresh that fixture:

```bash
python3 tools/screenshots-datagen.py
```

Run:

```bash
./mvnw -pl cli -am -Pscreenshots package -DskipTests
```

Override fixture path:

```bash
./mvnw -pl cli -am -Pscreenshots package -DskipTests -Dscreenshots.parquetFile=/absolute/path/to/file.parquet
```

Notes:
- Existing screenshots are overwritten in place.
- The fixture parquet file must already exist before running the profile.
- When `-Dscreenshots.parquetFile=...` is provided, that fixture is used directly.
