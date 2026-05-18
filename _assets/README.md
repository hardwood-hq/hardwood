# Assets

This directory is designed to store current and future brand and aesthetic assets for the Hardwood project. It is intended as a fluid, evolving home for any design material that informs how the project presents itself — logo variants (light/dark, monochrome, social-preview crops), iconography, color palette references, typography and font choices, illustration sources, and similar visual artifacts.

As the project's identity grows, expect this directory to grow with it. New variants and supporting materials should be added here rather than scattered across module-level resource folders, so that there is a single canonical source of truth for the project's visual identity.

## Logo

The Hardwood logo is available as a transparent SVG in two variants.

| Primary | Grayscale |
| :---: | :---: |
| <img src="hardwood.svg" alt="Hardwood logo" width="128" height="128" /> | <img src="hardwood-grayscale.svg" alt="Hardwood logo (grayscale)" width="128" height="128" /> |
| `hardwood.svg` | `hardwood-grayscale.svg` |
| Full-color mark. Default for light and dark backgrounds where color is supported (README headers, project sites, slide decks). | Desaturated variant for print, single-color contexts, or surfaces where the full-color mark would clash. |

### Transparency previews

Both variants ship with transparent backgrounds. The previews below composite each logo onto a complementary fill so the transparency is visible at a glance:

| On slate blue | On warm amber |
| :---: | :---: |
| <img src="previews/hardwood-on-blue.svg" alt="Hardwood logo on slate blue" width="128" height="128" /> | <img src="previews/hardwood-grayscale-on-amber.svg" alt="Hardwood grayscale logo on warm amber" width="128" height="128" /> |
| `#1e3a5f` — a cool slate blue chosen to contrast the logo's warm wood tones. | `#b9661f` — the project's own brand amber, used here to keep the grayscale mark visually tied to the primary palette. |

The preview files live under [`previews/`](previews/) and are for documentation only — embed the canonical `hardwood.svg` / `hardwood-grayscale.svg` in real surfaces.

## Usage

Embed directly via Markdown or HTML:

```markdown
![Hardwood](_assets/hardwood.svg)
```

```html
<img src="_assets/hardwood.svg" alt="Hardwood" width="128" />
```

For GitHub-rendered Markdown, prefer the HTML form when you need to control sizing.
