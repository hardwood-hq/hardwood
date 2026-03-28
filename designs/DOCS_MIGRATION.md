# Hardwood Documentation Site — Implementation Plan

**Status: Implemented**

## Overview

Set up a versioned documentation website for the Hardwood project using MkDocs with the
Material theme. The documentation source lives in the main `hardwood-hq/hardwood` repo; the
built site is published to a dedicated `hardwood-hq/hardwood-hq.github.io` repo via GitHub Pages.
Versioning is managed by `mike`. JavaDoc is built and co-published alongside the prose docs on
every tagged release.

---

## Repositories

| Repo | Purpose |
|---|---|
| `hardwood-hq/hardwood` | Source of truth: `docs/`, `mkdocs.yml`, CI workflow |
| `hardwood-hq/hardwood-hq.github.io` | Deployment target only — never committed to manually |

The `hardwood-hq.github.io` repo must exist and have GitHub Pages enabled on its `gh-pages` branch
before the first deploy can run. Initialize it with a README so the default branch exists.

---

## Task 1 — Create `hardwood-hq/hardwood-hq.github.io` *(human, do before running Claude Code)*

1. Create a new public repository named `hardwood-hq.github.io` under the `hardwood-hq` org,
   initialized with a README (the publish workflow checks out the default branch, so it must
   exist). GitHub treats any repo named `<org>.github.io` as the organisation's root Pages
   site, so the published URL will be `https://hardwood-hq.github.io/` — no repo subdirectory
   prefix.
2. After the first successful deploy (which creates the `gh-pages` branch), go to
   **Settings → Pages** and set the source to **Deploy from a branch**, branch `gh-pages`,
   folder `/ (root)`.
3. Optionally configure a custom domain (e.g. `docs.hardwood-hq.dev`) and add a `CNAME` record
   at the DNS provider pointing to `hardwood-hq.github.io`.

No further manual work is needed in this repo — CI owns it from here.

---

## Task 2 — Configure Cross-Repo Triggering *(human, do before running Claude Code)*

Rather than giving the `hardwood` repo direct write access to the site repo, `hardwood` fires a
`repository_dispatch` event at `hardwood-hq.github.io`. The site repo has its own workflow that
wakes up, checks out the source, and runs the build. Deployment logic lives where it belongs —
in the repo being deployed.

### 2.1 Create a fine-grained PAT

1. In GitHub, go to **Settings → Developer settings → Personal access tokens → Fine-grained
   tokens → Generate new token**.
2. Set **Resource owner** to `hardwood-hq` (the org).
3. Set **Repository access** to **Only select repositories** → `hardwood-hq.github.io`.
4. Under **Permissions → Repository permissions**, grant:
   - **Contents**: Read and write (required — the `repository_dispatch` API endpoint is
     under Contents, not Actions)
5. Set an expiration and note the token — you will not see it again.

A fine-grained PAT scoped to a single repo and a single permission is materially narrower than
a classic PAT or a deploy key with write access.

### 2.2 Add the PAT as a secret in `hardwood`

In `hardwood` → **Settings → Secrets and variables → Actions → New repository secret**:
- Name: `DOCS_DISPATCH_TOKEN`
- Value: the PAT generated above

---

## Task 3 — Add MkDocs Configuration to `hardwood-hq/hardwood`

### 3.1 Directory layout

Create the following structure in the root of the `hardwood` repo:

```
docs/
├── Dockerfile
├── mkdocs.yml
├── requirements.txt
└── content/
    ├── index.md          # Landing page (migrated from README)
    ├── getting-started.md
    ├── configuration.md
    ├── api-reference.md  # Prose API guide; links to JavaDoc for detail
    └── ...               # Further pages as needed
```

The `docs/` directory sits alongside the Java source code in the repo root and contains
everything needed to build the documentation site. The `mkdocs.yml` and `requirements.txt`
live inside `docs/` rather than the repo root so they do not clutter the project root next to
`pom.xml`, `src/`, etc. The `content/` subdirectory holds the Markdown pages themselves.

Update `mkdocs.yml` to reflect the nested layout:

```yaml
docs_dir: content
```

And reference the config file explicitly in all `mike` and `mkdocs` commands using
`--config-file docs/mkdocs.yml`.

The root `README.md` should be kept but trimmed to a short project description with a link to
the docs site — it serves GitHub visitors, not doc readers.

### 3.2 `mkdocs.yml`

Place this at `docs/mkdocs.yml`:

```yaml
site_name: Hardwood
site_description: A zero-dependency Java Parquet file reader
site_url: https://hardwood-hq.github.io/
repo_url: https://github.com/hardwood-hq/hardwood
repo_name: hardwood-hq/hardwood
edit_uri: edit/main/docs/content/

docs_dir: content

nav:
  - Home: index.md
  - Getting Started: getting-started.md
  - Configuration: configuration.md
  - API Reference: api-reference.md
  - Release Notes: release-notes.md

theme:
  name: material
  palette:
    - scheme: default
      toggle:
        icon: material/brightness-7
        name: Switch to dark mode
    - scheme: slate
      toggle:
        icon: material/brightness-4
        name: Switch to light mode
  features:
    - navigation.tabs
    - navigation.sections
    - navigation.expand
    - navigation.top
    - search.highlight
    - content.code.copy
    - content.action.edit

plugins:
  - search

markdown_extensions:
  - admonition
  - pymdownx.details
  - pymdownx.superfences
  - pymdownx.highlight:
      anchor_linenums: true
  - pymdownx.inlinehilite
  - pymdownx.snippets
  - pymdownx.tabbed:
      alternate_style: true
  - attr_list
  - md_in_html
  - toc:
      permalink: true

extra:
  version:
    provider: mike
    alias: true
    default: latest
  social:
    - icon: fontawesome/brands/github
      link: https://github.com/hardwood-hq/hardwood

# Canonical URL is set automatically by mike for the latest version.
# Older versions receive a "you're viewing an old version" banner via
# the Material theme's version warning feature.
```

### 3.3 Python dependencies

Add a `docs/requirements.txt` (used by CI and the Dockerfile):

```
mkdocs-material>=9.5
mike>=2.1
```

### 3.4 Dockerfile

Add a `docs/Dockerfile` for local development:

```dockerfile
FROM python:3-slim
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
WORKDIR /docs
ENTRYPOINT ["mkdocs"]
CMD ["serve", "--dev-addr=0.0.0.0:8000"]
```

The image contains only what is needed for local preview. CI installs dependencies directly
because it also needs Java and Graphviz for JavaDoc generation.

---

## Task 4 — Migrate README Content into `docs/`

Content currently in `README.md` should be split into focused Markdown pages under
`docs/content/`. Suggested initial structure:

| File | Content |
|---|---|
| `docs/content/index.md` | Project introduction, headline features, quick example |
| `docs/content/getting-started.md` | Installation, Maven/Gradle coordinates, minimal usage |
| `docs/content/configuration.md` | All configuration options with examples |
| `docs/content/api-reference.md` | Prose guide to the public API; links out to JavaDoc |
| `docs/content/release-notes.md` | Changelog (can link to GitHub Releases) |

The `README.md` in the repo root should be updated to a short description with a badge and a
link to the documentation site.

---

## Task 5 — GitHub Actions Workflows

There are now two workflows: one in each repo.

### 5.1 Trigger workflow — `hardwood-hq/hardwood`

This workflow's only job is to fire a `repository_dispatch` event at the site repo. It does not
build or deploy anything itself.

Create `.github/workflows/docs-trigger.yml` in the `hardwood` repo:

```yaml
name: Trigger documentation publish

on:
  push:
    tags: ['v*']

jobs:
  dispatch:
    runs-on: ubuntu-latest

    steps:
      - name: Fire repository_dispatch to site repo
        env:
          GH_TOKEN: ${{ secrets.DOCS_DISPATCH_TOKEN }}
          VERSION: ${{ github.ref_name }}
        run: |
          gh api repos/hardwood-hq/hardwood-hq.github.io/dispatches \
            --field event_type=publish-docs \
            --field client_payload="$(cat <<EOF
          {
            "source_ref": "${{ github.sha }}",
            "version": "${VERSION#v}"
          }
          EOF
          )"
```

The `gh` CLI is pre-installed on all GitHub-hosted runners, so no third-party action is needed.

### 5.2 Publish workflow — `hardwood-hq/hardwood-hq.github.io`

This workflow owns the full build and deploy. Its default checkout is the site repo itself, so
`origin` naturally points to `hardwood-hq.github.io` and mike can push without extra remote
configuration. The hardwood source is cloned into a `source/` subdirectory at the exact SHA the
trigger passed.

Create `.github/workflows/publish.yml` in the `hardwood-hq.github.io` repo:

```yaml
name: Publish documentation

on:
  repository_dispatch:
    types: [publish-docs]
  workflow_dispatch:
    inputs:
      source_ref:
        description: 'Tag or SHA from hardwood to build (e.g. v0.2.0)'
        required: true
      version:
        description: 'Version label without v prefix (e.g. 0.2.0)'
        required: true

jobs:
  publish:
    runs-on: ubuntu-latest

    env:
      SOURCE_REF: >-
        ${{ github.event_name == 'repository_dispatch'
            && github.event.client_payload.source_ref
            || inputs.source_ref }}
      VERSION: >-
        ${{ github.event_name == 'repository_dispatch'
            && github.event.client_payload.version
            || inputs.version }}

    steps:
      - name: Checkout site repo
        uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd  # v6
        with:
          fetch-depth: 0        # mike needs gh-pages history

      - name: Checkout hardwood source
        uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd  # v6
        with:
          repository: hardwood-hq/hardwood
          ref: ${{ env.SOURCE_REF }}
          path: source

      - name: Set up Python
        uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065  # v5
        with:
          python-version: '3.x'

      - name: Install MkDocs dependencies
        run: pip install -r source/docs/requirements.txt

      - name: Set up Java
        uses: actions/setup-java@be666c2fcd27ec809703dec50e508c2fdc7f6654  # v5
        with:
          distribution: temurin
          java-version: '25'

      - name: Install Graphviz
        run: sudo apt-get install -y graphviz

      - name: Configure git identity
        run: |
          git config --global user.name "github-actions[bot]"
          git config --global user.email "github-actions[bot]@users.noreply.github.com"

      - name: Build JavaDoc
        working-directory: source
        run: ./mvnw javadoc:aggregate -q

      - name: Deploy docs
        run: |
          mike deploy \
            --config-file source/docs/mkdocs.yml \
            --branch gh-pages \
            --set-default \
            --update-aliases \
            $VERSION latest

          git checkout gh-pages

          bash source/docs/patch-noindex.sh "$VERSION"

          mkdir -p api/$VERSION
          cp -r source/target/site/apidocs/. api/$VERSION/
          rm -rf api/latest
          cp -r source/target/site/apidocs/. api/latest/

          git add api/
          git add -u
          git commit --amend --no-edit
          git push origin gh-pages
```

### Notes on the two-workflow design

- The first checkout is the site repo itself, so `origin` points to
  `hardwood-hq.github.io` and the default `GITHUB_TOKEN` has write access. Mike can push
  to `origin` directly — no extra remotes or auth wiring needed. The PAT is only used for
  the cross-repo trigger in step 5.1.
- The hardwood source is cloned into `source/`. Because this is a separate nested git repo,
  it survives `git checkout gh-pages` in the parent and remains available for the noindex
  script and JavaDoc copy — no `/tmp` stashing required.
- `mike deploy` runs without `--push` and uses `--set-default` (`-S`) to combine the deploy
  and set-default into a single commit on `gh-pages`. The workflow then checks out `gh-pages`,
  applies the noindex patch for older versions, copies JavaDoc into `api/`, and amends the
  commit before pushing — so the entire release lands as one commit. Staging uses
  `git add api/` (new files) and `git add -u` (noindex modifications to tracked HTML) to
  avoid accidentally adding the nested `source/` checkout.
- The `workflow_dispatch` block on the publish workflow allows manual re-deploys directly from
  the site repo, useful for recovering from a failed run without pushing a new commit.
- The `source_ref` in the payload is the exact commit SHA, not a branch name, so the site repo
  always builds a deterministic snapshot even if `main` moves forward between the trigger and
  the build.
- The JavaDoc build uses `javadoc:aggregate` (not `javadoc:javadoc`) to produce a combined
  JavaDoc across all modules. The UMLDoclet requires Graphviz for SVG diagram generation, so
  the workflow installs it before the build.

---

## Task 6 — SEO: Handling Duplicate Content Across Versions

MkDocs Material with `mike` does not automatically add `noindex` to older versions. The
`extra.version.default: latest` setting in `mkdocs.yml` shows a "you're viewing an old version"
banner, which helps users but does not prevent search engines from indexing duplicate content.

A standalone script injects `<meta name="robots" content="noindex, follow">` into every HTML
file in older versioned directories on `gh-pages`. It is idempotent — files that already carry
the tag are skipped.

Create `docs/patch-noindex.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

# Injects a noindex robots meta tag into HTML files of older versioned
# directories on the gh-pages branch. Idempotent — files that already
# contain the tag are skipped.
#
# Usage: patch-noindex.sh <current-version>
#   Run from the root of a gh-pages checkout.

CURRENT_VERSION="${1:?Usage: patch-noindex.sh <current-version>}"

patched=0
for dir in [0-9]*/; do
  [ -d "$dir" ] || continue
  version=$(basename "$dir")
  [ "$version" = "$CURRENT_VERSION" ] && continue

  while IFS= read -r file; do
    sed -i 's|</head>|<meta name="robots" content="noindex, follow">\n</head>|' "$file"
    patched=$((patched + 1))
  done < <(grep -rL 'name="robots"' "$dir" --include="*.html" || true)
done

echo "patch-noindex: patched $patched file(s)"
```

The script runs from the root of a `gh-pages` checkout and accepts the current version as its
only argument. In CI this is called during the release deploy step (Task 5.2) and its changes
are amended into mike's commit before pushing, so no separate commit is created.

---

## Task 7 — Local Development

Document this in `CONTRIBUTING.md` or `docs/contributing.md`:

```bash
# Build the image (once, or after changing requirements.txt)
docker build -t hardwood-docs docs/

# Serve locally with hot reload — preview at http://127.0.0.1:8000
docker run --rm -p 8000:8000 -v "$(pwd)/docs:/docs" hardwood-docs

# Build static site (output in docs/site/)
docker run --rm -v "$(pwd)/docs:/docs" hardwood-docs build
```

---

## Task 8 — PR Checklist Reminder (Optional)

Add a `.github/PULL_REQUEST_TEMPLATE.md` entry to remind contributors to update docs:

```markdown
## Checklist

- [ ] Code changes are covered by tests
- [ ] If this PR adds or changes user-facing behaviour, `docs/` has been updated
```

---

## Resulting URL Structure

After two releases the published site will have the following structure:

```
https://hardwood-hq.github.io/
  /                    → redirects to /latest/
  /latest/             → alias for the most recent release
  /0.1.0/              → frozen snapshot of the 0.1.0 release
  /0.2.0/              → frozen snapshot of the 0.2.0 release
  /api/latest/         → JavaDoc for the most recent release
  /api/0.1.0/          → JavaDoc for 0.1.0
  /api/0.2.0/          → JavaDoc for 0.2.0
```

If a custom domain is configured, replace `hardwood-hq.github.io` with that domain throughout,
and update `site_url` in `mkdocs.yml` accordingly.

---

## Summary of Files to Create or Modify

| File | Action | Repo |
|---|---|---|
| `docs/Dockerfile` | Create | `hardwood` |
| `docs/mkdocs.yml` | Create | `hardwood` |
| `docs/requirements.txt` | Create | `hardwood` |
| `docs/patch-noindex.sh` | Create | `hardwood` |
| `docs/content/index.md` | Create (migrate from README) | `hardwood` |
| `docs/content/getting-started.md` | Create (migrate from README) | `hardwood` |
| `docs/content/configuration.md` | Create (migrate from README) | `hardwood` |
| `docs/content/api-reference.md` | Create (migrate from README) | `hardwood` |
| `docs/content/release-notes.md` | Create | `hardwood` |
| `.github/workflows/docs-trigger.yml` | Create | `hardwood` |
| `.github/workflows/publish.yml` | Create | `hardwood-hq.github.io` |
| `.github/PULL_REQUEST_TEMPLATE.md` | Create or append | `hardwood` |
| `README.md` | Trim to short description + link to docs site | `hardwood` |

External actions (done by a human, not Claude Code):

- Create `hardwood-hq/hardwood-hq.github.io` repo and enable GitHub Pages on `gh-pages` branch
- Create a fine-grained PAT with Contents read/write permission on `hardwood-hq.github.io` (see Task 2)
- Add `DOCS_DISPATCH_TOKEN` secret to the `hardwood` repo
- Backfill docs for `v1.0.0.Alpha1`: in the `hardwood-hq.github.io` repo, go to
  **Actions → Publish documentation → Run workflow** and enter `source_ref`: `v1.0.0.Alpha1`,
  `version`: `1.0.0.Alpha1`
