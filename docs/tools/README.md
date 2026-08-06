# Docs generator

Renders the Markdown docs in [`../`](../) into a single self-contained HTML page
at `../html/index.html` (all CSS and JS inlined).

This is a **separate Go module** so its one dependency
([goldmark](https://github.com/yuin/goldmark)) never enters the library's
dependency-free `go.mod`.

## Usage

From the repository root:

```sh
make docs
```

Or directly:

```sh
cd docs/tools && go run ./gen
```

## Layout

- [`nav.json`](nav.json) — single source of truth for navigation. Each item
  names its Markdown `source`, an `output` name (used to derive the in-page
  anchor), and a card `blurb`.
- [`templates/layout.tmpl`](templates/layout.tmpl) — the page shell.
- [`assets/docs.css`](assets/docs.css), [`assets/docs.js`](assets/docs.js) —
  hand-authored styles and behavior, inlined into the output at build time.
- [`gen/main.go`](gen/main.go) — the generator.

## How it works

- Each doc is rendered from Markdown with goldmark (GFM: tables, strikethrough,
  autolinks) and auto-generated heading IDs, then emitted as a hidden
  `<section>`. A small client-side router in `docs.js` shows one section at a
  time, so it reads like a multi-page site from one file.
- The home view (`#top`) is a curated shell in the template; its cards and the
  sidebar are built from `nav.json`, so navigation can't drift from the doc set.
- Cross-references between docs (e.g. `SCHEDULER.md`) are rewritten to in-page
  anchors (`#scheduler`).
- Quick Start and Common Recipes are extracted from named `## ` sections of the
  root `README.md`, so that content isn't duplicated.
- Because everything is one file, the page works offline. With JavaScript
  disabled the router can't run, so a `<noscript>` fallback reveals every
  section as one long readable document.

## Adding a page

Add an entry under the appropriate group in `nav.json` with `title`, `source`,
`output`, and `blurb`, then run `make docs`.

## Editing content

Edit the Markdown in [`../`](../) and re-run `make docs`. The generated
`../html/` output is build-only (git-ignored) — do not edit it by hand.
