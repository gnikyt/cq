# Docs generator

Renders the Markdown docs in [`../`](../) into the static HTML site under
[`../html/`](../html).

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

## How it works

- [`nav.json`](nav.json) is the single source of truth for navigation. Each
  item names its Markdown `source`, its `output` filename, and a card `blurb`.
  The sidebar (every page) and the landing page's cards are both built from it,
  so navigation can never drift from the page set.
- Content pages are rendered from Markdown with goldmark (GFM: tables,
  strikethrough, autolinks) and auto-generated heading IDs.
- The landing page (`index.html`) is a curated shell in
  [`templates/layout.tmpl`](templates/layout.tmpl) — its intro copy is
  hand-written, but its cards and sidebar come from `nav.json`.
- Syntax highlighting, the on-page table of contents, anchor links, copy
  buttons, and the theme toggle all run client-side in
  [`../html/assets/docs.js`](../html/assets/docs.js); the generator does not
  touch them.

## Adding a page

Add an entry under the appropriate group in `nav.json` with `title`, `source`,
`output`, and `blurb`, then run `make docs`.

## Editing content

Edit the Markdown in [`../`](../) and re-run `make docs`. The generated
`../html/*.html` files are build output — do not edit them by hand.
