package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/parser"
	gmhtml "github.com/yuin/goldmark/renderer/html"
)

// ---- manifest (nav.json) ----

type site struct {
	Title   string `json:"title"`
	Short   string `json:"short"`
	Version string `json:"version"`
	Repo    string `json:"repo"`
}

type rawItem struct {
	Title   string `json:"title"`
	Source  string `json:"source"`
	Section string `json:"section"` // extract just this "## <section>" block from Source
	Anchor  string `json:"anchor"`  // in-page section id this doc renders under
	Href    string `json:"href"`    // explicit link target (e.g. "#top"); used when Anchor is empty
	Blurb   string `json:"blurb"`
}

type rawGroup struct {
	Title     string    `json:"title"`
	HideCards bool      `json:"hideCards"` // omit this group from the home card grid
	Items     []rawItem `json:"items"`
}

type manifest struct {
	Site   site       `json:"site"`
	Groups []rawGroup `json:"groups"`
}

// ---- template models ----

type navItem struct {
	Title string
	Href  string
}

type navGroup struct {
	Title string
	Items []navItem
}

type card struct {
	Title string
	Href  string
	Blurb string
}

type cardGroup struct {
	Title string
	Cards []card
}

type link struct {
	Title string
	Href  string
}

type docSection struct {
	ID      string
	Content template.HTML
}

// sdoc is a rendered doc awaiting pager wiring.
type sdoc struct {
	id    string
	title string
	html  string
}

type pageData struct {
	Site     site
	Desc     string
	CSS      template.CSS
	JS       template.JS
	Nav      []navGroup
	Cards    []cardGroup
	Sections []docSection
}

func main() {
	root := flag.String("root", "../..", "repository root")
	flag.Parse()

	toolsDir := filepath.Join(*root, "docs", "tools")
	navPath := filepath.Join(toolsDir, "nav.json")
	tmplGlob := filepath.Join(toolsDir, "templates", "*.tmpl")
	assetsDir := filepath.Join(toolsDir, "assets")
	mdDir := filepath.Join(*root, "docs")
	outDir := filepath.Join(*root, "docs")

	man, err := loadManifest(navPath)
	if err != nil {
		log.Fatalf("manifest: %v", err)
	}

	tmpl, err := template.ParseGlob(tmplGlob)
	if err != nil {
		log.Fatalf("templates: %v", err)
	}

	md := goldmark.New(
		goldmark.WithExtensions(extension.GFM),
		goldmark.WithParserOptions(parser.WithAutoHeadingID()),
		goldmark.WithRendererOptions(gmhtml.WithUnsafe()),
	)

	// Cross-references to docs Markdown (e.g. SCHEDULER.md) become in-page
	// anchors (#scheduler). Only whole-file sources map cleanly by basename;
	// section extracts share a source (README.md) and would collide.
	linkMap := map[string]string{}
	for _, g := range man.Groups {
		for _, it := range g.Items {
			if it.Source != "" && it.Section == "" && it.Anchor != "" {
				linkMap[filepath.Base(it.Source)] = "#" + it.Anchor
			}
		}
	}

	// Render each doc in manifest order.
	var docs []sdoc
	for _, g := range man.Groups {
		for _, it := range g.Items {
			if it.Source == "" {
				continue
			}
			body := convertDoc(md, filepath.Join(mdDir, it.Source), it.Section)
			docs = append(docs, sdoc{
				id:    it.Anchor,
				title: it.Title,
				html:  rewriteLinks(body, linkMap),
			})
		}
	}

	// Wrap each doc as a section with a Previous / Next pager.
	sections := make([]docSection, 0, len(docs))
	for i, d := range docs {
		prev := &link{Title: "Documentation", Href: "#top"}
		if i > 0 {
			prev = &link{Title: docs[i-1].title, Href: "#" + docs[i-1].id}
		}
		var next *link
		if i < len(docs)-1 {
			next = &link{Title: docs[i+1].title, Href: "#" + docs[i+1].id}
		}
		sections = append(sections, docSection{
			ID:      d.id,
			Content: template.HTML(d.html + pagerHTML(prev, next)), //nolint:gosec // trusted local docs
		})
	}

	css, err := os.ReadFile(filepath.Join(assetsDir, "docs.css"))
	if err != nil {
		log.Fatalf("read docs.css: %v", err)
	}
	js, err := os.ReadFile(filepath.Join(assetsDir, "docs.js"))
	if err != nil {
		log.Fatalf("read docs.js: %v", err)
	}

	data := pageData{
		Site:     man.Site,
		Desc:     "Reference documentation for " + man.Site.Short + ", a lightweight, auto-scaling queue for processing Go functions as jobs.",
		CSS:      template.CSS(css), //nolint:gosec // our own stylesheet
		JS:       template.JS(js),   //nolint:gosec // our own script
		Nav:      buildNav(man),
		Cards:    buildCards(man),
		Sections: sections,
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatalf("mkdir %s: %v", outDir, err)
	}
	out := filepath.Join(outDir, "index.html")
	if err := render(tmpl, out, data); err != nil {
		log.Fatalf("render %s: %v", out, err)
	}
	fmt.Printf("generated %s (%d docs)\n", out, len(docs))
}

func loadManifest(path string) (manifest, error) {
	var man manifest
	b, err := os.ReadFile(path)
	if err != nil {
		return man, err
	}
	err = json.Unmarshal(b, &man)
	return man, err
}

// convertDoc reads a Markdown source and renders it to HTML. When section is
// set, only that "## <section>" block is extracted and promoted to a page.
func convertDoc(md goldmark.Markdown, path, section string) string {
	src, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read %s: %v", path, err)
	}
	if section != "" {
		src, err = extractSection(src, section)
		if err != nil {
			log.Fatalf("%s: %v", path, err)
		}
		src = shiftHeadingsUp(src)
	}
	var body bytes.Buffer
	if err := md.Convert(src, &body); err != nil {
		log.Fatalf("convert %s: %v", path, err)
	}
	return body.String()
}

// buildNav resolves each sidebar item to its in-page anchor.
func buildNav(man manifest) []navGroup {
	groups := make([]navGroup, 0, len(man.Groups))
	for _, g := range man.Groups {
		items := make([]navItem, 0, len(g.Items))
		for _, it := range g.Items {
			href := it.Href
			if it.Anchor != "" {
				href = "#" + it.Anchor
			}
			items = append(items, navItem{Title: it.Title, Href: href})
		}
		groups = append(groups, navGroup{Title: g.Title, Items: items})
	}
	return groups
}

// buildCards builds the home card grid from groups that hold content pages.
func buildCards(man manifest) []cardGroup {
	var out []cardGroup
	for _, g := range man.Groups {
		if g.HideCards {
			continue
		}
		var cards []card
		for _, it := range g.Items {
			if it.Source == "" {
				continue
			}
			cards = append(cards, card{Title: it.Title, Href: "#" + it.Anchor, Blurb: it.Blurb})
		}
		if len(cards) > 0 {
			out = append(out, cardGroup{Title: g.Title, Cards: cards})
		}
	}
	return out
}

// extractSection returns the lines from a "## <section>" heading up to (but
// not including) the next H2, ignoring headings inside fenced code blocks.
func extractSection(md []byte, section string) ([]byte, error) {
	lines := strings.Split(string(md), "\n")
	target := "## " + section
	start, end := -1, len(lines)
	inFence := false
	for i, l := range lines {
		if strings.HasPrefix(l, "```") {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		if start == -1 {
			if l == target {
				start = i
			}
			continue
		}
		if strings.HasPrefix(l, "## ") { // next H2 ends the section
			end = i
			break
		}
	}
	if start == -1 {
		return nil, fmt.Errorf("section %q not found", section)
	}
	return []byte(strings.Join(lines[start:end], "\n")), nil
}

// shiftHeadingsUp promotes every heading by one level (## -> #, ### -> ##, ...)
// so an extracted section reads as a standalone page. Code fences are skipped.
func shiftHeadingsUp(md []byte) []byte {
	lines := strings.Split(string(md), "\n")
	inFence := false
	for i, l := range lines {
		if strings.HasPrefix(l, "```") {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		h := 0
		for h < len(l) && l[h] == '#' {
			h++
		}
		if h >= 2 && h < len(l) && l[h] == ' ' {
			lines[i] = l[1:] // drop one '#'
		}
	}
	return []byte(strings.Join(lines, "\n"))
}

// pagerHTML renders the Previous/Next footer for a doc section.
func pagerHTML(prev, next *link) string {
	var b strings.Builder
	b.WriteString(`<div class="pager">`)
	if prev != nil {
		b.WriteString(`<a href="` + prev.Href + `"><span class="dir">← Previous</span>` + template.HTMLEscapeString(prev.Title) + `</a>`)
	} else {
		b.WriteString(`<span></span>`)
	}
	if next != nil {
		b.WriteString(`<a class="next" href="` + next.Href + `"><span class="dir">Next →</span>` + template.HTMLEscapeString(next.Title) + `</a>`)
	} else {
		b.WriteString(`<span></span>`)
	}
	b.WriteString(`</div>`)
	return b.String()
}

var mdHref = regexp.MustCompile(`href="([^"]+?\.md)(#[^"]*)?"`)

// rewriteLinks repoints links to docs Markdown files at their in-page anchor.
func rewriteLinks(html string, m map[string]string) string {
	return mdHref.ReplaceAllStringFunc(html, func(s string) string {
		sub := mdHref.FindStringSubmatch(s)
		if out, ok := m[filepath.Base(sub[1])]; ok {
			return `href="` + out + sub[2] + `"`
		}
		return s
	})
}

func render(tmpl *template.Template, out string, data pageData) error {
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "page", data); err != nil {
		return err
	}
	return os.WriteFile(out, buf.Bytes(), 0o644)
}
