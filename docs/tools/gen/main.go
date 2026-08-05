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
	Output  string `json:"output"`
	Href    string `json:"href"`
	Blurb   string `json:"blurb"`
}

type rawGroup struct {
	Title     string    `json:"title"`
	HideCards bool      `json:"hideCards"` // omit this group from the landing card grid
	Items     []rawItem `json:"items"`
}

type manifest struct {
	Site   site       `json:"site"`
	Groups []rawGroup `json:"groups"`
}

type navItem struct {
	Title  string
	Href   string
	Active bool
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

type pageData struct {
	Site       site
	Title      string
	Desc       string
	GroupTitle string
	Content    template.HTML
	Nav        []navGroup
	Prev       *link
	Next       *link
	Cards      []cardGroup
}

func main() {
	root := flag.String("root", "../..", "repository root")
	flag.Parse()

	navPath := filepath.Join(*root, "docs", "tools", "nav.json")
	tmplGlob := filepath.Join(*root, "docs", "tools", "templates", "*.tmpl")
	mdDir := filepath.Join(*root, "docs")
	outDir := filepath.Join(*root, "docs", "html")

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

	// Pager chain: the landing, then every content page in manifest order.
	// linkMap rewrites cross-references to docs Markdown files (e.g.
	// SCHEDULER.md) into their generated HTML output (scheduler.html).
	chain := []link{{Title: "Documentation", Href: "index.html"}}
	linkMap := map[string]string{}
	for _, g := range man.Groups {
		for _, it := range g.Items {
			if it.Source != "" {
				chain = append(chain, link{Title: it.Title, Href: it.Output})
			}
			// Only whole-file docs sources are safe to map by basename;
			// section extracts share a source (README.md) and would collide.
			if it.Source != "" && it.Section == "" && it.Output != "" {
				linkMap[filepath.Base(it.Source)] = it.Output
			}
		}
	}

	count := 0

	// Content pages.
	for _, g := range man.Groups {
		for _, it := range g.Items {
			if it.Source == "" {
				continue
			}
			src, err := os.ReadFile(filepath.Join(mdDir, it.Source))
			if err != nil {
				log.Fatalf("read %s: %v", it.Source, err)
			}

			// A section extract pulls one "## <section>" block from a larger
			// file (e.g. the root README) and promotes it to a standalone page.
			if it.Section != "" {
				src, err = extractSection(src, it.Section)
				if err != nil {
					log.Fatalf("%s: %v", it.Source, err)
				}
				src = shiftHeadingsUp(src)
			}

			var body bytes.Buffer
			if err := md.Convert(src, &body); err != nil {
				log.Fatalf("convert %s: %v", it.Source, err)
			}
			html := rewriteLinks(body.String(), linkMap)

			desc := it.Blurb
			if desc == "" {
				desc = it.Title + " — " + man.Site.Short + " documentation"
			}

			data := pageData{
				Site:       man.Site,
				Title:      it.Title,
				Desc:       desc,
				GroupTitle: g.Title,
				Content:    template.HTML(html), //nolint:gosec // trusted local docs
				Nav:        buildNav(man, it.Output),
				Prev:       neighbor(chain, it.Output, -1),
				Next:       neighbor(chain, it.Output, +1),
			}
			if err := render(tmpl, "page", filepath.Join(outDir, it.Output), data); err != nil {
				log.Fatalf("render %s: %v", it.Output, err)
			}
			count++
		}
	}

	// Landing page.
	landing := pageData{
		Site:  man.Site,
		Desc:  "Reference documentation for " + man.Site.Short + ", a lightweight, auto-scaling queue for processing Go functions as jobs.",
		Nav:   buildNav(man, "index.html"),
		Cards: buildCards(man),
	}
	if err := render(tmpl, "landing", filepath.Join(outDir, "index.html"), landing); err != nil {
		log.Fatalf("render index.html: %v", err)
	}
	count++

	fmt.Printf("generated %d pages into %s\n", count, outDir)
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

func buildNav(man manifest, current string) []navGroup {
	groups := make([]navGroup, 0, len(man.Groups))
	for _, g := range man.Groups {
		items := make([]navItem, 0, len(g.Items))
		for _, it := range g.Items {
			href := it.Href
			if it.Output != "" {
				href = it.Output
			}
			items = append(items, navItem{
				Title:  it.Title,
				Href:   href,
				Active: it.Output != "" && it.Output == current,
			})
		}
		groups = append(groups, navGroup{Title: g.Title, Items: items})
	}
	return groups
}

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
			cards = append(cards, card{Title: it.Title, Href: it.Output, Blurb: it.Blurb})
		}
		if len(cards) > 0 {
			out = append(out, cardGroup{Title: g.Title, Cards: cards})
		}
	}
	return out
}

func neighbor(chain []link, output string, offset int) *link {
	for i, l := range chain {
		if l.Href == output {
			j := i + offset
			if j >= 0 && j < len(chain) {
				return &chain[j]
			}
			return nil
		}
	}
	return nil
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

var mdHref = regexp.MustCompile(`href="([^"]+?\.md)(#[^"]*)?"`)

// rewriteLinks repoints links to docs Markdown files at their HTML output.
func rewriteLinks(html string, m map[string]string) string {
	return mdHref.ReplaceAllStringFunc(html, func(s string) string {
		sub := mdHref.FindStringSubmatch(s)
		if out, ok := m[filepath.Base(sub[1])]; ok {
			return `href="` + out + sub[2] + `"`
		}
		return s
	})
}

func render(tmpl *template.Template, name, out string, data pageData) error {
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		return err
	}
	return os.WriteFile(out, buf.Bytes(), 0o644)
}
