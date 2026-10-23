package observer

import (
	"fmt"
	"io"
	"strings"
	"unicode"

	"github.com/andybalholm/cascadia"
	"golang.org/x/net/html"
)

// Normalize keeps block boundaries for readable diffs and preserves inline text
// adjacency (e.g. Hel<b>lo</b>). Source indentation has no semantic significance.
func Normalize(r io.Reader, ignore []string) (string, error) {
	doc, err := html.Parse(r)
	if err != nil {
		return "", err
	}
	ignored := make(map[*html.Node]struct{})
	for _, rule := range ignore {
		sel, err := cascadia.Compile(rule)
		if err != nil {
			return "", fmt.Errorf("invalid selector: %w", err)
		}
		for _, n := range cascadia.QueryAll(doc, sel) {
			ignored[n] = struct{}{}
		}
	}
	for n := range ignored {
		if n.Parent != nil {
			n.Parent.RemoveChild(n)
		}
	}
	var text strings.Builder
	lines := []string{}
	flush := func() {
		line := strings.Join(strings.FieldsFunc(text.String(), unicode.IsSpace), " ")
		if line != "" {
			lines = append(lines, line)
		}
		text.Reset()
	}
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch n.Data {
			case "script", "style", "head", "template":
				return
			}
			if isBlock(n.Data) {
				flush()
			}
		}
		if n.Type == html.TextNode {
			text.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
		if n.Type == html.ElementNode && isBlock(n.Data) {
			flush()
		}
	}
	walk(doc)
	flush()
	return strings.Join(lines, "\n"), nil
}

func isBlock(tag string) bool {
	switch tag {
	case "address", "article", "aside", "blockquote", "br", "dd", "div", "dl", "dt", "fieldset", "figcaption", "figure", "footer", "form", "h1", "h2", "h3", "h4", "h5", "h6", "header", "hr", "li", "main", "nav", "ol", "p", "pre", "section", "table", "tbody", "td", "th", "thead", "tr", "ul":
		return true
	}
	return false
}
