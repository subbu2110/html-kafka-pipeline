// Package parser extracts structured tabular data from raw HTML.
package parser

import (
	"fmt"
	"io"
	"log/slog"
	"strings"

	"golang.org/x/net/html"
)

// Table holds the parsed headers and rows from an HTML <table>.
type Table struct {
	Headers []string
	Rows    [][]string
}

// ParseOptions controls which table is selected from the page.
type ParseOptions struct {
	// TableIndex selects the Nth table (0-based) when CSSClass is empty or unmatched.
	TableIndex int
	// CSSClass prefers a table whose class attribute contains this value.
	CSSClass string
}

// Parse reads HTML from r and extracts a Table according to opts.
func Parse(r io.Reader, opts ParseOptions) (*Table, error) {
	doc, err := html.Parse(r)
	if err != nil {
		return nil, fmt.Errorf("parse HTML: %w", err)
	}

	tables := findTables(doc)
	if len(tables) == 0 {
		return nil, fmt.Errorf("no <table> elements found in document")
	}

	node := selectTable(tables, opts)
	return extractTable(node)
}

// findTables returns all <table> nodes in document order.
func findTables(root *html.Node) []*html.Node {
	var tables []*html.Node
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "table" {
			tables = append(tables, n)
			return // don't recurse into nested tables
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(root)
	return tables
}

// selectTable picks the best matching table node from the list.
func selectTable(tables []*html.Node, opts ParseOptions) *html.Node {
	if opts.CSSClass != "" {
		for _, t := range tables {
			if nodeHasClass(t, opts.CSSClass) {
				return t
			}
		}
		slog.Warn("CSS class not found, falling back to index", "class", opts.CSSClass, "tables", len(tables), "index", opts.TableIndex)
	}
	idx := opts.TableIndex
	if idx < 0 || idx >= len(tables) {
		slog.Warn("table index out of range, using 0", "index", opts.TableIndex, "tables", len(tables))
		idx = 0
	}
	return tables[idx]
}

func nodeHasClass(n *html.Node, class string) bool {
	for _, a := range n.Attr {
		if a.Key == "class" {
			for _, c := range strings.Fields(a.Val) {
				if c == class {
					return true
				}
			}
		}
	}
	return false
}

// activeSpan tracks a cell value that spans multiple rows via rowspan.
type activeSpan struct {
	text     string
	rowsLeft int
}

// extractTable builds a fully resolved 2D grid from a <table> node, correctly
// handling both colspan and rowspan, then splits it into headers + data rows.
//
// Algorithm: maintain a map of col → activeSpan for cells whose rowspan extends
// into future rows. Before processing each <tr>, pre-fill any columns that are
// still covered by an active span, then walk the actual <td>/<th> cells,
// skipping occupied columns and registering new spans as we go.
func extractTable(table *html.Node) (*Table, error) {
	type gridRow struct {
		cells     []string
		hasHeader bool // true when the source <tr> contained at least one <th>
	}

	allRows := collectRows(table)
	if len(allRows) == 0 {
		return nil, fmt.Errorf("table has no rows")
	}

	var grid []gridRow
	pending := map[int]activeSpan{} // col index → still-active rowspan

	for _, tr := range allRows {
		// colMap holds the resolved text for each column in this row.
		colMap := map[int]string{}

		// Pre-fill columns covered by rowspans from previous rows.
		for col, sp := range pending {
			colMap[col] = sp.text
		}

		// Walk the physical cells in this <tr>.
		// newSpans collects rowspans born in THIS row so they are not
		// decremented until future rows consume them.
		col := 0
		newSpans := map[int]activeSpan{}
		for c := tr.FirstChild; c != nil; c = c.NextSibling {
			if c.Type != html.ElementNode || (c.Data != "td" && c.Data != "th") {
				continue
			}
			// Advance past columns already occupied (by rowspan or earlier cells).
			for {
				if _, occupied := colMap[col]; !occupied {
					break
				}
				col++
			}

			text := strings.TrimSpace(nodeText(c))
			cspan := attrInt(c, "colspan", 1)
			rspan := attrInt(c, "rowspan", 1)

			for cs := 0; cs < cspan; cs++ {
				colMap[col+cs] = text
				if rspan > 1 {
					newSpans[col+cs] = activeSpan{text: text, rowsLeft: rspan - 1}
				}
			}
			col += cspan
		}

		// Decrement spans from PREVIOUS rows; evict those that have expired.
		for c, sp := range pending {
			sp.rowsLeft--
			if sp.rowsLeft <= 0 {
				delete(pending, c)
			} else {
				pending[c] = sp
			}
		}

		// Register spans born in this row so they are active for future rows.
		for c, sp := range newSpans {
			pending[c] = sp
		}

		// Convert colMap to a dense slice.
		maxCol := -1
		for c := range colMap {
			if c > maxCol {
				maxCol = c
			}
		}
		if maxCol < 0 {
			continue // skip completely empty rows
		}
		cells := make([]string, maxCol+1)
		for c, text := range colMap {
			cells[c] = text
		}
		grid = append(grid, gridRow{cells: cells, hasHeader: rowHasTag(tr, "th")})
	}

	if len(grid) == 0 {
		return nil, fmt.Errorf("could not detect column headers in table")
	}

	// First grid row that has <th> cells becomes the header.
	// Fallback: use the very first row.
	headerIdx := 0
	for i, gr := range grid {
		if gr.hasHeader {
			headerIdx = i
			break
		}
	}

	headers := grid[headerIdx].cells
	if len(headers) == 0 {
		return nil, fmt.Errorf("could not detect column headers in table")
	}

	var rows [][]string
	for i, gr := range grid {
		if i == headerIdx {
			continue
		}
		// Normalise row length to match header count.
		row := make([]string, len(headers))
		for j := 0; j < len(headers) && j < len(gr.cells); j++ {
			row[j] = gr.cells[j]
		}
		rows = append(rows, row)
	}

	return &Table{Headers: headers, Rows: rows}, nil
}

// collectRows returns all <tr> nodes inside the table in document order.
func collectRows(table *html.Node) []*html.Node {
	var rows []*html.Node
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "tr" {
			rows = append(rows, n)
			return
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(table)
	return rows
}

func rowHasTag(tr *html.Node, tag string) bool {
	for c := tr.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && c.Data == tag {
			return true
		}
	}
	return false
}

// nodeText recursively extracts all visible text from a node.
func nodeText(n *html.Node) string {
	var sb strings.Builder
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.TextNode {
			sb.WriteString(node.Data)
		}
		for c := node.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(n)
	return sb.String()
}

func attrInt(n *html.Node, key string, def int) int {
	for _, a := range n.Attr {
		if a.Key == key {
			var v int
			if _, err := fmt.Sscan(a.Val, &v); err == nil && v > 0 {
				return v
			}
		}
	}
	return def
}
