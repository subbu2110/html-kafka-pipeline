package parser

import (
	"strings"
	"testing"
)

const simpleTable = `
<html><body>
<table>
  <tr><th>Name</th><th>Population</th><th>Area</th></tr>
  <tr><td>India</td><td>1,400,000,000</td><td>3,287,263</td></tr>
  <tr><td>China</td><td>1,300,000,000</td><td>9,596,960</td></tr>
</table>
</body></html>`

func TestParse_simpleTable(t *testing.T) {
	tbl, err := Parse(strings.NewReader(simpleTable), ParseOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tbl.Headers) != 3 {
		t.Fatalf("expected 3 headers, got %d: %v", len(tbl.Headers), tbl.Headers)
	}
	if tbl.Headers[0] != "Name" || tbl.Headers[1] != "Population" {
		t.Errorf("unexpected headers: %v", tbl.Headers)
	}
	if len(tbl.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(tbl.Rows))
	}
	if tbl.Rows[0][0] != "India" {
		t.Errorf("expected India, got %q", tbl.Rows[0][0])
	}
}

const multiTable = `
<html><body>
<table class="nav"><tr><th>Nav</th></tr><tr><td>Home</td></tr></table>
<table class="wikitable">
  <tr><th>Country</th><th>GDP</th></tr>
  <tr><td>USA</td><td>25000</td></tr>
</table>
</body></html>`

func TestParse_selectByCSSClass(t *testing.T) {
	tbl, err := Parse(strings.NewReader(multiTable), ParseOptions{CSSClass: "wikitable"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tbl.Headers[0] != "Country" {
		t.Errorf("expected wikitable, got headers %v", tbl.Headers)
	}
}

func TestParse_selectByIndex(t *testing.T) {
	tbl, err := Parse(strings.NewReader(multiTable), ParseOptions{TableIndex: 0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tbl.Headers[0] != "Nav" {
		t.Errorf("expected Nav table, got headers %v", tbl.Headers)
	}
}

const colspanTable = `
<html><body>
<table>
  <tr><th colspan="2">Name</th><th>Score</th></tr>
  <tr><td>Alice</td><td>Smith</td><td>95</td></tr>
</table>
</body></html>`

func TestParse_colspan(t *testing.T) {
	tbl, err := Parse(strings.NewReader(colspanTable), ParseOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// colspan=2 on the first header should expand to two "Name" entries.
	if len(tbl.Headers) != 3 {
		t.Fatalf("expected 3 headers after colspan expansion, got %d: %v", len(tbl.Headers), tbl.Headers)
	}
}

const rowspanTable = `
<html><body>
<table>
  <tr><th>Region</th><th>Country</th><th>Population</th></tr>
  <tr><td rowspan="2">Asia</td><td>India</td><td>1400000000</td></tr>
  <tr>                         <td>China</td><td>1300000000</td></tr>
  <tr><td rowspan="1">Europe</td><td>Germany</td><td>84000000</td></tr>
</table>
</body></html>`

func TestParse_rowspan(t *testing.T) {
	tbl, err := Parse(strings.NewReader(rowspanTable), ParseOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tbl.Rows) != 3 {
		t.Fatalf("expected 3 data rows, got %d", len(tbl.Rows))
	}
	// Both Asia rows must have "Asia" in column 0.
	if tbl.Rows[0][0] != "Asia" {
		t.Errorf("row 0 col 0: got %q, want \"Asia\"", tbl.Rows[0][0])
	}
	if tbl.Rows[1][0] != "Asia" {
		t.Errorf("row 1 col 0: got %q, want \"Asia\" (rowspan propagation failed)", tbl.Rows[1][0])
	}
	if tbl.Rows[1][1] != "China" {
		t.Errorf("row 1 col 1: got %q, want \"China\"", tbl.Rows[1][1])
	}
	if tbl.Rows[2][0] != "Europe" {
		t.Errorf("row 2 col 0: got %q, want \"Europe\"", tbl.Rows[2][0])
	}
}

func TestParse_noTable(t *testing.T) {
	_, err := Parse(strings.NewReader("<html><body><p>no table here</p></body></html>"), ParseOptions{})
	if err == nil {
		t.Error("expected error for page with no table")
	}
}

func TestParse_cssClassNotFound_fallsBackToIndex(t *testing.T) {
	// "dataTable" class doesn't exist; should fall back to TableIndex=1 (the wikitable).
	tbl, err := Parse(strings.NewReader(multiTable), ParseOptions{
		CSSClass:   "dataTable",
		TableIndex: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tbl.Headers[0] != "Country" {
		t.Errorf("expected fallback to index-1 table (Country), got headers %v", tbl.Headers)
	}
}

func TestParse_outOfBoundsIndex(t *testing.T) {
	// Should fall back to index 0 rather than panic.
	tbl, err := Parse(strings.NewReader(simpleTable), ParseOptions{TableIndex: 99})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tbl.Rows) == 0 {
		t.Error("expected rows from fallback table")
	}
}
