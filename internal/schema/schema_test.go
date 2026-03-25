package schema

import (
	"strings"
	"testing"
)

func TestCleanValue(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"India[1]", "India"},
		{"  1,234,567 [a] ", "1,234,567"},
		{"—", ""},
		{"N/A", ""},
		{"", ""},
		{"Hello", "Hello"},
	}
	for _, tc := range cases {
		if got := CleanValue(tc.in); got != tc.want {
			t.Errorf("CleanValue(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSanitiseName(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"Country / Territory", "country_territory"},
		{"% of World", "of_world"},
		{"Rank[1]", "rank_"}, // "rank" is a reserved word
		{"", "col"},
		{"GDP (nominal)", "gdp_nominal"},
		// 65-char input must be truncated to 64 chars (MySQL column name limit).
		{
			"This_Is_A_Very_Long_Column_Header_Name_That_Exceeds_MySQL_Limit_X",
			"this_is_a_very_long_column_header_name_that_exceeds_mysql_limit_",
		},
	}
	for _, tc := range cases {
		if got := SanitiseName(tc.in); got != tc.want {
			t.Errorf("SanitiseName(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestInfer_duplicateColumnNames(t *testing.T) {
	// "% change" and "$ change" both sanitise to "change".
	// Two empty <th> cells both sanitise to "col".
	headers := []string{"% change", "$ change", "", ""}
	rows := [][]string{{"1", "2", "3", "4"}}

	sc := Infer("t", headers, rows)

	names := make([]string, len(sc.Columns))
	for i, c := range sc.Columns {
		names[i] = c.Name
	}

	// All four names must be distinct — duplicates would cause a DDL syntax error.
	seen := map[string]bool{}
	for _, n := range names {
		if seen[n] {
			t.Errorf("duplicate column name %q in %v", n, names)
		}
		seen[n] = true
	}
	// First occurrence keeps the base name; subsequent ones get _2, _3 ...
	if names[0] != "change" {
		t.Errorf("names[0] = %q, want \"change\"", names[0])
	}
	if names[1] != "change_2" {
		t.Errorf("names[1] = %q, want \"change_2\"", names[1])
	}
	if names[2] != "col" {
		t.Errorf("names[2] = %q, want \"col\"", names[2])
	}
	if names[3] != "col_2" {
		t.Errorf("names[3] = %q, want \"col_2\"", names[3])
	}
}

func TestInfer_yearNotDate(t *testing.T) {
	// Bare 4-digit years must infer as BIGINT, not DATE.
	headers := []string{"year"}
	rows := [][]string{{"2020"}, {"2021"}, {"2022"}}
	sc := Infer("t", headers, rows)
	if sc.Columns[0].Type != TypeBigInt {
		t.Errorf("year column: got %q, want BIGINT", sc.Columns[0].Type)
	}
}

func TestInfer_currencyColumns(t *testing.T) {
	headers := []string{"price", "market_cap", "change_pct"}
	rows := [][]string{
		{"$1,234", "€5,000,000", "3.5%"},
		{"$999", "€1,200,000", "1.2%"},
		{"$10,000", "€800,000", "-0.5%"},
	}
	sc := Infer("t", headers, rows)

	want := map[string]ColumnType{
		"price":      TypeBigInt,
		"market_cap": TypeBigInt,
		"change_pct": TypeDouble,
	}
	for _, col := range sc.Columns {
		if exp, ok := want[col.Name]; ok && col.Type != exp {
			t.Errorf("column %q: got %q, want %q", col.Name, col.Type, exp)
		}
	}
}

func TestInfer_types(t *testing.T) {
	headers := []string{"name", "population", "pct", "founded", "notes"}
	rows := [][]string{
		{"India", "1,400,000,000", "17.5%", "1947-08-15", "Large country"},
		{"China", "1,300,000,000", "16.2%", "1949-10-01", "Also large"},
		{"USA", "331,000,000", "4.2%", "1776-07-04", ""},
	}

	sc := Infer("test_table", headers, rows)

	want := map[string]ColumnType{
		"name":       TypeVarchar,
		"population": TypeBigInt,
		"pct":        TypeDouble,
		"founded":    TypeDate,
		"notes":      TypeVarchar,
	}
	for _, col := range sc.Columns {
		if expected, ok := want[col.Name]; ok {
			if col.Type != expected {
				t.Errorf("column %q: got type %q, want %q", col.Name, col.Type, expected)
			}
		}
	}
}

func TestInfer_allText(t *testing.T) {
	headers := []string{"city"}
	rows := [][]string{{"New York"}, {"Los Angeles"}, {"Chicago"}}
	sc := Infer("cities", headers, rows)
	if sc.Columns[0].Type != TypeVarchar {
		t.Errorf("expected VARCHAR, got %q", sc.Columns[0].Type)
	}
}

func TestInfer_mixed_int_float(t *testing.T) {
	// A column with integers and one float should become DOUBLE.
	headers := []string{"value"}
	rows := [][]string{{"1"}, {"2"}, {"3.5"}}
	sc := Infer("t", headers, rows)
	if sc.Columns[0].Type != TypeDouble {
		t.Errorf("expected DOUBLE, got %q", sc.Columns[0].Type)
	}
}

func TestInfer_emptyRowsSkipped(t *testing.T) {
	headers := []string{"num"}
	rows := [][]string{{"42"}, {""}, {"—"}, {"100"}}
	sc := Infer("t", headers, rows)
	if sc.Columns[0].Type != TypeBigInt {
		t.Errorf("empty/null rows should not downgrade type; got %q", sc.Columns[0].Type)
	}
}

func TestCreateTableSQL(t *testing.T) {
	headers := []string{"country", "population"}
	rows := [][]string{{"India", "1400000000"}}
	sc := Infer("countries", headers, rows)
	ddl := sc.CreateTableSQL(DialectMySQL)

	for _, must := range []string{
		"CREATE TABLE IF NOT EXISTS",
		"`countries`",
		"`_id` BIGINT AUTO_INCREMENT PRIMARY KEY",
		"`_row_hash` VARCHAR(64)",
		"`country`",
		"`population`",
	} {
		if !strings.Contains(ddl, must) {
			t.Errorf("DDL missing %q\nGot:\n%s", must, ddl)
		}
	}
}

func TestInfer_timestampColumns(t *testing.T) {
	headers := []string{"created_at", "date_only"}
	rows := [][]string{
		{"2024-01-15 09:30:00", "2024-01-15"},
		{"2024-02-20 14:45:00", "2024-02-20"},
	}
	sc := Infer("events", headers, rows)
	colMap := map[string]ColumnType{}
	for _, col := range sc.Columns {
		colMap[col.Name] = col.Type
	}
	if colMap["created_at"] != TypeTimestamp {
		t.Errorf("created_at: got %q, want TIMESTAMP", colMap["created_at"])
	}
	if colMap["date_only"] != TypeDate {
		t.Errorf("date_only: got %q, want DATE", colMap["date_only"])
	}
}

func TestCreateTableSQL_Postgres(t *testing.T) {
	headers := []string{"country", "population"}
	rows := [][]string{{"India", "1400000000"}}
	sc := Infer("countries", headers, rows)
	ddl := sc.CreateTableSQL(DialectPostgres)

	for _, must := range []string{
		"CREATE TABLE IF NOT EXISTS",
		`"countries"`,
		`"_id" BIGSERIAL PRIMARY KEY`,
		`"_row_hash" VARCHAR(64)`,
		`"country"`,
		`"population"`,
	} {
		if !strings.Contains(ddl, must) {
			t.Errorf("Postgres DDL missing %q\nGot:\n%s", must, ddl)
		}
	}
}
