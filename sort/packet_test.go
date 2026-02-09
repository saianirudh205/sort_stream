package sort

import (
	"testing"
)

// ------------------------------------------------------------
// Happy path
// ------------------------------------------------------------

func TestParsePacket_ValidLine(t *testing.T) {
	line := "123,John Doe,Sales,Europe"

	p, err := parsePacket(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if p.RawData != line {
		t.Fatalf("RawData mismatch: got=%q want=%q", p.RawData, line)
	}

	if p.ID != 123 {
		t.Fatalf("ID mismatch: got=%d want=123", p.ID)
	}

	if p.Name != "John Doe" {
		t.Fatalf("Name mismatch: got=%q want=%q", p.Name, "John Doe")
	}

	if p.Continent != "Europe" {
		t.Fatalf("Continent mismatch: got=%q want=%q", p.Continent, "Europe")
	}
}

// ------------------------------------------------------------
// Structural failures
// ------------------------------------------------------------

func TestParsePacket_MissingFirstComma(t *testing.T) {
	_, err := parsePacket("123John,Sales,Europe")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestParsePacket_MissingSecondComma(t *testing.T) {
	_, err := parsePacket("123,JohnSalesEurope")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestParsePacket_MissingThirdComma(t *testing.T) {
	_, err := parsePacket("123,John,SalesEurope")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

// ------------------------------------------------------------
// ID parsing failures
// ------------------------------------------------------------

func TestParsePacket_NonNumericID(t *testing.T) {
	_, err := parsePacket("abc,John,Sales,Europe")
	if err == nil {
		t.Fatalf("expected error for non-numeric ID")
	}
}

func TestParsePacket_NegativeID(t *testing.T) {
	p, err := parsePacket("-42,John,Sales,Europe")
	if err != nil {
		t.Fatalf("negative IDs should parse, got error: %v", err)
	}
	if p.ID != -42 {
		t.Fatalf("ID mismatch: got=%d want=-42", p.ID)
	}
}

// ------------------------------------------------------------
// Edge cases (these reveal real bugs)
// ------------------------------------------------------------

func TestParsePacket_EmptyFields(t *testing.T) {
	line := "123,,Sales,"
	p, err := parsePacket(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if p.Name != "" {
		t.Fatalf("expected empty Name, got=%q", p.Name)
	}

	if p.Continent != "" {
		t.Fatalf("expected empty Continent, got=%q", p.Continent)
	}
}

func TestParsePacket_ExtraCommasInTail(t *testing.T) {
	line := "1,John,Sales,North,America"

	p, err := parsePacket(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if p.Continent != "North,America" {
		t.Fatalf("continent parsing broken: got=%q", p.Continent)
	}
}

func TestParsePacket_WhitespacePreserved(t *testing.T) {
	line := "7, John Doe , Sales , Europe "

	p, err := parsePacket(line)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if p.Name != " John Doe " {
		t.Fatalf("whitespace should be preserved, got=%q", p.Name)
	}

	if p.Continent != " Europe " {
		t.Fatalf("whitespace should be preserved, got=%q", p.Continent)
	}
}
