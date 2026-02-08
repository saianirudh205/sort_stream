package sort

import (
	"testing"
)



func TestVerifySorted_ID(t *testing.T) {
	data := []Packet{
		{ID: 1},
		{ID: 2},
		{ID: 3},
	}

	if !verifySorted(data, "id") {
		t.Fatalf("expected data to be sorted by id")
	}
}

func TestVerifySorted_ID_Fail(t *testing.T) {
	data := []Packet{
		{ID: 2},
		{ID: 1},
	}

	if verifySorted(data, "id") {
		t.Fatalf("expected id sort to fail")
	}
}

func TestVerifySorted_Name(t *testing.T) {
	data := []Packet{
		{Name: "Alice"},
		{Name: "Bob"},
		{Name: "Charlie"},
	}

	if !verifySorted(data, "name") {
		t.Fatalf("expected data to be sorted by name")
	}
}

func TestVerifySorted_Continent(t *testing.T) {
	data := []Packet{
		{Continent: "Africa"},
		{Continent: "Asia"},
		{Continent: "Europe"},
	}

	if !verifySorted(data, "continent") {
		t.Fatalf("expected data to be sorted by continent")
	}
}
