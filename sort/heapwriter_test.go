package sort

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// ------------------------------------------------------------
// Helpers
// ------------------------------------------------------------

func readLines(t *testing.T, path string) []string {
	t.Helper()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read %s: %v", path, err)
	}
	out := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(out) == 1 && out[0] == "" {
		return nil
	}
	return out
}

func assertEqual(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("length mismatch: got=%d want=%d\n%v", len(got), len(want), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("mismatch at %d: got=%q want=%q", i, got[i], want[i])
		}
	}
}

// ------------------------------------------------------------
// Tests
// ------------------------------------------------------------

func TestHeapWriter_FlushOnFlushSize(t *testing.T) {
	dir := t.TempDir()

	hw, err := NewHeapWriter(0, dir, 2)
	if err != nil {
		t.Fatal(err)
	}

	hw.AddPacket(Packet{ID: 2, Name: "B", Continent: "EU", RawData: "2,B,X,EU"})
	hw.AddPacket(Packet{ID: 1, Name: "A", Continent: "AS", RawData: "1,A,X,AS"})

	if hw.GetTotalWritten() != 2 {
		t.Fatalf("expected 2 written, got %d", hw.GetTotalWritten())
	}

	hw.Close()
}

func TestHeapWriter_SortedByID(t *testing.T) {
	dir := t.TempDir()
	hw, _ := NewHeapWriter(0, dir, 10)

	hw.AddPacket(Packet{ID: 3, Name: "C", Continent: "EU", RawData: "3,C,X,EU"})
	hw.AddPacket(Packet{ID: 1, Name: "A", Continent: "AS", RawData: "1,A,X,AS"})
	hw.AddPacket(Packet{ID: 2, Name: "B", Continent: "AF", RawData: "2,B,X,AF"})
	hw.Close()

	lines := readLines(t, filepath.Join(dir, "id-p0.txt"))

	assertEqual(t, lines, []string{
		"1,A,X,AS",
		"2,B,X,AF",
		"3,C,X,EU",
	})
}

func TestHeapWriter_SortedByName(t *testing.T) {
	dir := t.TempDir()
	hw, _ := NewHeapWriter(0, dir, 10)

	hw.AddPacket(Packet{ID: 1, Name: "Charlie", Continent: "EU", RawData: "1,Charlie,X,EU"})
	hw.AddPacket(Packet{ID: 2, Name: "Alice", Continent: "AS", RawData: "2,Alice,X,AS"})
	hw.AddPacket(Packet{ID: 3, Name: "Bob", Continent: "AF", RawData: "3,Bob,X,AF"})
	hw.Close()

	lines := readLines(t, filepath.Join(dir, "name-p0.txt"))

	assertEqual(t, lines, []string{
		"2,Alice,X,AS",
		"3,Bob,X,AF",
		"1,Charlie,X,EU",
	})
}

func TestHeapWriter_SortedByContinent_Grouped(t *testing.T) {
	dir := t.TempDir()
	hw, _ := NewHeapWriter(0, dir, 10)

	hw.AddPacket(Packet{ID: 1, Name: "A", Continent: "Europe", RawData: "1,A,X,Europe"})
	hw.AddPacket(Packet{ID: 2, Name: "B", Continent: "Asia", RawData: "2,B,X,Asia"})
	hw.AddPacket(Packet{ID: 3, Name: "C", Continent: "Asia", RawData: "3,C,X,Asia"})
	hw.AddPacket(Packet{ID: 4, Name: "D", Continent: "Africa", RawData: "4,D,X,Africa"})
	hw.Close()

	lines := readLines(t, filepath.Join(dir, "continent-p0.txt"))

	// heap is NOT stable; only assert grouping order
	if !strings.HasPrefix(lines[0], "4,D,X,Africa") {
		t.Fatalf("Africa should come first, got %v", lines)
	}
}

func TestHeapWriter_MultipleFlushCycles(t *testing.T) {
	dir := t.TempDir()
	hw, _ := NewHeapWriter(0, dir, 2)

	for i := 0; i < 5; i++ {
		hw.AddPacket(Packet{
			ID:        i,
			Name:      "N",
			Continent: "C",
			RawData:   fmt.Sprintf("%d", i),
		})
	}
	hw.Close()

	if hw.GetTotalWritten() != 5 {
		t.Fatalf("expected 5 written, got %d", hw.GetTotalWritten())
	}
}

func TestHeapWriter_ConcurrentAddPacket(t *testing.T) {
	dir := t.TempDir()
	hw, _ := NewHeapWriter(0, dir, 50)

	var wg sync.WaitGroup
	total := 100

	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hw.AddPacket(Packet{
				ID:        i,
				Name:      "X",
				Continent: "Y",
				RawData:   "x",
			})
		}(i)
	}

	wg.Wait()
	hw.Close()

	if hw.GetTotalWritten() != total {
		t.Fatalf("concurrent write lost data: got=%d want=%d",
			hw.GetTotalWritten(), total)
	}
}
