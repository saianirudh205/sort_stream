package sort

import (
	"fmt"
	"strconv"
	"strings"
)

// ---------------------------------------------------------------------------
// Packet
// ---------------------------------------------------------------------------

type Packet struct {
	RawData   string
	ID        int
	Name      string
	Continent string
}

func parsePacket(line string) (Packet, error) {
	f1 := strings.IndexByte(line, ',')
	if f1 < 0 {
		return Packet{}, fmt.Errorf("bad line: missing field 1")
	}
	f2 := strings.IndexByte(line[f1+1:], ',')
	if f2 < 0 {
		return Packet{}, fmt.Errorf("bad line: missing field 2")
	}
	f2 += f1 + 1
	f3 := strings.IndexByte(line[f2+1:], ',')
	if f3 < 0 {
		return Packet{}, fmt.Errorf("bad line: missing field 3")
	}
	f3 += f2 + 1

	id, err := strconv.Atoi(line[:f1])
	if err != nil {
		return Packet{}, fmt.Errorf("bad id %q: %w", line[:f1], err)
	}

	return Packet{
		RawData:   line,
		ID:        id,
		Name:      line[f1+1 : f2],
		Continent: line[f3+1:],
	}, nil
}
