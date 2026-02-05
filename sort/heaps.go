package sort

// ---------------------------------------------------------------------------
// Index-based heaps – all three share a *[]Packet (pointer-to-slice).
// This is the critical part: every heap dereferences the SAME pointer,
// so when AddPacket appends or flush resets the slice, all heaps see it
// instantly. No stale references, no leaked memory.
// ---------------------------------------------------------------------------

type IDHeap struct {
	idx     []int32
	packets *[]Packet // pointer-to-slice, shared
}

func (h IDHeap) Len() int            { return len(h.idx) }
func (h IDHeap) Less(i, j int) bool  { return (*h.packets)[h.idx[i]].ID < (*h.packets)[h.idx[j]].ID }
func (h IDHeap) Swap(i, j int)       { h.idx[i], h.idx[j] = h.idx[j], h.idx[i] }
func (h *IDHeap) Push(x interface{}) { h.idx = append(h.idx, x.(int32)) }
func (h *IDHeap) Pop() interface{} {
	n := len(h.idx)
	v := h.idx[n-1]
	h.idx = h.idx[:n-1]
	return v
}

type NameHeap struct {
	idx     []int32
	packets *[]Packet
}

func (h NameHeap) Len() int            { return len(h.idx) }
func (h NameHeap) Less(i, j int) bool  { return (*h.packets)[h.idx[i]].Name < (*h.packets)[h.idx[j]].Name }
func (h NameHeap) Swap(i, j int)       { h.idx[i], h.idx[j] = h.idx[j], h.idx[i] }
func (h *NameHeap) Push(x interface{}) { h.idx = append(h.idx, x.(int32)) }
func (h *NameHeap) Pop() interface{} {
	n := len(h.idx)
	v := h.idx[n-1]
	h.idx = h.idx[:n-1]
	return v
}

type ContinentHeap struct {
	idx     []int32
	packets *[]Packet
}

func (h ContinentHeap) Len() int { return len(h.idx) }
func (h ContinentHeap) Less(i, j int) bool {
	return (*h.packets)[h.idx[i]].Continent < (*h.packets)[h.idx[j]].Continent
}
func (h ContinentHeap) Swap(i, j int)       { h.idx[i], h.idx[j] = h.idx[j], h.idx[i] }
func (h *ContinentHeap) Push(x interface{}) { h.idx = append(h.idx, x.(int32)) }
func (h *ContinentHeap) Pop() interface{} {
	n := len(h.idx)
	v := h.idx[n-1]
	h.idx = h.idx[:n-1]
	return v
}
