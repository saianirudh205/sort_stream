package sort

import (

	"container/heap"
	"testing"
	
)


// ============================================================
// calculateBackoff
// ============================================================


// ============================================================
// MergeHeap
// ============================================================

func TestMergeHeap_SortByID(t *testing.T) {
	h := &MergeHeap{sortKey: "id"}
	heap.Init(h)

	heap.Push(h, MergeItem{packet: Packet{ID: 3}})
	heap.Push(h, MergeItem{packet: Packet{ID: 1}})
	heap.Push(h, MergeItem{packet: Packet{ID: 2}})

	if heap.Pop(h).(MergeItem).packet.ID != 1 {
		t.Fatal("expected ID=1")
	}
	if heap.Pop(h).(MergeItem).packet.ID != 2 {
		t.Fatal("expected ID=2")
	}
	if heap.Pop(h).(MergeItem).packet.ID != 3 {
		t.Fatal("expected ID=3")
	}
}

func TestMergeHeap_SortByName(t *testing.T) {
	h := &MergeHeap{sortKey: "name"}
	heap.Init(h)

	heap.Push(h, MergeItem{packet: Packet{Name: "Charlie"}})
	heap.Push(h, MergeItem{packet: Packet{Name: "Alice"}})
	heap.Push(h, MergeItem{packet: Packet{Name: "Bob"}})

	if heap.Pop(h).(MergeItem).packet.Name != "Alice" {
		t.Fatal("expected Alice")
	}
	if heap.Pop(h).(MergeItem).packet.Name != "Bob" {
		t.Fatal("expected Bob")
	}
	if heap.Pop(h).(MergeItem).packet.Name != "Charlie" {
		t.Fatal("expected Charlie")
	}
}

func TestMergeHeap_SortByContinent(t *testing.T) {
	h := &MergeHeap{sortKey: "continent"}
	heap.Init(h)

	heap.Push(h, MergeItem{packet: Packet{Continent: "Europe"}})
	heap.Push(h, MergeItem{packet: Packet{Continent: "Africa"}})
	heap.Push(h, MergeItem{packet: Packet{Continent: "Asia"}})

	if heap.Pop(h).(MergeItem).packet.Continent != "Africa" {
		t.Fatal("expected Africa")
	}
	if heap.Pop(h).(MergeItem).packet.Continent != "Asia" {
		t.Fatal("expected Asia")
	}
	if heap.Pop(h).(MergeItem).packet.Continent != "Europe" {
		t.Fatal("expected Europe")
	}
}
