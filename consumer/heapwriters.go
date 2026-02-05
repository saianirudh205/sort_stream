package main

import (
	"bufio"
	"container/heap"
	
	"fmt"
	
	"os"
	
	
	"sync"
	"time"

	
)

type HeapWriter struct {
	packets *[]Packet // pointer-to-slice – the single source of truth

	idHeap        *IDHeap
	nameHeap      *NameHeap
	continentHeap *ContinentHeap

	idFile          *os.File
	nameFile        *os.File
	continentFile   *os.File
	idWriter        *bufio.Writer
	nameWriter      *bufio.Writer
	continentWriter *bufio.Writer

	flushSize    int
	totalWritten int
	mu           sync.Mutex
}

func NewHeapWriter(partition int, tempDir string, flushSize int) (*HeapWriter, error) {
	openFile := func(name string) (*os.File, error) {
		return os.Create(fmt.Sprintf("%s/%s-p%d.txt", tempDir, name, partition))
	}

	idFile, err := openFile("id")
	if err != nil {
		return nil, err
	}
	nameFile, err := openFile("name")
	if err != nil {
		idFile.Close()
		return nil, err
	}
	continentFile, err := openFile("continent")
	if err != nil {
		idFile.Close()
		nameFile.Close()
		return nil, err
	}

	packets := make([]Packet, 0, flushSize)
	pPtr := &packets // one pointer, shared by everything

	return &HeapWriter{
		packets:       pPtr,
		idHeap:        &IDHeap{idx: make([]int32, 0, flushSize), packets: pPtr},
		nameHeap:      &NameHeap{idx: make([]int32, 0, flushSize), packets: pPtr},
		continentHeap: &ContinentHeap{idx: make([]int32, 0, flushSize), packets: pPtr},

		idFile:          idFile,
		nameFile:        nameFile,
		continentFile:   continentFile,
		idWriter:        bufio.NewWriterSize(idFile, 1<<20),
		nameWriter:      bufio.NewWriterSize(nameFile, 1<<20),
		continentWriter: bufio.NewWriterSize(continentFile, 1<<20),

		flushSize: flushSize,
	}, nil
}

func (hw *HeapWriter) AddPacket(p Packet) error {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	idx := int32(len(*hw.packets))
	*hw.packets = append(*hw.packets, p)
	// No re-pointing needed. Heaps hold pPtr, they dereference it every time.

	heap.Push(hw.idHeap, idx)
	heap.Push(hw.nameHeap, idx)
	heap.Push(hw.continentHeap, idx)

	if len(*hw.packets) >= hw.flushSize {
		return hw.flush()
	}
	return nil
}

func (hw *HeapWriter) flush() error {
	pkts := *hw.packets

	for hw.idHeap.Len() > 0 {
		idx := heap.Pop(hw.idHeap).(int32)
		hw.idWriter.WriteString(pkts[idx].RawData)
		hw.idWriter.WriteByte('\n')
	}
	if err := hw.idWriter.Flush(); err != nil {
		return err
	}

	for hw.nameHeap.Len() > 0 {
		idx := heap.Pop(hw.nameHeap).(int32)
		hw.nameWriter.WriteString(pkts[idx].RawData)
		hw.nameWriter.WriteByte('\n')
	}
	if err := hw.nameWriter.Flush(); err != nil {
		return err
	}

	for hw.continentHeap.Len() > 0 {
		idx := heap.Pop(hw.continentHeap).(int32)
		hw.continentWriter.WriteString(pkts[idx].RawData)
		hw.continentWriter.WriteByte('\n')
	}
	if err := hw.continentWriter.Flush(); err != nil {
		return err
	}

	hw.totalWritten += len(pkts)

	// Reset. The backing array stays allocated for reuse – only length goes to 0.
	*hw.packets = (*hw.packets)[:0]
	hw.idHeap.idx = hw.idHeap.idx[:0]
	hw.nameHeap.idx = hw.nameHeap.idx[:0]
	hw.continentHeap.idx = hw.continentHeap.idx[:0]

	return nil
}

func (hw *HeapWriter) Close() error {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	if len(*hw.packets) > 0 {
		if err := hw.flush(); err != nil {
			return err
		}
	}

	hw.idWriter.Flush()
	hw.nameWriter.Flush()
	hw.continentWriter.Flush()

	hw.idFile.Close()
	hw.nameFile.Close()
	hw.continentFile.Close()
	return nil
}

func (hw *HeapWriter) GetTotalWritten() int {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	return hw.totalWritten
}
