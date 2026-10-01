package server

import (
	"bytes"
	"errors"
	"testing"
)

func TestPartBufferEmitsExactParts(t *testing.T) {
	const size = 10
	data := make([]byte, 37)
	for i := range data {
		data[i] = byte(i)
	}

	// Chunk sizes chosen to straddle part boundaries in awkward ways.
	for _, chunk := range []int{1, 3, 7, 10, 11, 37} {
		var parts [][]byte
		p := &partBuffer{size: size, flush: func(b []byte) error {
			parts = append(parts, append([]byte(nil), b...)) // copy: buf is reused
			return nil
		}}
		for off := 0; off < len(data); off += chunk {
			if err := p.write(data[off:min(off+chunk, len(data))]); err != nil {
				t.Fatal(err)
			}
		}
		if err := p.finish(); err != nil {
			t.Fatal(err)
		}

		if len(parts) != 4 {
			t.Fatalf("chunk=%d: got %d parts, want 4", chunk, len(parts))
		}
		for i, part := range parts[:3] {
			if len(part) != size {
				t.Fatalf("chunk=%d: part %d is %d bytes, want %d", chunk, i, len(part), size)
			}
		}
		if got := bytes.Join(parts, nil); !bytes.Equal(got, data) {
			t.Fatalf("chunk=%d: reassembled bytes differ", chunk)
		}
	}
}

func TestPartBufferFinishWithNothingBuffered(t *testing.T) {
	calls := 0
	p := &partBuffer{size: 4, flush: func([]byte) error { calls++; return nil }}
	if err := p.write([]byte{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if err := p.finish(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("got %d flushes, want 1 (no empty final part)", calls)
	}
}

func TestPartBufferPropagatesFlushError(t *testing.T) {
	boom := errors.New("s3 down")
	p := &partBuffer{size: 2, flush: func([]byte) error { return boom }}
	if err := p.write([]byte{1, 2, 3}); !errors.Is(err, boom) {
		t.Fatalf("got %v, want flush error", err)
	}
}
