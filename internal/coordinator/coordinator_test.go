package coordinator

import "testing"

// buildChunks is a pure function — no DB or RPC needed.

func TestBuildChunks_EvenDivision(t *testing.T) {
	// 100 blocks, chunk size 10 → exactly 10 chunks of equal size.
	chunks := buildChunks(100, 199, 10)
	if len(chunks) != 10 {
		t.Fatalf("len: got %d, want 10", len(chunks))
	}
	if chunks[0].FromBlock != 100 || chunks[0].ToBlock != 109 {
		t.Errorf("first chunk: got [%d,%d], want [100,109]", chunks[0].FromBlock, chunks[0].ToBlock)
	}
	if chunks[9].FromBlock != 190 || chunks[9].ToBlock != 199 {
		t.Errorf("last chunk: got [%d,%d], want [190,199]", chunks[9].FromBlock, chunks[9].ToBlock)
	}
}

func TestBuildChunks_LastChunkSmaller(t *testing.T) {
	// 105 blocks, chunk size 50 → 2 full + 1 partial.
	chunks := buildChunks(0, 104, 50)
	if len(chunks) != 3 {
		t.Fatalf("len: got %d, want 3", len(chunks))
	}
	last := chunks[2]
	if last.FromBlock != 100 || last.ToBlock != 104 {
		t.Errorf("last chunk: got [%d,%d], want [100,104]", last.FromBlock, last.ToBlock)
	}
}

func TestBuildChunks_SingleBlock(t *testing.T) {
	chunks := buildChunks(42, 42, 100)
	if len(chunks) != 1 {
		t.Fatalf("len: got %d, want 1", len(chunks))
	}
	if chunks[0].FromBlock != 42 || chunks[0].ToBlock != 42 {
		t.Errorf("chunk: got [%d,%d], want [42,42]", chunks[0].FromBlock, chunks[0].ToBlock)
	}
}

func TestBuildChunks_RangeEqualsChunkSize(t *testing.T) {
	chunks := buildChunks(0, 99, 100)
	if len(chunks) != 1 {
		t.Fatalf("len: got %d, want 1", len(chunks))
	}
	if chunks[0].FromBlock != 0 || chunks[0].ToBlock != 99 {
		t.Errorf("chunk: got [%d,%d], want [0,99]", chunks[0].FromBlock, chunks[0].ToBlock)
	}
}

func TestBuildChunks_CoversFullRange(t *testing.T) {
	from, to, size := 1000, 1099, 7
	chunks := buildChunks(from, to, size)

	if chunks[0].FromBlock != from {
		t.Errorf("first.From: got %d, want %d", chunks[0].FromBlock, from)
	}
	if chunks[len(chunks)-1].ToBlock != to {
		t.Errorf("last.To: got %d, want %d", chunks[len(chunks)-1].ToBlock, to)
	}
}

func TestBuildChunks_NoGapsOrOverlaps(t *testing.T) {
	chunks := buildChunks(0, 999, 100)
	for i := 1; i < len(chunks); i++ {
		prev, cur := chunks[i-1], chunks[i]
		if cur.FromBlock != prev.ToBlock+1 {
			t.Errorf("gap/overlap between chunk %d and %d: [%d,%d] vs [%d,%d]",
				i-1, i, prev.FromBlock, prev.ToBlock, cur.FromBlock, cur.ToBlock)
		}
	}
}
