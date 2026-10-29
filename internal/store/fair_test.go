package store

import (
	"slices"
	"testing"

	"github.com/google/uuid"
)

func TestPickWithinCapsKeepsFairOrderAndEnforcesCaps(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	run := func() uuid.UUID { return uuid.New() }
	// Same shape dueCandidates hands us: a, b, a, b, a, b.
	candidates := []candidate{{run(), a}, {run(), b}, {run(), a}, {run(), b}, {run(), a}, {run(), b}}
	free := map[uuid.UUID]int{a: 1, b: 3}

	ids, throttled := pickWithinCaps(candidates, free)

	want := []uuid.UUID{candidates[0].id, candidates[1].id, candidates[3].id, candidates[5].id}
	if !slices.Equal(ids, want) {
		t.Fatalf("want a's first run then b's three in order, got %v", ids)
	}
	if throttled != 2 {
		t.Fatalf("want a's two extra runs held back, got %d", throttled)
	}
	if free[a] != 1 || free[b] != 3 {
		t.Fatalf("caller's free-slot map must not be modified, got %v", free)
	}
}

func TestPickWithinCapsWithNoFreeSlots(t *testing.T) {
	full, over, unknown := uuid.New(), uuid.New(), uuid.New()
	candidates := []candidate{{uuid.New(), full}, {uuid.New(), over}, {uuid.New(), unknown}}
	// over: cap was lowered while runs were in flight. unknown: no tenant row.
	ids, throttled := pickWithinCaps(candidates, map[uuid.UUID]int{full: 0, over: -4})
	if len(ids) != 0 || throttled != 3 {
		t.Fatalf("want nothing claimed and 3 held back, got %d claimed, %d held back", len(ids), throttled)
	}
}

func TestPickWithinCapsWithRoomForEveryone(t *testing.T) {
	tenant := uuid.New()
	candidates := []candidate{{uuid.New(), tenant}, {uuid.New(), tenant}}
	ids, throttled := pickWithinCaps(candidates, map[uuid.UUID]int{tenant: 100})
	if len(ids) != 2 || throttled != 0 {
		t.Fatalf("want both claimed, got %d claimed, %d held back", len(ids), throttled)
	}
}
