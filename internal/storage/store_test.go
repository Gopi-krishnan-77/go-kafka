package storage

import (
	"testing"
	"time"

	"github.com/weekend/go-kafka-fun/internal/events"
)

func sampleEvent(id, name, category string, mood int, score float64) events.WeekendEvent {
	return events.WeekendEvent{
		ID:        id,
		Name:      name,
		Category:  category,
		MoodBoost: mood,
		FunScore:  score,
		Priority:  "medium",
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(1 * time.Hour),
	}
}

func TestPutGetDelete(t *testing.T) {
	s := New()
	e := sampleEvent("1", "Hiking", "outdoors", 8, 72)
	s.Put(e)

	got, ok := s.Get("1")
	if !ok {
		t.Fatal("expected to find event")
	}
	if got.Name != "Hiking" {
		t.Fatalf("expected Hiking, got %s", got.Name)
	}

	if !s.Delete("1") {
		t.Fatal("expected delete to return true")
	}
	if _, ok := s.Get("1"); ok {
		t.Fatal("expected event to be deleted")
	}
}

func TestListPagination(t *testing.T) {
	s := New()
	for i := 0; i < 25; i++ {
		s.Put(sampleEvent(
			"e"+string(rune('A'+i)),
			"Event",
			"cat",
			5,
			float64(i),
		))
	}
	result := s.List(events.Filter{}, 2, 10)
	if result.Total != 25 {
		t.Fatalf("total: got %d, want 25", result.Total)
	}
	if len(result.Events) != 10 {
		t.Fatalf("page size: got %d, want 10", len(result.Events))
	}
}

func TestSearch(t *testing.T) {
	s := New()
	s.Put(sampleEvent("1", "Sunrise Hike", "outdoors", 9, 80))
	s.Put(sampleEvent("2", "Board Games", "social", 7, 50))

	results := s.Search("hike")
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].ID != "1" {
		t.Fatalf("expected event 1, got %s", results[0].ID)
	}
}

func TestEvictExpired(t *testing.T) {
	s := New()
	e := sampleEvent("1", "Old Event", "cat", 5, 30)
	e.ExpiresAt = time.Now().UTC().Add(-1 * time.Minute)
	s.Put(e)

	removed := s.EvictExpired()
	if removed != 1 {
		t.Fatalf("expected 1 removed, got %d", removed)
	}
	if s.Size() != 0 {
		t.Fatalf("expected empty store, got %d", s.Size())
	}
}

func TestStats(t *testing.T) {
	s := New()
	s.Put(sampleEvent("1", "A", "outdoors", 8, 85))
	s.Put(sampleEvent("2", "B", "outdoors", 6, 45))
	s.Put(sampleEvent("3", "C", "social", 7, 60))

	stats := s.Stats()
	if stats.TotalEvents != 3 {
		t.Fatalf("total: got %d, want 3", stats.TotalEvents)
	}
	if stats.ByCategory["outdoors"] != 2 {
		t.Fatalf("outdoors: got %d, want 2", stats.ByCategory["outdoors"])
	}
}
