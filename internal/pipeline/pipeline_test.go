package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/weekend/go-kafka-fun/internal/events"
	"github.com/weekend/go-kafka-fun/internal/storage"
)

func newTestEvent() *events.WeekendEvent {
	return &events.WeekendEvent{
		Name:      "Sunrise Hike",
		Category:  "outdoors",
		MoodBoost: 9,
		Tags:      []string{"nature", "exercise"},
		CreatedAt: time.Date(2025, 7, 5, 8, 0, 0, 0, time.UTC), // Saturday in summer
	}
}

func TestValidateStage(t *testing.T) {
	s := ValidateStage{}
	e := newTestEvent()
	if err := s.Process(context.Background(), e); err != nil {
		t.Fatalf("should pass: %v", err)
	}

	bad := &events.WeekendEvent{Name: "", Category: "x", MoodBoost: 5}
	if err := s.Process(context.Background(), bad); err == nil {
		t.Fatal("expected validation error for empty name")
	}
}

func TestEnrichStage(t *testing.T) {
	s := EnrichStage{TTL: 24 * time.Hour}
	e := newTestEvent()
	if err := s.Process(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if e.ID == "" {
		t.Fatal("ID should be set")
	}
	if e.DayOfWeek != "Saturday" {
		t.Fatalf("expected Saturday, got %s", e.DayOfWeek)
	}
	if e.Season != "summer" {
		t.Fatalf("expected summer, got %s", e.Season)
	}
	if e.ExpiresAt.IsZero() {
		t.Fatal("ExpiresAt should be set")
	}
}

func TestScoreStage(t *testing.T) {
	s := ScoreStage{}
	e := newTestEvent()
	e.DayOfWeek = e.CreatedAt.Weekday().String()
	if err := s.Process(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if e.FunScore <= 0 {
		t.Fatalf("expected positive score, got %f", e.FunScore)
	}
	// High mood (9) + outdoors multiplier (1.5) + weekend bonus → should be high
	if e.FunScore < 50 {
		t.Fatalf("expected score >= 50 for a great outdoor weekend event, got %f", e.FunScore)
	}
}

func TestClassifyStage(t *testing.T) {
	s := ClassifyStage{}
	e := &events.WeekendEvent{FunScore: 85}
	if err := s.Process(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if e.Priority != events.PriorityCritical {
		t.Fatalf("expected critical, got %s", e.Priority)
	}

	e.FunScore = 20
	_ = s.Process(context.Background(), e)
	if e.Priority != events.PriorityLow {
		t.Fatalf("expected low, got %s", e.Priority)
	}
}

func TestFullPipeline(t *testing.T) {
	store := storage.New()
	p := New(
		5*time.Second,
		ValidateStage{},
		EnrichStage{TTL: 1 * time.Hour},
		ScoreStage{},
		ClassifyStage{},
		PersistStage{Store: store},
	)

	e := newTestEvent()
	if err := p.Run(context.Background(), e); err != nil {
		t.Fatalf("pipeline failed: %v", err)
	}

	// Verify it was persisted.
	got, ok := store.Get(e.ID)
	if !ok {
		t.Fatal("event not found in store after pipeline")
	}
	if got.FunScore <= 0 {
		t.Fatal("fun score not computed")
	}
	if got.Priority == "" {
		t.Fatal("priority not classified")
	}
}
