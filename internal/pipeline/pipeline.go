// Package pipeline implements a multi-stage, configurable event processing
// pipeline with per-stage timeouts and error handling.
package pipeline

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/weekend/go-kafka-fun/internal/events"
	"github.com/weekend/go-kafka-fun/internal/storage"
)

// Stage is a single processing step in the pipeline.
type Stage interface {
	Name() string
	Process(ctx context.Context, e *events.WeekendEvent) error
}

// Pipeline chains multiple Stages together and executes them in order.
type Pipeline struct {
	stages       []Stage
	stageTimeout time.Duration
}

// New creates a Pipeline with sensible defaults.
func New(stageTimeout time.Duration, stages ...Stage) *Pipeline {
	if stageTimeout <= 0 {
		stageTimeout = 5 * time.Second
	}
	return &Pipeline{stages: stages, stageTimeout: stageTimeout}
}

// Run executes every stage in order. Returns on first error.
func (p *Pipeline) Run(ctx context.Context, e *events.WeekendEvent) error {
	for _, s := range p.stages {
		sCtx, cancel := context.WithTimeout(ctx, p.stageTimeout)
		err := s.Process(sCtx, e)
		cancel()
		if err != nil {
			return fmt.Errorf("stage %s: %w", s.Name(), err)
		}
	}
	return nil
}

// StageNames returns an ordered list of stage names.
func (p *Pipeline) StageNames() []string {
	names := make([]string, len(p.stages))
	for i, s := range p.stages {
		names[i] = s.Name()
	}
	return names
}

// ──────────────────────────────────────────────
// Built-in stages
// ──────────────────────────────────────────────

// ValidateStage checks that core fields are present and within range.
type ValidateStage struct{}

func (ValidateStage) Name() string { return "validate" }
func (ValidateStage) Process(_ context.Context, e *events.WeekendEvent) error {
	return e.Validate()
}

// EnrichStage populates derived fields: ID, timestamps, day-of-week, season.
type EnrichStage struct {
	TTL time.Duration
}

func (EnrichStage) Name() string { return "enrich" }
func (s EnrichStage) Process(_ context.Context, e *events.WeekendEvent) error {
	if e.ID == "" {
		e.ID = uuid.New().String()
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now().UTC()
	}
	e.Enrich()
	e.ProcessedAt = time.Now().UTC()
	if s.TTL > 0 {
		e.ExpiresAt = e.CreatedAt.Add(s.TTL)
	}
	return nil
}

// ScoreStage computes a composite FunScore from multiple weighted factors.
//
// Factors:
//   - mood_boost (0-10)  × 4.0   → 0-40
//   - name length bonus           → 0-20  (log scale)
//   - category multiplier         → ×1.0-×1.5
//   - weekend bonus               → +10 if Sat/Sun
//   - tag bonus                   → +2 per tag (max +10)
//
// Final score is clamped to [0, 100].
type ScoreStage struct{}

func (ScoreStage) Name() string { return "score" }
func (ScoreStage) Process(_ context.Context, e *events.WeekendEvent) error {
	base := float64(e.MoodBoost) * 4.0

	nameLen := float64(len(e.Name))
	nameBonus := math.Min(20, math.Log2(nameLen+1)*5)

	catMul := categoryMultiplier(e.Category)

	weekendBonus := 0.0
	dow := e.CreatedAt.Weekday()
	if dow == time.Saturday || dow == time.Sunday {
		weekendBonus = 10
	}

	tagBonus := math.Min(10, float64(len(e.Tags))*2)

	score := (base+nameBonus)*catMul + weekendBonus + tagBonus
	e.FunScore = math.Round(math.Min(100, math.Max(0, score))*100) / 100
	return nil
}

func categoryMultiplier(cat string) float64 {
	switch strings.ToLower(cat) {
	case "outdoors", "adventure":
		return 1.5
	case "social", "games":
		return 1.3
	case "creative", "music":
		return 1.2
	case "food", "cooking":
		return 1.1
	default:
		return 1.0
	}
}

// ClassifyStage assigns a priority tier based on the computed fun score.
type ClassifyStage struct{}

func (ClassifyStage) Name() string { return "classify" }
func (ClassifyStage) Process(_ context.Context, e *events.WeekendEvent) error {
	e.ClassifyPriority()
	return nil
}

// PersistStage writes the fully enriched event to the storage engine.
type PersistStage struct {
	Store *storage.Store
}

func (PersistStage) Name() string { return "persist" }
func (s PersistStage) Process(_ context.Context, e *events.WeekendEvent) error {
	s.Store.Put(*e)
	return nil
}
