// Package events defines the enriched event model used throughout the system.
package events

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Priority levels for processed events.
const (
	PriorityCritical = "critical"
	PriorityHigh     = "high"
	PriorityMedium   = "medium"
	PriorityLow      = "low"
)

// WeekendEvent describes an activity submitted by the user and enriched
// through the processing pipeline.
type WeekendEvent struct {
	// Core fields (set by the user).
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Category  string   `json:"category"`
	MoodBoost int      `json:"mood_boost"`
	Tags      []string `json:"tags,omitempty"`

	// Derived fields (set by the pipeline).
	FunScore    float64   `json:"fun_score"`
	Priority    string    `json:"priority,omitempty"`
	DayOfWeek   string    `json:"day_of_week,omitempty"`
	Season      string    `json:"season,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	ProcessedAt time.Time `json:"processed_at,omitempty"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
}

// Key returns a partition key derived from the event category.
func (e WeekendEvent) Key() []byte {
	return []byte(e.Category)
}

// Bytes serialises the event to JSON.
func (e WeekendEvent) Bytes() ([]byte, error) {
	return json.Marshal(e)
}

// Validate checks that the event's core fields are sane.
func (e WeekendEvent) Validate() error {
	if strings.TrimSpace(e.Name) == "" {
		return errors.New("name is required")
	}
	if strings.TrimSpace(e.Category) == "" {
		return errors.New("category is required")
	}
	if e.MoodBoost < 1 || e.MoodBoost > 10 {
		return errors.New("mood_boost must be between 1 and 10")
	}
	return nil
}

// Enrich populates calculated fields based on CreatedAt and name length.
func (e *WeekendEvent) Enrich() {
	e.DayOfWeek = e.CreatedAt.Weekday().String()
	e.Season = seasonFor(e.CreatedAt)
}

// ClassifyPriority assigns a priority tier based on the fun score.
func (e *WeekendEvent) ClassifyPriority() {
	switch {
	case e.FunScore >= 80:
		e.Priority = PriorityCritical
	case e.FunScore >= 55:
		e.Priority = PriorityHigh
	case e.FunScore >= 30:
		e.Priority = PriorityMedium
	default:
		e.Priority = PriorityLow
	}
}

// MatchesFilter returns true if the event matches the given filter criteria.
// Empty filter fields are treated as wildcards.
type Filter struct {
	Category string
	Priority string
	Query    string // substring match on name
}

// MatchesFilter returns true when the event passes all non-empty filter conditions.
func (e WeekendEvent) MatchesFilter(f Filter) bool {
	if f.Category != "" && !strings.EqualFold(e.Category, f.Category) {
		return false
	}
	if f.Priority != "" && !strings.EqualFold(e.Priority, f.Priority) {
		return false
	}
	if f.Query != "" && !strings.Contains(strings.ToLower(e.Name), strings.ToLower(f.Query)) {
		return false
	}
	return true
}

// seasonFor returns the meteorological season for a given time.
func seasonFor(t time.Time) string {
	switch t.Month() {
	case time.December, time.January, time.February:
		return "winter"
	case time.March, time.April, time.May:
		return "spring"
	case time.June, time.July, time.August:
		return "summer"
	default:
		return "autumn"
	}
}
