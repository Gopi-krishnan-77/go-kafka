// Package storage provides a thread-safe, indexed in-memory store with
// TTL-based eviction, pagination, full-text search, and aggregated stats.
package storage

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/weekend/go-kafka-fun/internal/events"
)

// Store is the primary data repository for processed events.
type Store struct {
	mu      sync.RWMutex
	primary map[string]events.WeekendEvent // id → event

	// Secondary indices (values are sets of event IDs).
	byCategory map[string]map[string]struct{}
	byPriority map[string]map[string]struct{}
}

// New creates an empty Store.
func New() *Store {
	return &Store{
		primary:    make(map[string]events.WeekendEvent),
		byCategory: make(map[string]map[string]struct{}),
		byPriority: make(map[string]map[string]struct{}),
	}
}

// Put inserts or updates an event in the store.
func (s *Store) Put(e events.WeekendEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Remove old index entries if updating.
	if old, ok := s.primary[e.ID]; ok {
		s.removeIndex(old)
	}

	s.primary[e.ID] = e
	s.addIndex(e)
}

// Get returns an event by ID.
func (s *Store) Get(id string) (events.WeekendEvent, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.primary[id]
	return e, ok
}

// Delete removes an event by ID and returns true if it existed.
func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.primary[id]
	if !ok {
		return false
	}
	s.removeIndex(e)
	delete(s.primary, id)
	return true
}

// ListResult contains a page of events plus total matching count.
type ListResult struct {
	Events []events.WeekendEvent `json:"events"`
	Total  int                   `json:"total"`
	Page   int                   `json:"page"`
	Limit  int                   `json:"limit"`
}

// List returns a paginated, filtered, sorted list of events.
func (s *Store) List(f events.Filter, page, limit int) ListResult {
	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 100 {
		limit = 20
	}

	s.mu.RLock()
	// Start from an index if possible?
	var candidates []events.WeekendEvent
	if f.Category != "" {
		ids := s.byCategory[strings.ToLower(f.Category)]
		for id := range ids {
			e := s.primary[id]
			if e.MatchesFilter(f) {
				candidates = append(candidates, e)
			}
		}
	} else {
		for _, e := range s.primary {
			if e.MatchesFilter(f) {
				candidates = append(candidates, e)
			}
		}
	}
	s.mu.RUnlock()

	// Sort by created_at descending.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].CreatedAt.After(candidates[j].CreatedAt)
	})

	total := len(candidates)
	start := (page - 1) * limit
	if start >= total {
		return ListResult{Events: []events.WeekendEvent{}, Total: total, Page: page, Limit: limit}
	}
	end := start + limit
	if end > total {
		end = total
	}

	return ListResult{
		Events: candidates[start:end],
		Total:  total,
		Page:   page,
		Limit:  limit,
	}
}

// Search performs a substring search across event names and categories.
func (s *Store) Search(query string) []events.WeekendEvent {
	q := strings.ToLower(query)
	s.mu.RLock()
	defer s.mu.RUnlock()
	var results []events.WeekendEvent
	for _, e := range s.primary {
		if strings.Contains(strings.ToLower(e.Name), q) ||
			strings.Contains(strings.ToLower(e.Category), q) {
			results = append(results, e)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].FunScore > results[j].FunScore
	})
	return results
}

// EvictExpired removes events whose ExpiresAt is before now. Returns count removed.
func (s *Store) EvictExpired() int {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for id, e := range s.primary {
		if !e.ExpiresAt.IsZero() && e.ExpiresAt.Before(now) {
			s.removeIndex(e)
			delete(s.primary, id)
			removed++
		}
	}
	return removed
}

// AggregatedStats holds summary statistics.
type AggregatedStats struct {
	TotalEvents     int                `json:"total_events"`
	ByCategory      map[string]int     `json:"by_category"`
	ByPriority      map[string]int     `json:"by_priority"`
	AvgFunScore     float64            `json:"avg_fun_score"`
	TopCategories   []CategoryCount    `json:"top_categories"`
	ScoreDistrib    map[string]int     `json:"score_distribution"`
}

// CategoryCount pairs a category name with its count.
type CategoryCount struct {
	Category string `json:"category"`
	Count    int    `json:"count"`
}

// Stats computes aggregated statistics across all stored events.
func (s *Store) Stats() AggregatedStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	st := AggregatedStats{
		TotalEvents:  len(s.primary),
		ByCategory:   make(map[string]int),
		ByPriority:   make(map[string]int),
		ScoreDistrib: make(map[string]int),
	}

	var totalScore float64
	for _, e := range s.primary {
		st.ByCategory[e.Category]++
		st.ByPriority[e.Priority]++
		totalScore += e.FunScore

		switch {
		case e.FunScore >= 80:
			st.ScoreDistrib["80-100"]++
		case e.FunScore >= 55:
			st.ScoreDistrib["55-79"]++
		case e.FunScore >= 30:
			st.ScoreDistrib["30-54"]++
		default:
			st.ScoreDistrib["0-29"]++
		}
	}
	if st.TotalEvents > 0 {
		st.AvgFunScore = totalScore / float64(st.TotalEvents)
	}

	// Top categories
	cats := make([]CategoryCount, 0, len(st.ByCategory))
	for c, n := range st.ByCategory {
		cats = append(cats, CategoryCount{Category: c, Count: n})
	}
	sort.Slice(cats, func(i, j int) bool { return cats[i].Count > cats[j].Count })
	if len(cats) > 5 {
		cats = cats[:5]
	}
	st.TopCategories = cats
	return st
}

// Size returns the number of events in the store.
func (s *Store) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.primary)
}

// --- index helpers ---

func (s *Store) addIndex(e events.WeekendEvent) {
	cat := strings.ToLower(e.Category)
	if s.byCategory[cat] == nil {
		s.byCategory[cat] = make(map[string]struct{})
	}
	s.byCategory[cat][e.ID] = struct{}{}

	pri := strings.ToLower(e.Priority)
	if pri != "" {
		if s.byPriority[pri] == nil {
			s.byPriority[pri] = make(map[string]struct{})
		}
		s.byPriority[pri][e.ID] = struct{}{}
	}
}

func (s *Store) removeIndex(e events.WeekendEvent) {
	cat := strings.ToLower(e.Category)
	delete(s.byCategory[cat], e.ID)

	pri := strings.ToLower(e.Priority)
	if pri != "" {
		delete(s.byPriority[pri], e.ID)
	}
}
