package events

import (
	"encoding/json"
	"time"
)

// WeekendEvent describes one activity planned for the weekend.
type WeekendEvent struct {
	Name      string    `json:"name"`
	Category  string    `json:"category"`
	MoodBoost int       `json:"mood_boost"`
	CreatedAt time.Time `json:"created_at"`
}

func (e WeekendEvent) Key() []byte {
	return []byte(e.Category)
}

func (e WeekendEvent) Bytes() ([]byte, error) {
	return json.Marshal(e)
}
