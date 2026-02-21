// Package scheduler provides a lightweight cron-style job runner for
// periodic background tasks such as TTL eviction, metrics snapshots,
// and broker compaction.
package scheduler

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Job is a named function to run on a fixed interval.
type Job struct {
	Name     string
	Interval time.Duration
	Fn       func(ctx context.Context)
}

// Scheduler manages a set of recurring jobs.
type Scheduler struct {
	mu   sync.Mutex
	jobs []Job
	wg   sync.WaitGroup
}

// New creates a Scheduler.
func New() *Scheduler {
	return &Scheduler{}
}

// Add registers a new recurring job.
func (s *Scheduler) Add(name string, interval time.Duration, fn func(ctx context.Context)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs = append(s.jobs, Job{Name: name, Interval: interval, Fn: fn})
}

// Start launches all registered jobs. Each job runs in its own goroutine
// with a small random jitter to avoid thundering-herd on startup.
// Cancel the context to stop all jobs.
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, j := range s.jobs {
		j := j
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			// initial jitter: 0-500 ms
			jitter := time.Duration(rand.Intn(500)) * time.Millisecond
			select {
			case <-time.After(jitter):
			case <-ctx.Done():
				return
			}
			log.Printf("scheduler: started job %q interval=%s", j.Name, j.Interval)
			ticker := time.NewTicker(j.Interval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					log.Printf("scheduler: stopped job %q", j.Name)
					return
				case <-ticker.C:
					func() {
						defer func() {
							if r := recover(); r != nil {
								log.Printf("scheduler: job %q panicked: %v", j.Name, r)
							}
						}()
						j.Fn(ctx)
					}()
				}
			}
		}()
	}
}

// Wait blocks until all jobs have exited.
func (s *Scheduler) Wait() {
	s.wg.Wait()
}

// JobNames returns the list of registered job names.
func (s *Scheduler) JobNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, len(s.jobs))
	for i, j := range s.jobs {
		names[i] = j.Name
	}
	return names
}
