// Package faults implements crash points used by the failure-recovery tests.
//
// A point is enabled through FAULT_INJECTION (comma-separated names). When the
// execution reaches an enabled point the process exits immediately with code
// 137, without running deferred functions or Fx shutdown hooks — the same effect
// as a SIGKILL at that exact line. Production configuration leaves it empty.
package faults

import (
	"log/slog"
	"os"
	"sync"
)

const (
	// ConsumerAfterCommit: the SQS message was committed but not deleted.
	ConsumerAfterCommit = "consumer.after-commit"
	// OutboxAfterPublish: the event was sent to SQS but not marked published.
	OutboxAfterPublish = "outbox.after-publish"
	// OutboxAfterClaim: the event was leased but not sent.
	OutboxAfterClaim = "outbox.after-claim"
	// HTTPAfterCommit: the operation was committed but no response was sent.
	HTTPAfterCommit = "http.after-commit"
)

// Injector decides whether to crash at a point.
type Injector struct {
	points map[string]bool
	log    *slog.Logger
	exit   func(int)
	once   sync.Once
}

func New(points []string, log *slog.Logger) *Injector {
	m := map[string]bool{}
	for _, p := range points {
		if p != "" {
			m[p] = true
		}
	}
	if len(m) > 0 {
		log.Warn("fault injection enabled", slog.Any("points", points))
	}
	return &Injector{points: m, log: log, exit: os.Exit}
}

// Disabled returns an injector with no active point.
func Disabled() *Injector { return &Injector{points: map[string]bool{}, exit: os.Exit} }

// Crash terminates the process abruptly when point is enabled.
func (f *Injector) Crash(point string) {
	if f == nil || !f.points[point] {
		return
	}
	f.once.Do(func() {
		if f.log != nil {
			f.log.Error("fault injection: crashing", slog.String("point", point))
		}
		f.exit(137)
	})
}
