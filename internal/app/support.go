package app

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
)

// SystemClock is the production Clock.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// UUIDv7 is the production IDGenerator.
type UUIDv7 struct{}

func (UUIDv7) New() uuid.UUID { return uuid.Must(uuid.NewV7()) }

// ExponentialSchedule is the retry policy of pending references:
// next = now + min(Base * 2^attempts, Max), expiring after MaxAttempts retries or
// once TTL elapsed since the transaction was created.
type ExponentialSchedule struct {
	Base        time.Duration
	Max         time.Duration
	MaxAttempts int
	TTL         time.Duration
}

func (s ExponentialSchedule) Next(attempts int, createdAt, now time.Time) (time.Time, bool) {
	if attempts >= s.MaxAttempts || (s.TTL > 0 && now.Sub(createdAt) >= s.TTL) {
		return time.Time{}, true
	}
	return now.Add(Backoff(s.Base, s.Max, attempts)), false
}

// Backoff returns min(base * 2^attempt, max).
func Backoff(base, max time.Duration, attempt int) time.Duration {
	d := base
	for i := 0; i < attempt && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}

// retryConflicts runs fn again when it fails with ErrConcurrencyConflict, with a
// small jittered pause, at most `attempts` times.
func retryConflicts(ctx context.Context, attempts int, onRetry func(), fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil || !errors.Is(err, ErrConcurrencyConflict) {
			return err
		}
		onRetry()
		pause := time.Duration(5+rand.IntN(20*(i+1))) * time.Millisecond
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(pause):
		}
	}
	return err
}
