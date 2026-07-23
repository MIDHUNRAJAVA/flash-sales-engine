package breaker

import (
	"inventory-service/metrics"
	"log/slog"
	"time"

	"github.com/sony/gobreaker"
)

// RedisStock guards the Lua reserve call; NatsPublish guards the JetStream
// publish. Both trip on >=20 requests in the 10s window with >=50% failures,
// then let 3 probes through after a 5s cooldown.
var (
	RedisStock  = newBreaker("redis-stock")
	NatsPublish = newBreaker("nats-publish")
)

func newBreaker(name string) *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        name,
		MaxRequests: 3,
		Interval:    10 * time.Second,
		Timeout:     5 * time.Second,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.Requests >= 20 && float64(c.TotalFailures)/float64(c.Requests) >= 0.5
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			slog.Warn("circuit_state_change", "breaker", name, "from", from.String(), "to", to.String())
			metrics.CircuitBreakerState.WithLabelValues(name).Set(float64(to))
		},
	})
}
