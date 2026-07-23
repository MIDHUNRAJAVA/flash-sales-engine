package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Label cardinality rule: only bounded values (result, product_id) — never
// user_id or order_id, which mint one series per value.

var (
	StockRemaining = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "stock_remaining",
		Help: "Current stock in Redis, polled every 5s. The war metric.",
	}, []string{"product_id"})

	StockOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stock_operations_total",
		Help: "Buy script outcomes.",
	}, []string{"result"}) // decremented|out_of_stock|duplicate|quota|invalid|closed|error

	StockDecrementDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "stock_decrement_duration_seconds",
		Help:    "Lua buy script round-trip time.",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1},
	})

	PublishOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "publish_operations_total",
		Help: "JetStream publish outcomes (acked or not).",
	}, []string{"result"}) // success|failed

	CompensationOps = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "compensation_operations_total",
		Help: "Stock rollback outcomes after failed publishes. failed>0 is a P1.",
	}, []string{"result"}) // success|failed|noop

	ReservationsExpired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "reservations_expired_total",
		Help: "Reservations returned to stock by the expiry reaper.",
	})

	CircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "circuit_breaker_state",
		Help: "Breaker state: 0=closed, 1=half-open, 2=open (gobreaker.State values).",
	}, []string{"breaker"})
)
