package actions

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"inventory-service/breaker"
	"inventory-service/config"
	"inventory-service/datastore/nats"
	"inventory-service/datastore/redis"
	"inventory-service/logging"
	"inventory-service/metrics"
	"inventory-service/models"
	"sync"
	"time"

	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// activeProducts tracks sales this instance has touched, for the stock
// poller and the expiry reaper.
var activeProducts sync.Map

var tracer = otel.Tracer("inventory-service/actions")

type BuyOutcome struct {
	Code      int64
	OrderID   string
	Remaining int64
	Duplicate bool
}

// idempotencyHash binds the attempt to the authenticated user and product; a
// nonce reused across users cannot collide into someone else's order.
func idempotencyHash(req models.BuyRequest) string {
	nonce := req.ClientNonce
	if nonce == "" {
		// No nonce = no retry-safety for this attempt; each call is distinct.
		b := make([]byte, 16)
		_, _ = rand.Read(b)
		nonce = hex.EncodeToString(b)
	}
	sum := sha256.Sum256([]byte(req.UserID + "|" + req.ProductID + "|" + nonce))
	return hex.EncodeToString(sum[:])
}

func ProcessBuyRequest(ctx context.Context, cfg *config.Config, req models.BuyRequest, requestID string) (*BuyOutcome, error) {
	// Bound the whole decision (Redis leg + NATS publish incl. its 3x2s retry
	// budget) so a hung dependency — e.g. the Redis pool exhausted under a
	// stampede (PoolTimeout=30s) — fails the request instead of hanging the
	// connection well past the buy_latency SLO.
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	activeProducts.Store(req.ProductID, true)

	idemHash := idempotencyHash(req)
	orderID := fmt.Sprintf("ord-%d-%s", time.Now().UnixNano(), req.UserID)

	// Gate: if the publish breaker is open, don't reserve stock at all —
	// reserving then failing to publish only creates compensation churn.
	if breaker.NatsPublish.State() == gobreaker.StateOpen {
		cfg.Logger.WarnContext(ctx, "publish_attempted", "order_id", orderID,
			"breaker", "nats-publish", "state", "open", "request_id", requestID)
		return nil, fmt.Errorf("nats publish breaker open, refusing to reserve")
	}

	cfg.Logger.InfoContext(ctx, "stock_check_attempted", "user_id", logging.HashUserID(req.UserID),
		"product_id", req.ProductID, "quantity_requested", req.Quantity, "request_id", requestID)

	start := time.Now()
	rctx, span := tracer.Start(ctx, "redis.stock_decrement")
	resAny, err := breaker.RedisStock.Execute(func() (any, error) {
		return redis.ReserveStock(rctx, cfg.Redis.Client, req.ProductID, req.UserID, idemHash, orderID,
			req.Quantity, cfg.Sale.MaxPerUser,
			time.Duration(cfg.Sale.IdemTTLSec)*time.Second,
			time.Duration(cfg.Sale.ResvTTLSec)*time.Second,
			time.Duration(cfg.Sale.QuotaTTLSec)*time.Second)
	})
	metrics.StockDecrementDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		// Redis error OR breaker open/too-many-requests: both map to 503.
		span.SetStatus(codes.Error, err.Error())
		span.End()
		metrics.StockOps.WithLabelValues("error").Inc()
		cfg.Logger.ErrorContext(ctx, "stock_check_attempted", "error", err, "request_id", requestID)
		return nil, fmt.Errorf("failed to reserve stock: %w", err)
	}
	res := resAny.(*redis.BuyResult)
	span.SetAttributes(
		attribute.Int64("lua_return_code", res.Code),
		attribute.Int64("remaining_stock", res.Remaining),
		attribute.Int("quantity", req.Quantity),
		attribute.Bool("idempotency_hit", res.Code == redis.CodeDuplicate),
	)
	// out_of_stock / quota / invalid / closed are legitimate business outcomes,
	// not span errors. Only the -99 internal script fault is genuinely broken.
	if res.Code == redis.CodeInternalErr {
		span.SetStatus(codes.Error, "buy script internal error")
	}
	span.End()

	switch res.Code {
	case redis.CodeSuccess:
		metrics.StockOps.WithLabelValues("decremented").Inc()
	case redis.CodeDuplicate:
		metrics.StockOps.WithLabelValues("duplicate").Inc()
		cfg.Logger.InfoContext(ctx, "duplicate_detected",
			"order_id", res.OrderID, "request_id", requestID)
		return &BuyOutcome{Code: res.Code, OrderID: res.OrderID, Duplicate: true}, nil
	case redis.CodeOutOfStock:
		metrics.StockOps.WithLabelValues("out_of_stock").Inc()
		cfg.Logger.InfoContext(ctx, "stock_out", "product_id", req.ProductID,
			"quantity_requested", req.Quantity, "request_id", requestID)
		return &BuyOutcome{Code: res.Code}, nil
	case redis.CodeQuota:
		metrics.StockOps.WithLabelValues("quota").Inc()
		return &BuyOutcome{Code: res.Code}, nil
	case redis.CodeInvalidQty:
		metrics.StockOps.WithLabelValues("invalid").Inc()
		return &BuyOutcome{Code: res.Code}, nil
	case redis.CodeSaleClosed:
		metrics.StockOps.WithLabelValues("closed").Inc()
		return &BuyOutcome{Code: res.Code}, nil
	default:
		metrics.StockOps.WithLabelValues("error").Inc()
		return nil, fmt.Errorf("buy script internal error (code %d)", res.Code)
	}

	cfg.Logger.InfoContext(ctx, "stock_decremented", "order_id", orderID,
		"remaining_stock", res.Remaining, "lua_return_code", res.Code,
		"quantity_requested", req.Quantity, "request_id", requestID)
	metrics.StockRemaining.WithLabelValues(req.ProductID).Set(float64(res.Remaining))

	event := models.OrderEvent{
		OrderID:   orderID,
		UserID:    req.UserID,
		ProductID: req.ProductID,
		Quantity:  req.Quantity,
		Status:    "PENDING",
		Timestamp: time.Now().Unix(),
		RequestID: requestID,
	}

	cfg.Logger.InfoContext(ctx, "publish_attempted", "order_id", orderID,
		"messaging.destination", nats.OrderSubject, "request_id", requestID)
	pctx, pspan := tracer.Start(ctx, "nats.publish")
	pspan.SetAttributes(attribute.String("messaging.destination", nats.OrderSubject))
	_, pubErr := breaker.NatsPublish.Execute(func() (any, error) {
		return nil, nats.PublishOrderEvent(pctx, cfg, event, idemHash)
	})
	pspan.SetAttributes(attribute.Bool("pub_ack_received", pubErr == nil))
	if pubErr != nil {
		pspan.SetStatus(codes.Error, pubErr.Error())
	}
	pspan.End()
	if pubErr != nil {
		metrics.PublishOps.WithLabelValues("failed").Inc()
		cfg.Logger.ErrorContext(ctx, "publish_failed", "order_id", orderID, "error", pubErr)
		compensate(ctx, cfg, req.ProductID, orderID)
		return nil, fmt.Errorf("order not accepted, stock restored: %w", pubErr)
	}
	metrics.PublishOps.WithLabelValues("success").Inc()
	cfg.Logger.InfoContext(ctx, "publish_succeeded", "order_id", orderID, "request_id", requestID)

	return &BuyOutcome{Code: res.Code, OrderID: orderID, Remaining: res.Remaining}, nil
}

// compensate rolls the reservation back atomically (stock, quota, idempotency
// key, state -> CANCELLED). If the publish timeout was ambiguous and the
// message actually landed, the consumer's confirm CAS finds the tombstone and
// drops it — no oversell either way.
func compensate(ctx context.Context, cfg *config.Config, productID, orderID string) {
	// Compensation must outlive a disconnected client, so it runs on a fresh
	// background deadline — but carry the trace's span context so its logs still
	// join the request's trace.
	ctx = trace.ContextWithSpanContext(context.Background(), trace.SpanContextFromContext(ctx))
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	cfg.Logger.InfoContext(ctx, "compensation_triggered", "order_id", orderID)
	for i, backoff := range []time.Duration{0, 10 * time.Millisecond, 50 * time.Millisecond, 250 * time.Millisecond} {
		if i > 0 {
			time.Sleep(backoff)
		}
		done, err := redis.CancelReservation(ctx, cfg.Redis.Client, productID, orderID, "CANCELLED")
		if err == nil {
			if done {
				metrics.CompensationOps.WithLabelValues("success").Inc()
				cfg.Logger.InfoContext(ctx, "compensation_succeeded", "order_id", orderID)
			} else {
				metrics.CompensationOps.WithLabelValues("noop").Inc()
			}
			return
		}
		cfg.Logger.WarnContext(ctx, "compensation_failed", "order_id", orderID, "attempt", i+1, "error", err)
	}
	// Redis AND NATS both failing: this counter firing is a P1 page.
	metrics.CompensationOps.WithLabelValues("failed").Inc()
	cfg.Logger.ErrorContext(ctx, "compensation_failed", "order_id", orderID, "reason", "exhausted retries, stock leaked")
}

func SeedStock(ctx context.Context, cfg *config.Config, productID string, quantity int) error {
	cfg.Logger.Info("Seeding stock", "product_id", productID, "quantity", quantity)
	if err := redis.SeedStock(ctx, cfg.Redis.Client, productID, quantity); err != nil {
		cfg.Logger.Error("Redis error", "error", err)
		return err
	}
	activeProducts.Store(productID, true)
	metrics.StockRemaining.WithLabelValues(productID).Set(float64(quantity))
	return nil
}

// RunReaper returns expired reservations to stock every 5s. The cancel
// script's CAS means a reservation confirming concurrently is never
// double-returned.
func RunReaper(ctx context.Context, cfg *config.Config) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// A panic in one tick must not kill this goroutine forever — the reaper
			// is the only thing returning dead reservations to stock, so silently
			// losing it would leak stock for the rest of the sale.
			func() {
				defer func() {
					if r := recover(); r != nil {
						cfg.Logger.Error("Reaper: recovered from panic", "panic", r)
					}
				}()
				activeProducts.Range(func(key, _ any) bool {
					productID := key.(string)
					due, err := redis.DueReservations(ctx, cfg.Redis.Client, productID, 100)
					if err != nil {
						cfg.Logger.Error("Reaper: failed to list due reservations", "error", err)
						return true
					}
					for _, orderID := range due {
						done, err := redis.CancelReservation(ctx, cfg.Redis.Client, productID, orderID, "EXPIRED")
						if err != nil {
							cfg.Logger.Error("Reaper: cancel failed", "order_id", orderID, "error", err)
							continue
						}
						if done {
							metrics.ReservationsExpired.Inc()
							cfg.Logger.Warn("Reservation expired, stock returned", "order_id", orderID)
						} else {
							// Lost the CAS race (confirmed meanwhile) but still queued: drop the deadline entry.
							cfg.Redis.Client.ZRem(ctx, redis.DeadlinesKey(productID), orderID)
						}
					}
					return true
				})
			}()
		}
	}
}

// RunStockPoller refreshes the stock_remaining gauge from Redis truth every 5s.
func RunStockPoller(ctx context.Context, cfg *config.Config) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				defer func() {
					if r := recover(); r != nil {
						cfg.Logger.Error("StockPoller: recovered from panic", "panic", r)
					}
				}()
				activeProducts.Range(func(key, _ any) bool {
					productID := key.(string)
					if stock, err := redis.GetStock(ctx, cfg.Redis.Client, productID); err == nil {
						metrics.StockRemaining.WithLabelValues(productID).Set(float64(stock))
					} else {
						cfg.Logger.Warn("StockPoller: failed to read stock", "product_id", productID, "error", err)
					}
					return true
				})
			}()
		}
	}
}
