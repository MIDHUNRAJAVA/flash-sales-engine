package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"inventory-service/config"
	"inventory-service/models"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

const (
	OrderSubject = "orders.pending"
	DLQSubject   = "orders.dlq"
)

func Connect(config *config.Config) error {
	opts := []nats.Option{
		nats.UserInfo(config.Nats.Username, config.Nats.Password),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(250 * time.Millisecond),
	}
	nc, err := nats.Connect(config.Nats.URL, opts...)
	if err != nil {
		config.Logger.Error("NATS: Failed to connect", "error", err)
		return err
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return err
	}

	if err := ensureStreams(js); err != nil {
		nc.Close()
		return err
	}

	config.Nats.Client = nc
	config.Nats.JS = js
	config.Logger.Info("NATS: Connected, streams ensured")
	return nil
}

func ensureStreams(js nats.JetStreamContext) error {
	// ORDERS is a work queue: depth == unprocessed work, messages deleted on ack.
	// DiscardNew is load-bearing: at capacity the PUBLISH fails (-> compensation),
	// DiscardOld would silently delete an accepted order.
	streams := []*nats.StreamConfig{
		{
			Name:       "ORDERS",
			Subjects:   []string{OrderSubject},
			Retention:  nats.WorkQueuePolicy,
			Discard:    nats.DiscardNew,
			Storage:    nats.FileStorage,
			MaxMsgs:    100_000,
			MaxBytes:   64 << 20,
			MaxAge:     24 * time.Hour,
			Duplicates: 10 * time.Minute, // MsgId dedup window; must be <= idempotency TTL
			Replicas:   1,                // 3 in production (single-node NATS in compose)
		},
		{
			Name:      "ORDERS_DLQ",
			Subjects:  []string{DLQSubject},
			Retention: nats.LimitsPolicy,
			Discard:   nats.DiscardOld, // the DLQ is an archive, not a queue
			Storage:   nats.FileStorage,
			MaxAge:    168 * time.Hour,
			Replicas:  1,
		},
	}
	for _, sc := range streams {
		if _, err := js.StreamInfo(sc.Name); err == nil {
			continue
		} else if !errors.Is(err, nats.ErrStreamNotFound) {
			return err
		}
		if _, err := js.AddStream(sc); err != nil && !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			return err
		}
	}
	return nil
}

// PublishOrderEvent returns nil only after a PubAck: the stream has persisted
// the message. MsgId = idempotency hash, so retrying an ambiguous timeout is
// safe — JetStream drops duplicates inside the dedup window.
func PublishOrderEvent(ctx context.Context, config *config.Config, event models.OrderEvent, idemHash string) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	msg := &nats.Msg{Subject: OrderSubject, Data: data, Header: nats.Header{}}
	if event.RequestID != "" {
		msg.Header.Set("X-Request-ID", event.RequestID)
	}
	// Inject W3C traceparent so the Python consumer continues this trace.
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(msg.Header))

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		pubCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ack, err := config.Nats.JS.PublishMsg(msg, nats.MsgId(idemHash), nats.Context(pubCtx))
		cancel()
		if err == nil {
			config.Logger.InfoContext(ctx, "publish_acked",
				"stream", ack.Stream, "seq", ack.Sequence, "duplicate", ack.Duplicate)
			return nil
		}
		// A timeout is ambiguous — the message may have landed. Retrying with the
		// same MsgId is safe; the caller compensates only after all retries fail.
		lastErr = err
	}
	return fmt.Errorf("publish unacked after 3 attempts: %w", lastErr)
}

func Close(client *nats.Conn) {
	if client != nil {
		client.Close()
	}
}
