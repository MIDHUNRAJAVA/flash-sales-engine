package logging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// Handler wraps an inner slog.Handler and lifts the active span's trace_id /
// span_id onto every record, so a log line joins the trace it was emitted in.
type Handler struct {
	inner slog.Handler
}

func New(inner slog.Handler) *Handler {
	return &Handler{inner: inner}
}

func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &Handler{inner: h.inner.WithAttrs(attrs)}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	return &Handler{inner: h.inner.WithGroup(name)}
}

// HashUserID returns a truncated sha256 hex digest of a user ID. Call sites
// must use this instead of logging req.UserID directly — raw user IDs must
// never reach the log sink (Vector re-applies this scrub in-pipeline as
// defense in depth, but the source has to stop leaking it first).
func HashUserID(userID string) string {
	sum := sha256.Sum256([]byte(userID))
	return hex.EncodeToString(sum[:8])
}
