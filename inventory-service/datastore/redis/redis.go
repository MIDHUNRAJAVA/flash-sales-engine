package redis

import (
	"context"
	"fmt"
	"inventory-service/config"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

func Connect(config *config.Config) error {
	rdb := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", config.Redis.Host, config.Redis.Port),
		PoolSize:     100, // Increased for 5000 VUs
		MinIdleConns: 50,  // Keep 50 connections warm
		PoolTimeout:  30 * time.Second,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		config.Logger.Error("Redis: Failed to ping", "error", err)
		return err
	}

	// Emit a child span per command, continuing the request trace.
	if err := redisotel.InstrumentTracing(rdb); err != nil {
		config.Logger.Warn("Redis: tracing instrumentation failed", "error", err)
	}

	config.Redis.Client = rdb
	config.Logger.Info("Redis: Connected successfully")
	return nil
}

// All sale state shares one {sale:<product>} hash tag: every key lands on the
// same slot, so the scripts below stay valid if this ever moves to Redis Cluster.
func salePrefix(productID string) string { return fmt.Sprintf("{sale:%s}", productID) }

func StockKey(productID string) string     { return salePrefix(productID) + ":stock" }
func StateKey(productID string) string     { return salePrefix(productID) + ":state" }
func DeadlinesKey(productID string) string { return salePrefix(productID) + ":resv_deadlines" }
func IdemKey(productID, idemHash string) string {
	return salePrefix(productID) + ":idem:" + idemHash
}
func UserKey(productID, userID string) string {
	return salePrefix(productID) + ":user:" + userID + ":purchased"
}
func ResvKey(productID, orderID string) string {
	return salePrefix(productID) + ":resv:" + orderID
}

// Buy return codes (shared contract with the HTTP handler).
const (
	CodeSuccess     = 1
	CodeOutOfStock  = 0
	CodeDuplicate   = -1
	CodeQuota       = -2
	CodeInvalidQty  = -3
	CodeSaleClosed  = -4
	CodeInternalErr = -99
)

// One script = one serialized decision: idempotency, gate, quota and stock are
// checked and mutated under a single snapshot — no interleaving can oversell.
// The reservation value encodes STATE|qty|user|idemHash so cancel/expire can
// roll back quota and idempotency without a second lookup.
var buyScript = redis.NewScript(`
local qty = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
if not qty or qty < 1 or qty % 1 ~= 0 or qty > max then return {-3} end

if redis.call('GET', KEYS[4]) ~= 'OPEN' then return {-4} end

local cached = redis.call('GET', KEYS[2])
if cached then return {-1, cached} end

local bought = tonumber(redis.call('GET', KEYS[3]) or '0')
if bought + qty > max then return {-2} end

local stock = tonumber(redis.call('GET', KEYS[1]))
if stock == nil then return {-99} end
if stock < qty then return {0} end

redis.call('DECRBY', KEYS[1], qty)
redis.call('INCRBY', KEYS[3], qty)
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[6]))
redis.call('SET', KEYS[2], ARGV[4], 'EX', tonumber(ARGV[3]))
redis.call('SET', KEYS[5], 'RESERVED|' .. qty .. '|' .. ARGV[7] .. '|' .. ARGV[8], 'EX', 86400)
local t = redis.call('TIME')
redis.call('ZADD', KEYS[6], t[1] + tonumber(ARGV[5]), ARGV[4])
return {1, stock - qty, ARGV[4]}
`)

// CAS out of RESERVED: exactly one of confirm / cancel / expire wins, the rest
// become no-ops. Compensating on an ambiguous publish timeout is safe because
// a late-arriving message finds the CANCELLED tombstone and is dropped.
// The user/idem keys are rebuilt from the stored value — same hash tag, same
// slot, so this stays cluster-safe at runtime.
var cancelScript = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if not v then return 0 end
local state, qty, user, idem = string.match(v, '^(%u+)|(%d+)|([^|]*)|([^|]*)$')
if state ~= 'RESERVED' then return 0 end
redis.call('SET', KEYS[1], ARGV[2] .. '|' .. qty .. '|' .. user .. '|' .. idem, 'EX', 86400)
redis.call('INCRBY', KEYS[2], tonumber(qty))
redis.call('DECRBY', ARGV[3] .. ':user:' .. user .. ':purchased', tonumber(qty))
redis.call('DEL', ARGV[3] .. ':idem:' .. idem)
redis.call('ZREM', KEYS[3], ARGV[1])
return 1
`)

type BuyResult struct {
	Code      int64
	Remaining int64
	OrderID   string
}

func ReserveStock(ctx context.Context, client *redis.Client, productID, userID, idemHash, orderID string,
	quantity, maxPerUser int, idemTTL, resvTTL, quotaTTL time.Duration) (*BuyResult, error) {

	keys := []string{
		StockKey(productID),
		IdemKey(productID, idemHash),
		UserKey(productID, userID),
		StateKey(productID),
		ResvKey(productID, orderID),
		DeadlinesKey(productID),
	}
	raw, err := buyScript.Run(ctx, client, keys,
		quantity, maxPerUser, int(idemTTL.Seconds()), orderID, int(resvTTL.Seconds()),
		int(quotaTTL.Seconds()), userID, idemHash).Slice()
	if err != nil {
		return nil, err
	}

	res := &BuyResult{Code: raw[0].(int64)}
	switch res.Code {
	case CodeSuccess:
		res.Remaining = raw[1].(int64)
		res.OrderID = raw[2].(string)
	case CodeDuplicate:
		res.OrderID = raw[1].(string)
	}
	return res, nil
}

// CancelReservation rolls back a reservation (state = CANCELLED or EXPIRED).
// Returns true if this call performed the rollback, false if the reservation
// had already left RESERVED (confirmed or already rolled back).
func CancelReservation(ctx context.Context, client *redis.Client, productID, orderID, state string) (bool, error) {
	keys := []string{
		ResvKey(productID, orderID),
		StockKey(productID),
		DeadlinesKey(productID),
	}
	n, err := cancelScript.Run(ctx, client, keys, orderID, state, salePrefix(productID)).Int()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// DueReservations returns order IDs whose reservation deadline has passed.
func DueReservations(ctx context.Context, client *redis.Client, productID string, limit int64) ([]string, error) {
	now := time.Now().Unix()
	return client.ZRangeByScore(ctx, DeadlinesKey(productID), &redis.ZRangeBy{
		Min: "-inf", Max: fmt.Sprintf("%d", now), Count: limit,
	}).Result()
}

// SeedStock resets the sale: stock set, gate opened, stale deadlines cleared.
func SeedStock(ctx context.Context, client *redis.Client, productID string, quantity int) error {
	pipe := client.TxPipeline()
	pipe.Set(ctx, StockKey(productID), quantity, 0)
	pipe.Set(ctx, StateKey(productID), "OPEN", 0)
	pipe.Del(ctx, DeadlinesKey(productID))
	_, err := pipe.Exec(ctx)
	return err
}

func GetStock(ctx context.Context, client *redis.Client, productID string) (int64, error) {
	return client.Get(ctx, StockKey(productID)).Int64()
}

func Close(client *redis.Client) error {
	return client.Close()
}
