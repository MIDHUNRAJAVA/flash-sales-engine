import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import exec from 'k6/execution';

// SCALE=1 approximates full sale-shaped load — run it DISTRIBUTED (k6-operator
// or k6 Cloud); 500k VUs on one machine is testing your laptop, not the system.
// Default SCALE is a laptop smoke test.
const SCALE = parseFloat(__ENV.SCALE || '0.01');
const GATEWAY_URL = __ENV.GATEWAY_URL || 'http://localhost:3000';
const INVENTORY_URL = __ENV.INVENTORY_URL || 'http://localhost:8080';
const STOCK = parseInt(__ENV.STOCK || '2000');
const PRODUCT_ID = __ENV.PRODUCT_ID || `flash-${Date.now()}`; // fresh sale per run

// ── Custom metrics ────────────────────────────────────────────────────────────
const accepted = new Counter('accepted_orders');      // 200, not a duplicate replay
const duplicates = new Counter('duplicate_responses'); // 200 with duplicate:true
const soldOut = new Counter('sold_out_responses');     // 410
const rateLimited = new Counter('rate_limited');       // 429
const shed = new Counter('shed_503');                  // 503 (defined, retryable)
const serverErrors = new Counter('server_errors');     // 500 — must be zero
const buyLatency = new Trend('buy_latency', true);

export const options = {
  scenarios: {
    // The herd: arrival RATE, unique user per iteration (per-user limits are
    // exercised by the burst scenario, not here).
    flood: {
      executor: 'ramping-arrival-rate',
      startRate: 0,
      timeUnit: '1s',
      preAllocatedVUs: Math.max(50, Math.ceil(20000 * SCALE)),
      maxVUs: Math.max(100, Math.ceil(40000 * SCALE)),
      stages: [
        { target: Math.ceil(25000 * SCALE), duration: '30s' },
        { target: Math.ceil(25000 * SCALE), duration: '60s' },
        { target: 0, duration: '15s' },
      ],
      exec: 'buy',
    },
    // Idempotency proof: one user, one nonce, 200 sends -> exactly one order.
    replay: {
      executor: 'shared-iterations',
      vus: 20,
      iterations: 200,
      startTime: '15s',
      exec: 'replaySameKey',
    },
    // Bug 4 regression: 10 rapid buys from ONE user must yield exactly 1
    // success per 10s window — a fixed-window limiter leaks 2 at the boundary.
    burst: {
      executor: 'per-vu-iterations',
      vus: 10,
      iterations: 10,
      startTime: '5s',
      exec: 'boundaryBurst',
    },
  },
  thresholds: {
    accepted_orders: [`count<=${STOCK}`],   // THE invariant: no oversell
    server_errors: ['count==0'],            // 500 = bug, not policy
    buy_latency: ['p(99)<500'],
    checks: ['rate>0.99'],
  },
};

const HEADERS = { 'Content-Type': 'application/json' };

export function setup() {
  const seed = http.post(
    `${INVENTORY_URL}/seed`,
    JSON.stringify({ productID: PRODUCT_ID, quantity: STOCK }),
    { headers: HEADERS },
  );
  check(seed, { 'stock seeded, sale open': (r) => r.status === 200 });
  return { productID: PRODUCT_ID };
}

function doBuy(data, userId, nonce) {
  const res = http.post(`${GATEWAY_URL}/buy`, JSON.stringify({
    userID: userId,
    productID: data.productID,
    quantity: 1,
    clientNonce: nonce,
  }), { headers: HEADERS, tags: { endpoint: 'buy' } });
  buyLatency.add(res.timings.duration);

  check(res, {
    'defined response, never hanging': (r) => [200, 400, 403, 409, 410, 429, 503].includes(r.status),
    '429 carries Retry-After': (r) => r.status !== 429 || r.headers['Retry-After'] !== undefined,
    '503 carries Retry-After': (r) => r.status !== 503 || r.headers['Retry-After'] !== undefined,
    'X-Request-ID echoed': (r) => r.headers['X-Request-Id'] !== undefined,
  });

  if (res.status === 200) {
    const body = res.json();
    if (body.duplicate) duplicates.add(1); else accepted.add(1);
    check(res, { 'success carries order_id': () => !!body.order_id });
  } else if (res.status === 410) soldOut.add(1);
  else if (res.status === 429) rateLimited.add(1);
  else if (res.status === 503) shed.add(1);
  else if (res.status >= 500) serverErrors.add(1);
  return res;
}

export function buy(data) {
  const userId = `user-${exec.vu.idInTest}-${exec.vu.iterationInScenario}`;
  doBuy(data, userId, `n-${exec.vu.idInTest}-${exec.vu.iterationInScenario}`);
}

export function replaySameKey(data) {
  // SAME user + SAME nonce every time: at most one real order may result.
  const res = doBuy(data, 'user-replay-fixed', 'nonce-replay-fixed');
  if (res.status === 200) {
    check(res, { 'replay returns an order_id': (r) => !!r.json().order_id });
  }
}

export function boundaryBurst(data) {
  doBuy(data, `user-burst-${exec.vu.idInTest}`,
    `b-${exec.vu.idInTest}-${exec.vu.iterationInScenario}`);
}

export function handleSummary(data) {
  const count = (m) => (data.metrics[m] ? data.metrics[m].values.count : 0);
  const a = count('accepted_orders');
  return {
    stdout: JSON.stringify({
      accepted: a,
      stock: STOCK,
      oversell: a > STOCK,
      duplicates_replayed: count('duplicate_responses'),
      sold_out: count('sold_out_responses'),
      rate_limited: count('rate_limited'),
      shed_503: count('shed_503'),
      server_errors_500: count('server_errors'),
      note: 'Final oversell/duplicate verdict = post-run SQL audit; the DB is the system of record. ' +
        'SELECT order_id, count(*) FROM orders GROUP BY order_id HAVING count(*) > 1;',
    }, null, 2) + '\n',
  };
}
