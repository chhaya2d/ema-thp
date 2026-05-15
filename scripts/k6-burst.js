// k6 burst — demonstrates Ema's two-layer rate limit story:
//   1. Service-layer limit (user + tenant) fires at the service entry, BEFORE the cache lookup.
//      So even high-RPS bursts of identical (cacheable) queries are throttled per-user.
//   2. Cache absorbs the upstream load — at most one provider call per cache TTL window.
//
// Pairs with `gradlew run --args web` running locally.
//
// Run:
//   k6 run scripts/k6-burst.js                       # default 100 RPS as alice for 2 min
//   k6 run -e USER=bob -e BASE=http://localhost:8080 scripts/k6-burst.js
//
// Then in another shell:
//   curl -s http://localhost:8080/metrics            # see counters & freshness histogram
//
// Expected observation:
//
//   Service-layer behavior:
//     - First ~10 requests succeed (user bucket burst=10, refill 5 RPS).
//     - Sustained 100 RPS << refill rate → ~5 successes/sec, ~95 denials/sec.
//     - Net: ~600 OK + ~11,400 429s over 2 min as alice.
//     - 429s carry RATE_LIMIT_EXHAUSTED envelope + Retry-After header + scope=USER.
//
//   Cache / connector behavior:
//     - The few requests that pass the service limiter hit the snapshot cache after first
//       materialization. Notion TTL = 2 min (default), so 1 provider call covers the run.
//     - emathp_provider_calls_total{connector=notion} barely climbs.
//     - emathp_snapshot_cache_hits_total{connector=notion} climbs with the OK responses.
//
// This proves: "Ema honors its own SLO per user even when responses are cached."

import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: 100,               // 100 req/sec — far above user bucket (5 RPS refill, 10 burst)
      timeUnit: '1s',
      duration: '2m',
      preAllocatedVUs: 30,
      maxVUs: 100,
    },
  },
  thresholds: {
    // Expect a flood of 429s — that's the point. Don't fail the run on them.
    'http_req_failed': ['rate<1.0'],
  },
};

const BASE = __ENV.BASE || 'http://localhost:8080';
const USER = __ENV.USER || 'alice';

// Identical payload every request — would cache hit on the engine side, but the service-layer
// limiter still debits the user bucket per request.
const PAYLOAD = JSON.stringify({
  sql: "SELECT title, updatedAt FROM notion WHERE updatedAt > '2020-01-01' ORDER BY updatedAt DESC LIMIT 20",
  pageSize: 5,
  connectorMode: 'demo',
  mockUserId: USER,
  // No maxStaleness — let the connector default (Notion = 2 min) apply. Cache stays fresh for
  // the full 2-min run, so the few non-rate-limited requests are mostly cache hits.
});

export default function () {
  const res = http.post(`${BASE}/api/query`, PAYLOAD, {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'api_query' },
  });
  check(res, {
    'is 200 or 429': (r) => r.status === 200 || r.status === 429,
    '429 has Retry-After': (r) =>
      r.status !== 429 || r.headers['Retry-After'] !== undefined,
    '429 envelope has code RATE_LIMIT_EXHAUSTED': (r) =>
      r.status !== 429 || (r.body && r.body.includes('RATE_LIMIT_EXHAUSTED')),
  });
}
