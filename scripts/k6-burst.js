// k6 burst script — drives the demo server hard enough to trip per-user rate limits
// and warm the snapshot cache. Pairs with `gradlew run --args web` running locally.
//
// Run:
//   k6 run scripts/k6-burst.js                       # default 60 VUs for 60s as alice
//   k6 run -e USER=bob -e BASE=http://localhost:8080 scripts/k6-burst.js
//
// Then in another shell:
//   curl -s http://localhost:8080/metrics            # see counters & freshness histogram
//
// What this proves:
//   - emathp_provider_calls_total  → climbs as page-loop fetches happen.
//   - emathp_rate_limit_denied_total{scope="USER"} → climbs once the per-user bucket
//     (5 rps / burst 10 by default) is exhausted; demo server returns HTTP 429 with
//     Retry-After + RATE_LIMIT_EXHAUSTED envelope.
//   - emathp_snapshot_cache_hits_total / _misses_total → ratio shifts toward hits
//     after the first warm-up call materializes the snapshot.
//   - emathp_response_freshness_ms → histogram populates with age of served data.

import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  scenarios: {
    burst: {
      executor: 'constant-arrival-rate',
      rate: 50,                       // 50 requests/sec — well above 5/s user bucket
      timeUnit: '1s',
      duration: '60s',
      preAllocatedVUs: 30,
      maxVUs: 100,
    },
  },
  thresholds: {
    // Expect some 429s — that's the point of the burst. Do not fail the run on them.
    'http_req_failed': ['rate<1.0'],
  },
};

const BASE = __ENV.BASE || 'http://localhost:8080';
const USER = __ENV.USER || 'alice';

const PAYLOAD = JSON.stringify({
  sql: "SELECT title, updatedAt FROM resources WHERE updatedAt > '2020-01-01' ORDER BY updatedAt DESC LIMIT 20",
  pageSize: 5,
  connectorMode: 'demo',
  mockUserId: USER,
  maxStaleness: '30s',
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
  });
  // No sleep — let the arrival-rate executor pace.
}
