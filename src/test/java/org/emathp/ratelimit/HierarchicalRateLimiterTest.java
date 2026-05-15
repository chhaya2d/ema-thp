package org.emathp.ratelimit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HierarchicalRateLimiterTest {

    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        pool = Executors.newFixedThreadPool(16);
    }

    @AfterEach
    void tearDown() {
        pool.shutdown();
    }

    @Test
    void tryAcquireOkWhenAllScopesHaveBudget() {
        HierarchicalRateLimiterConfig cfg =
                new HierarchicalRateLimiterConfig(
                        new TokenBucketConfig(1000.0, 100.0),
                        new TokenBucketConfig(1000.0, 100.0),
                        new TokenBucketConfig(1000.0, 100.0));
        HierarchicalRateLimiter limiter = new HierarchicalRateLimiter(cfg);
        RequestContext ctx = new RequestContext("t1", "u1", "google-drive");
        RateLimitResult r = limiter.tryAcquire(ctx);
        assertTrue(r.allowed());
        assertEquals(0L, r.retryAfterMs());
        assertNull(r.violatedScope());
        assertTrue(limiter.allow(ctx));
    }

    @Test
    void firstViolatedScopeIsConnectorWhenConnectorExhausted() {
        FakeNanoClock clock = new FakeNanoClock();
        HierarchicalRateLimiterConfig cfg =
                new HierarchicalRateLimiterConfig(
                        new TokenBucketConfig(10.0, 1.0),
                        new TokenBucketConfig(10.0, 10.0),
                        new TokenBucketConfig(10.0, 10.0));
        HierarchicalRateLimiter limiter = new HierarchicalRateLimiter(clock, cfg);
        RequestContext ctx = new RequestContext("t1", "u1", "google-drive");
        assertTrue(limiter.tryAcquire(ctx).allowed());
        RateLimitResult denied = limiter.tryAcquire(ctx);
        assertFalse(denied.allowed());
        assertEquals(RateLimitScope.CONNECTOR, denied.violatedScope());
        assertTrue(denied.retryAfterMs() >= 1L);
    }

    @Test
    void tenantDenyRollsBackConnectorDebit() {
        FakeNanoClock clock = new FakeNanoClock();
        // Connector burst 2 so we can observe "would be 0" if rollback were missing.
        HierarchicalRateLimiterConfig cfg =
                new HierarchicalRateLimiterConfig(
                        new TokenBucketConfig(100.0, 2.0),
                        new TokenBucketConfig(10.0, 1.0),
                        new TokenBucketConfig(100.0, 100.0));
        HierarchicalRateLimiter limiter = new HierarchicalRateLimiter(clock, cfg);
        RequestContext ctx = new RequestContext("t1", "u1", "google-drive");
        assertTrue(limiter.tryAcquire(ctx).allowed());
        RateLimitResult denied = limiter.tryAcquire(ctx);
        assertFalse(denied.allowed());
        assertEquals(RateLimitScope.TENANT, denied.violatedScope());
        // Refill tenant (0 → 1 over 200ms at 10 tok/s); connector must still have one token left
        // thanks to rollback; otherwise the next line would fail on CONNECTOR first.
        clock.advanceMillis(200);
        RateLimitResult ok = limiter.tryAcquire(ctx);
        assertTrue(ok.allowed(), "connector budget must not leak when tenant denies");
    }

    @Test
    void concurrentBurstRespectsConnectorCapacity() throws Exception {
        FakeNanoClock clock = new FakeNanoClock();
        clock.setNanos(0L);
        HierarchicalRateLimiterConfig cfg =
                new HierarchicalRateLimiterConfig(
                        new TokenBucketConfig(1.0, 5.0),
                        new TokenBucketConfig(1_000_000.0, 1_000_000.0),
                        new TokenBucketConfig(1_000_000.0, 1_000_000.0));
        HierarchicalRateLimiter limiter = new HierarchicalRateLimiter(clock, cfg);
        RequestContext ctx = new RequestContext("tenant-x", "user-y", "notion");

        int threads = 50;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger connectorDenials = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(
                    () -> {
                        try {
                            start.await();
                            RateLimitResult r = limiter.tryAcquire(ctx);
                            if (r.allowed()) {
                                successes.incrementAndGet();
                            } else if (r.violatedScope() == RateLimitScope.CONNECTOR) {
                                connectorDenials.incrementAndGet();
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        } finally {
                            done.countDown();
                        }
                    });
        }
        start.countDown();
        done.await();
        assertEquals(5, successes.get(), "burst capacity 5 on connector");
        assertEquals(threads - 5, connectorDenials.get());
    }

    @Test
    void serviceLayerConfig_skipsConnectorScope() {
        // forService() leaves connector null — only tenant + user buckets are checked.
        FakeNanoClock clock = new FakeNanoClock();
        HierarchicalRateLimiterConfig cfg =
                HierarchicalRateLimiterConfig.forService(
                        new TokenBucketConfig(10.0, 10.0), // tenant
                        new TokenBucketConfig(10.0, 1.0)); // user, burst=1
        HierarchicalRateLimiter limiter = new HierarchicalRateLimiter(clock, cfg);
        RequestContext ctx = new RequestContext("t1", "u1", "google-drive");

        // First call succeeds (user burst=1).
        assertTrue(limiter.tryAcquire(ctx).allowed());
        // Second call denied — but at USER scope, not CONNECTOR, because connector bucket is
        // null and therefore not checked.
        RateLimitResult denied = limiter.tryAcquire(ctx);
        assertFalse(denied.allowed());
        assertEquals(RateLimitScope.USER, denied.violatedScope());
    }

    @Test
    void connectorLayerConfig_skipsTenantAndUserScopes() {
        // forConnector() leaves tenant + user null — only the connector bucket is checked.
        FakeNanoClock clock = new FakeNanoClock();
        HierarchicalRateLimiterConfig cfg =
                HierarchicalRateLimiterConfig.forConnector(new TokenBucketConfig(10.0, 1.0));
        HierarchicalRateLimiter limiter = new HierarchicalRateLimiter(clock, cfg);
        RequestContext ctx = new RequestContext("t1", "u1", "google-drive");

        assertTrue(limiter.tryAcquire(ctx).allowed());
        RateLimitResult denied = limiter.tryAcquire(ctx);
        assertFalse(denied.allowed());
        assertEquals(RateLimitScope.CONNECTOR, denied.violatedScope());
    }
}
