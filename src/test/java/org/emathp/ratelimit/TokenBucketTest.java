package org.emathp.ratelimit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TokenBucketTest {

    @Test
    void lazyRefillAccumulatesTokensUpToBurst() {
        FakeNanoClock clock = new FakeNanoClock();
        clock.setNanos(0);
        // 10 tokens/sec, burst 2 — start full at 2
        TokenBucket bucket = new TokenBucket(clock, new TokenBucketConfig(10.0, 2.0));
        assertTrue(bucket.tryConsumeOne().allowed());
        assertTrue(bucket.tryConsumeOne().allowed());
        TokenBucket.TryOutcome denied = bucket.tryConsumeOne();
        assertFalse(denied.allowed());
        assertTrue(denied.retryAfterMs() >= 1L);

        // 0.1s → 1 token
        clock.advanceMillis(100);
        assertTrue(bucket.tryConsumeOne().allowed());
    }

    @Test
    void refundRestoresCapacityAfterFailedHierarchySimulation() {
        FakeNanoClock clock = new FakeNanoClock();
        TokenBucket bucket = new TokenBucket(clock, new TokenBucketConfig(100.0, 3.0));
        assertTrue(bucket.tryConsumeOne().allowed());
        bucket.refundOne();
        assertTrue(bucket.tryConsumeOne().allowed());
        assertTrue(bucket.tryConsumeOne().allowed());
        assertTrue(bucket.tryConsumeOne().allowed());
        assertFalse(bucket.tryConsumeOne().allowed());
    }
}
