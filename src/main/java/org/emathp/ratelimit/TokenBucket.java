package org.emathp.ratelimit;

import java.util.Objects;

/**
 * Thread-safe token bucket with <strong>lazy refill</strong>: no background thread; balance is
 * advanced from {@link TimeSource#nanoTime()} only when {@link #tryConsumeOne()} or {@link
 * #refundOne()} runs.
 *
 * <p>All mutating paths are {@code synchronized} on this instance (simple, predictable; not
 * lock-free). High contention on a hot key will serialize — acceptable for a prototype.
 *
 * <p><strong>Refund tradeoff:</strong> {@link #refundOne()} adds one token after the same lazy
 * refill step used on consume. Under concurrent load this is a <em>best-effort</em> undo for
 * hierarchical acquire rollback: it restores approximate fairness vs leaking quota on partial
 * failure, but is not a formally proven atomic multi-bucket transaction (production systems often use
 * centralized reservation or ordered locking).
 */
public final class TokenBucket {

    private final TimeSource time;
    private final double capacity;
    private final double refillPerNanosecond;
    private double tokens;
    private long lastRefillNanos;

    public TokenBucket(TimeSource time, TokenBucketConfig config) {
        this.time = Objects.requireNonNull(time, "time");
        Objects.requireNonNull(config, "config");
        this.capacity = config.burstCapacity();
        this.refillPerNanosecond = config.refillTokensPerSecond() / 1_000_000_000.0;
        this.tokens = config.burstCapacity();
        this.lastRefillNanos = time.nanoTime();
    }

    /**
     * Refill to {@code now}, then consume one token if available.
     *
     * @return outcome with retry hint when insufficient tokens remain after refill
     */
    public synchronized TryOutcome tryConsumeOne() {
        long now = time.nanoTime();
        refillLocked(now);
        if (tokens + 1e-12 >= 1.0) {
            tokens -= 1.0;
            return TryOutcome.permitted();
        }
        double deficit = 1.0 - tokens;
        double retryNanos = deficit / refillPerNanosecond;
        long retryMs = (long) Math.ceil(retryNanos / 1_000_000.0);
        return TryOutcome.denied(Math.max(1L, retryMs));
    }

    /**
     * Undo one successful {@link #tryConsumeOne()} during hierarchical rollback (see class
     * javadoc).
     */
    public synchronized void refundOne() {
        long now = time.nanoTime();
        refillLocked(now);
        tokens = Math.min(capacity, tokens + 1.0);
    }

    private void refillLocked(long now) {
        long elapsed = now - lastRefillNanos;
        if (elapsed <= 0) {
            return;
        }
        tokens = Math.min(capacity, tokens + elapsed * refillPerNanosecond);
        lastRefillNanos = now;
    }

    /** Result of a single-token try (internal; exposed for tests of {@link TokenBucket} in isolation). */
    public record TryOutcome(boolean allowed, long retryAfterMs) {

        static TryOutcome permitted() {
            return new TryOutcome(true, 0L);
        }

        static TryOutcome denied(long retryAfterMs) {
            return new TryOutcome(false, retryAfterMs);
        }
    }
}
