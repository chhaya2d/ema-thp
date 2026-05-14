package org.emathp.ratelimit;

/**
 * Token-bucket parameters. Refill is lazy: applied inside {@link TokenBucket#tryConsumeOne} using
 * elapsed wall time ({@link TimeSource#nanoTime}).
 *
 * @param refillTokensPerSecond steady-state sustained rate (tokens added per second of elapsed
 *     time). Must be positive.
 * @param burstCapacity maximum token balance (burst). Must be positive.
 */
public record TokenBucketConfig(double refillTokensPerSecond, double burstCapacity) {

    public TokenBucketConfig {
        if (!(refillTokensPerSecond > 0.0)) {
            throw new IllegalArgumentException("refillTokensPerSecond must be > 0");
        }
        if (!(burstCapacity > 0.0)) {
            throw new IllegalArgumentException("burstCapacity must be > 0");
        }
    }
}
