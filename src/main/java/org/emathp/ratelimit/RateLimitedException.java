package org.emathp.ratelimit;

import java.util.Objects;

/**
 * Thrown when a {@link RateLimitPolicy} denies a request. The engine page loop unwinds on this
 * exception so a single denial fails the whole user query rather than producing partial results.
 */
public final class RateLimitedException extends RuntimeException {

    private final RateLimitResult result;

    public RateLimitedException(RateLimitResult result) {
        super(formatMessage(result));
        this.result = Objects.requireNonNull(result, "result");
        if (result.allowed()) {
            throw new IllegalArgumentException("result must be denied");
        }
    }

    public RateLimitResult result() {
        return result;
    }

    public long retryAfterMs() {
        return result.retryAfterMs();
    }

    public RateLimitScope violatedScope() {
        return result.violatedScope();
    }

    private static String formatMessage(RateLimitResult r) {
        return "rate limit exceeded: scope="
                + r.violatedScope()
                + " retryAfterMs="
                + r.retryAfterMs();
    }
}
