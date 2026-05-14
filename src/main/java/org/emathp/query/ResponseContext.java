package org.emathp.query;

import com.google.gson.JsonObject;
import java.util.Objects;

/**
 * Service-level response envelope: outcome (success body or failure code), trace echo,
 * server-side elapsed time, and data freshness. The HTTP layer maps {@link
 * Outcome.Failure#code()} → status code and {@link Outcome.Failure#retryAfterMs()} → {@code
 * Retry-After} header.
 *
 * <p>Exceptions never cross the service boundary — every failure path returns a {@link
 * Outcome.Failure}. Tests prefer the throwing convenience on {@link FederatedQueryService}.
 *
 * @param freshnessMs      age in ms of the freshest used snapshot data (now − oldest chunk
 *                         createdAt). {@code null} on failures, on responses that touched no
 *                         chunks (zero-row), or when the body does not carry the field.
 * @param rateLimitStatus  {@code "OK"} on success and non-rate-limit failures; {@code
 *                         "EXHAUSTED"} when the request was denied with {@link
 *                         ErrorCode#RATE_LIMIT_EXHAUSTED}. Mirrors PDF's {@code
 *                         rate_limit_status} response field.
 */
public record ResponseContext(
        String traceId,
        long serverElapsedMs,
        Long freshnessMs,
        String rateLimitStatus,
        Outcome outcome) {

    public ResponseContext {
        Objects.requireNonNull(traceId, "traceId");
        Objects.requireNonNull(outcome, "outcome");
    }

    public boolean isSuccess() {
        return outcome instanceof Outcome.Success;
    }

    /** Convenience for success-only paths; throws if outcome is a failure. */
    public JsonObject body() {
        if (outcome instanceof Outcome.Success s) {
            return s.body();
        }
        throw new IllegalStateException("ResponseContext is a failure: " + outcome);
    }

    public sealed interface Outcome {

        record Success(JsonObject body) implements Outcome {
            public Success {
                Objects.requireNonNull(body, "body");
            }
        }

        /**
         * @param retryAfterMs  client-facing retry hint (rate-limit), or {@code null}
         * @param violatedScope rate-limit scope (CONNECTOR/TENANT/USER) on rate limit, or
         *                      connector name on source errors, or {@code null}
         */
        record Failure(ErrorCode code, String message, Long retryAfterMs, String violatedScope)
                implements Outcome {
            public Failure {
                Objects.requireNonNull(code, "code");
                if (message == null) {
                    message = code.name();
                }
            }
        }
    }
}
