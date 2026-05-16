package org.emathp.query;

import com.google.gson.JsonObject;
import java.util.Objects;

/**
 * Service-level response envelope: outcome (success body or failure code), trace echo,
 * server-side elapsed time, and data freshness. The HTTP layer maps every field here to a
 * response header — the typed contract for what the wire carries. To swap HTTP for another
 * transport, only the serializer changes; this record stays canonical.
 *
 * <p>Exceptions never cross the service boundary — every failure path returns a {@link
 * Outcome.Failure}. Tests prefer the throwing convenience on {@link FederatedQueryService}.
 *
 * @param freshnessMs      age in ms of the freshest used snapshot data (now − oldest chunk
 *                         createdAt). {@code null} on failures, on responses that touched no
 *                         chunks (zero-row), or when the body does not carry the field.
 *                         Maps to {@code X-Freshness-Ms} when non-null.
 * @param rateLimitStatus  {@code "OK"} on success and non-rate-limit failures; {@code
 *                         "EXHAUSTED"} when the request was denied with {@link
 *                         ErrorCode#RATE_LIMIT_EXHAUSTED}. Maps to {@code X-RateLimit-Status}.
 * @param cacheStatus      {@code "HIT"} or {@code "MISS"} on success responses;
 *                         {@code null} on failure responses. Single-source: HIT when all sides
 *                         served from chunks; MISS otherwise. Join: HIT when the full
 *                         materialization was reused. Maps to {@code X-Cache-Status}.
 * @param debug            Server-populated observability fields. Always non-null on success
 *                         responses; {@code null} on failures from before identity resolution.
 *                         The HTTP layer emits these as {@code X-Snapshot-Path} /
 *                         {@code X-Query-Hash} / {@code X-Tenant-Id} / {@code X-Role} only when
 *                         the caller sent {@code Debug: true}. Always-on population so tests
 *                         and CLI callers can assert on them unconditionally.
 */
public record ResponseContext(
        String traceId,
        long serverElapsedMs,
        Long freshnessMs,
        String rateLimitStatus,
        String cacheStatus,
        DebugResponseContext debug,
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
