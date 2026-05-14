package org.emathp.query;

import java.util.Objects;

/**
 * Lingua-franca exception type for cross-layer failures. Any service-boundary code path that
 * catches a layer-specific exception ({@code RateLimitedException}, connector exceptions, parser
 * failures) should wrap as {@code ApiException} so the HTTP/CLI envelope mapping has one type to
 * pattern-match against.
 *
 * <p>For idiomatic outcome-style responses, prefer letting the service translate exceptions into
 * {@link ResponseContext.Outcome.Failure} at the boundary — this exception is the carrier inside
 * engine code.
 */
public final class ApiException extends RuntimeException {

    private final ErrorCode code;
    private final Long retryAfterMs;
    private final String violatedScope;

    public ApiException(ErrorCode code, String message) {
        this(code, message, null, null, null);
    }

    public ApiException(ErrorCode code, String message, Throwable cause) {
        this(code, message, null, null, cause);
    }

    public ApiException(ErrorCode code, String message, Long retryAfterMs, String violatedScope) {
        this(code, message, retryAfterMs, violatedScope, null);
    }

    public ApiException(
            ErrorCode code, String message, Long retryAfterMs, String violatedScope, Throwable cause) {
        super(message, cause);
        this.code = Objects.requireNonNull(code, "code");
        this.retryAfterMs = retryAfterMs;
        this.violatedScope = violatedScope;
    }

    public ErrorCode code() {
        return code;
    }

    public Long retryAfterMs() {
        return retryAfterMs;
    }

    public String violatedScope() {
        return violatedScope;
    }
}
