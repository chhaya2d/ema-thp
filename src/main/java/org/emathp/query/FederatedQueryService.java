package org.emathp.query;

import com.google.gson.JsonObject;
import org.emathp.cache.QueryCacheScope;
import org.emathp.pagination.UiResponsePaging;

/**
 * Parses SQL and runs snapshot-backed single- or multi-source queries. Implementations live next
 * to their wiring (e.g. {@code org.emathp.web.DefaultFederatedQueryService}); HTTP adapters call
 * this interface only.
 *
 * <p>Errors do not cross this boundary as exceptions — every failure path returns a {@link
 * ResponseContext.Outcome.Failure}. The HTTP layer maps the failure to status + envelope; tests
 * that want exception-style flow can call {@link #executeOrThrow}.
 */
public interface FederatedQueryService {

    /** Runs the request. Always returns a {@link ResponseContext} (success or failure outcome). */
    ResponseContext execute(RequestContext ctx, FederatedQueryRequest request);

    /** Scope implied by the service's construction-time principal (e.g. server default user). */
    QueryCacheScope defaultCacheScope();

    /**
     * Convenience for CLI / tests that don't need typed-outcome semantics: returns the success
     * body or throws {@link ApiException} on failure.
     */
    default JsonObject executeOrThrow(RequestContext ctx, FederatedQueryRequest request) {
        ResponseContext rc = execute(ctx, request);
        if (rc.outcome() instanceof ResponseContext.Outcome.Success s) {
            return s.body();
        }
        ResponseContext.Outcome.Failure f = (ResponseContext.Outcome.Failure) rc.outcome();
        throw new ApiException(f.code(), f.message(), f.retryAfterMs(), f.violatedScope());
    }

    /** Parses a logical row offset string for UI-style cursor pagination (delegates to paging util). */
    static int parseLogicalCursorOffset(String logicalCursorOffset) {
        return UiResponsePaging.parseUiOffset(logicalCursorOffset);
    }
}
