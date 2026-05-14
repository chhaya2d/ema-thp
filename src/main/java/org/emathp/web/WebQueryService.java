package org.emathp.web;

import java.util.Objects;
import org.emathp.query.ApiException;
import org.emathp.query.ErrorCode;
import org.emathp.query.FederatedQueryRequest;
import org.emathp.query.FederatedQueryService;
import org.emathp.query.RequestContext;
import org.emathp.query.ResponseContext;

/**
 * HTTP-facing façade: selects the {@link FederatedQueryService} stack (demo vs live) for a
 * request. Parsing JSON/form into {@link HttpQueryPayload} stays in {@link DemoWebServer} or
 * callers; this type only routes an already-built {@link FederatedQueryRequest}.
 *
 * <p>Routing failures (unknown {@code connectorMode}, live stack not configured) are surfaced as
 * {@link ResponseContext.Outcome.Failure} so callers can branch on the same outcome shape as
 * execution failures — no exceptions cross this boundary.
 */
public final class WebQueryService {

    private final FederatedQueryService demo;
    private final FederatedQueryService live;

    public WebQueryService(FederatedQueryService demo, FederatedQueryService live) {
        this.demo = Objects.requireNonNull(demo, "demo");
        this.live = live;
    }

    public ResponseContext execute(
            String connectorMode, RequestContext ctx, FederatedQueryRequest request) {
        try {
            return resolve(connectorMode).execute(ctx, request);
        } catch (ApiException e) {
            return new ResponseContext(
                    ctx.traceId(),
                    0L,
                    new ResponseContext.Outcome.Failure(
                            e.code(), e.getMessage(), e.retryAfterMs(), e.violatedScope()));
        }
    }

    private FederatedQueryService resolve(String connectorMode) {
        if ("demo".equalsIgnoreCase(connectorMode)) {
            return demo;
        }
        if ("live".equalsIgnoreCase(connectorMode)) {
            if (live == null) {
                throw new ApiException(
                        ErrorCode.SOURCE_UNAVAILABLE,
                        "Live mode requires CONNECTOR_TOKEN_KEY, GOOGLE_OAUTH_CLIENT_ID, "
                                + "GOOGLE_OAUTH_CLIENT_SECRET, and a working token DB.");
            }
            return live;
        }
        throw new ApiException(
                ErrorCode.BAD_QUERY, "connectorMode must be \"demo\" or \"live\"");
    }

    public boolean hasLiveStack() {
        return live != null;
    }
}
