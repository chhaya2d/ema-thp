package org.emathp.web;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.emathp.query.DebugResponseContext;
import org.emathp.query.ErrorCode;
import org.emathp.query.ResponseContext;

/**
 * Thin HTTP boundary helper. All knowledge of HTTP headers — both reading inbound and writing
 * outbound — lives here; everything downstream operates on {@link ResponseContext} (the typed
 * canonical representation of what the wire carries) and is HTTP-agnostic.
 *
 * <h3>Inbound headers</h3>
 *
 * <table>
 *   <caption>Request headers consumed by {@link #parse}</caption>
 *   <tr><th>Header</th><th>Becomes</th></tr>
 *   <tr><td>{@code Content-Type}</td><td>{@code wantsJson} flag (JSON vs form parser switch)</td></tr>
 *   <tr><td>{@code X-User-Id}</td><td>{@code userIdHeader} — preferred over body {@code mockUserId}</td></tr>
 *   <tr><td>{@code Cache-Control: max-age=N}</td><td>{@code maxStaleness = Duration.ofSeconds(N)}</td></tr>
 *   <tr><td>{@code Debug}</td><td>{@code debug} flag — gates debug response headers</td></tr>
 * </table>
 *
 * <h3>Outbound headers, sourced from {@link ResponseContext}</h3>
 *
 * <table>
 *   <caption>Always-on headers</caption>
 *   <tr><th>Header</th><th>Source</th></tr>
 *   <tr><td>{@code Content-Type}</td><td>application/json or text/html</td></tr>
 *   <tr><td>{@code X-Trace-Id}</td><td>{@code reqHeaders.traceId()}</td></tr>
 *   <tr><td>{@code X-RateLimit-Status}</td><td>{@code rc.rateLimitStatus()}</td></tr>
 * </table>
 *
 * <table>
 *   <caption>Conditional headers</caption>
 *   <tr><th>Header</th><th>When</th><th>Source</th></tr>
 *   <tr><td>{@code X-Cache-Status}</td><td>Success (cacheStatus non-null)</td><td>{@code rc.cacheStatus()}: {@code HIT} or {@code MISS}</td></tr>
 *   <tr><td>{@code X-Freshness-Ms}</td><td>Success (freshnessMs non-null)</td><td>{@code rc.freshnessMs()}: age in ms</td></tr>
 *   <tr><td>{@code X-RateLimit-Scope}</td><td>{@code RATE_LIMIT_EXHAUSTED} with scope</td><td>{@code failure.violatedScope()}</td></tr>
 *   <tr><td>{@code Retry-After}</td><td>Failure with retry hint</td><td>{@link ErrorResponder}</td></tr>
 * </table>
 *
 * <table>
 *   <caption>Debug headers ({@code Debug: true} request, ResponseContext.debug non-null)</caption>
 *   <tr><th>Header</th><th>Source</th></tr>
 *   <tr><td>{@code X-Snapshot-Path}</td><td>{@code rc.debug().snapshotPath()}</td></tr>
 *   <tr><td>{@code X-Query-Hash}</td><td>{@code rc.debug().queryHash()}</td></tr>
 *   <tr><td>{@code X-Tenant-Id}</td><td>{@code rc.debug().tenantId()}</td></tr>
 *   <tr><td>{@code X-Role}</td><td>{@code rc.debug().roleSlug()}</td></tr>
 * </table>
 */
final class HttpEnvelope {

    private HttpEnvelope() {}

    /**
     * Parsed view of inbound HTTP headers. Body parsing remains in the caller because it depends
     * on demo-specific fields (connectorMode, mockUserId).
     */
    record RequestHeaders(
            boolean wantsJson,
            String userIdHeader,
            Duration maxStaleness,
            boolean debug,
            String traceId) {}

    /** Reads HTTP request headers and generates a server-side traceId. */
    static RequestHeaders parse(HttpExchange ex) {
        Headers h = ex.getRequestHeaders();
        String ct = h.getFirst("Content-Type");
        boolean wantsJson = ct != null && ct.toLowerCase().startsWith("application/json");

        String userId = h.getFirst("X-User-Id");
        if (userId != null) {
            userId = userId.trim();
            if (userId.isEmpty()) userId = null;
        }

        Duration maxStaleness = parseCacheControlMaxAge(h.getFirst("Cache-Control"));
        boolean debug = "true".equalsIgnoreCase(h.getFirst("Debug"));

        return new RequestHeaders(
                wantsJson, userId, maxStaleness, debug, UUID.randomUUID().toString());
    }

    /**
     * Parses the {@code max-age} directive from a {@code Cache-Control} header value.
     * Returns null when the header is absent, doesn't contain {@code max-age}, has a
     * non-numeric value, or has a negative value.
     */
    static Duration parseCacheControlMaxAge(String cacheControl) {
        if (cacheControl == null) {
            return null;
        }
        for (String part : cacheControl.split(",")) {
            String p = part.trim();
            if (p.toLowerCase().startsWith("max-age=")) {
                try {
                    long seconds = Long.parseLong(p.substring("max-age=".length()).trim());
                    if (seconds < 0) {
                        return null;
                    }
                    return Duration.ofSeconds(seconds);
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }
        return null;
    }

    // -------- write: success --------

    /** Writes a successful JSON response: status 200, all applicable headers, JSON body. */
    static void writeSuccessJson(HttpExchange ex, RequestHeaders reqHeaders, ResponseContext rc)
            throws IOException {
        applySuccessHeaders(ex.getResponseHeaders(), reqHeaders, rc);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        byte[] bytes = rc.body().toString().getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    /**
     * Writes a successful HTML wrap: same headers as JSON, body is the supplied HTML.
     * Used when the caller didn't ask for JSON (no {@code application/json} Content-Type).
     */
    static void writeSuccessHtml(
            HttpExchange ex, RequestHeaders reqHeaders, ResponseContext rc, String html)
            throws IOException {
        applySuccessHeaders(ex.getResponseHeaders(), reqHeaders, rc);
        ex.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        byte[] bytes = html.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    // -------- write: failure --------

    /**
     * Writes a failure response with full {@link ResponseContext} context (the service-returned
     * path). Adds debug headers from {@code rc.debug()} if the caller requested {@code Debug:
     * true}; delegates status / Retry-After / body envelope to {@link ErrorResponder}.
     */
    static void writeFailure(HttpExchange ex, RequestHeaders reqHeaders, ResponseContext rc)
            throws IOException {
        ResponseContext.Outcome.Failure failure = (ResponseContext.Outcome.Failure) rc.outcome();
        applyFailureRateLimitHeaders(ex.getResponseHeaders(), failure);
        if (reqHeaders.debug() && rc.debug() != null) {
            applyDebugHeaders(ex.getResponseHeaders(), rc.debug());
        }
        ErrorResponder.writeFailure(ex, reqHeaders.wantsJson(), reqHeaders.traceId(), failure);
    }

    /**
     * Writes a failure that originated before the service layer was called (e.g. parse-time
     * BAD_QUERY). No {@link ResponseContext} exists, so no debug headers are emitted; rate-limit
     * status is derived from the failure code.
     */
    static void writeFailure(
            HttpExchange ex,
            RequestHeaders reqHeaders,
            ResponseContext.Outcome.Failure failure)
            throws IOException {
        applyFailureRateLimitHeaders(ex.getResponseHeaders(), failure);
        ErrorResponder.writeFailure(ex, reqHeaders.wantsJson(), reqHeaders.traceId(), failure);
    }

    // -------- header helpers (package-private for unit testing) --------

    /**
     * Applies the always-on success headers (X-Trace-Id, X-Cache-Status, X-Freshness-Ms,
     * X-RateLimit-Status) plus debug headers when {@code reqHeaders.debug()} is true and the
     * ResponseContext carries a non-null debug payload. Caller sets {@code Content-Type}
     * separately.
     */
    static void applySuccessHeaders(
            Headers responseHeaders, RequestHeaders reqHeaders, ResponseContext rc) {
        responseHeaders.add("X-Trace-Id", reqHeaders.traceId());
        applyCacheStatusHeader(responseHeaders, rc);
        applyFreshnessHeader(responseHeaders, rc);
        applyRateLimitStatusHeader(responseHeaders, rc != null ? rc.rateLimitStatus() : null);
        if (reqHeaders.debug() && rc != null && rc.debug() != null) {
            applyDebugHeaders(responseHeaders, rc.debug());
        }
    }

    /** {@code X-Cache-Status: HIT | MISS} from {@link ResponseContext#cacheStatus()}. No-op when null. */
    static void applyCacheStatusHeader(Headers responseHeaders, ResponseContext rc) {
        if (rc == null || rc.cacheStatus() == null) {
            return;
        }
        responseHeaders.add("X-Cache-Status", rc.cacheStatus());
    }

    /** {@code X-Freshness-Ms: <ms>} from {@link ResponseContext#freshnessMs()}. No-op when null. */
    static void applyFreshnessHeader(Headers responseHeaders, ResponseContext rc) {
        if (rc == null || rc.freshnessMs() == null) {
            return;
        }
        responseHeaders.add("X-Freshness-Ms", String.valueOf(rc.freshnessMs()));
    }

    /**
     * {@code X-RateLimit-Status: OK | EXHAUSTED} on success responses. Defaults to {@code OK}
     * when the value is null (defensive — the service-layer always sets it).
     */
    static void applyRateLimitStatusHeader(Headers responseHeaders, String rateLimitStatus) {
        responseHeaders.add(
                "X-RateLimit-Status", rateLimitStatus != null ? rateLimitStatus : "OK");
    }

    /**
     * On failure responses, emits {@code X-RateLimit-Status} (always, derived from code) and
     * {@code X-RateLimit-Scope} (only on {@code RATE_LIMIT_EXHAUSTED} failures with a non-null
     * {@code violatedScope}).
     */
    static void applyFailureRateLimitHeaders(
            Headers responseHeaders, ResponseContext.Outcome.Failure failure) {
        boolean isRateLimit = failure.code() == ErrorCode.RATE_LIMIT_EXHAUSTED;
        responseHeaders.add("X-RateLimit-Status", isRateLimit ? "EXHAUSTED" : "OK");
        if (isRateLimit && failure.violatedScope() != null && !failure.violatedScope().isBlank()) {
            responseHeaders.add("X-RateLimit-Scope", failure.violatedScope());
        }
    }

    /**
     * All four debug headers from a {@link DebugResponseContext} — gated by the caller having
     * sent {@code Debug: true}. Each field is independently optional.
     */
    static void applyDebugHeaders(Headers responseHeaders, DebugResponseContext debug) {
        if (debug == null) {
            return;
        }
        if (debug.snapshotPath() != null) {
            responseHeaders.add("X-Snapshot-Path", debug.snapshotPath());
        }
        if (debug.queryHash() != null) {
            responseHeaders.add("X-Query-Hash", debug.queryHash());
        }
        if (debug.tenantId() != null && !debug.tenantId().isBlank()) {
            responseHeaders.add("X-Tenant-Id", debug.tenantId());
        }
        if (debug.roleSlug() != null && !debug.roleSlug().isBlank()) {
            responseHeaders.add("X-Role", debug.roleSlug());
        }
    }
}
