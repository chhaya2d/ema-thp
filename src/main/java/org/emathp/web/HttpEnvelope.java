package org.emathp.web;

import com.google.gson.JsonObject;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.emathp.query.ErrorCode;
import org.emathp.query.RequestContext;
import org.emathp.query.ResponseContext;

/**
 * Thin HTTP boundary helper. All knowledge of HTTP headers — both reading
 * inbound and writing outbound — lives here; everything downstream operates
 * on {@link RequestContext} / {@link ResponseContext} and is HTTP-agnostic.
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
 * <h3>Outbound headers — always set</h3>
 *
 * <table>
 *   <caption>Response headers emitted on every response</caption>
 *   <tr><th>Header</th><th>Value</th></tr>
 *   <tr><td>{@code Content-Type}</td><td>{@code application/json; charset=utf-8} or {@code text/html; charset=utf-8}</td></tr>
 *   <tr><td>{@code X-Trace-Id}</td><td>server-generated UUID for this request</td></tr>
 * </table>
 *
 * <h3>Outbound headers — conditional</h3>
 *
 * <table>
 *   <caption>Response headers emitted under specific conditions</caption>
 *   <tr><th>Header</th><th>When</th><th>Source</th></tr>
 *   <tr><td>{@code X-Cache-Status}</td><td>Success path</td><td>Body {@code cacheHit}: {@code HIT} or {@code MISS}</td></tr>
 *   <tr><td>{@code X-Freshness-Ms}</td><td>Success with non-null body {@code freshness_ms}</td><td>Age of the freshest used chunk, in milliseconds (matches body field exactly)</td></tr>
 *   <tr><td>{@code X-RateLimit-Status}</td><td>Always</td><td>Body {@code rate_limit_status} or derived from failure code: {@code OK} / {@code EXHAUSTED}</td></tr>
 *   <tr><td>{@code X-RateLimit-Scope}</td><td>{@code RATE_LIMIT_EXHAUSTED} failure with a scope</td><td>{@code failure.violatedScope()}: {@code USER} / {@code TENANT} / {@code CONNECTOR}</td></tr>
 *   <tr><td>{@code Retry-After}</td><td>Failure with {@code RATE_LIMIT_EXHAUSTED}</td><td>{@link ErrorResponder}</td></tr>
 * </table>
 *
 * <h3>Outbound headers — debug-only ({@code Debug: true} request)</h3>
 *
 * <table>
 *   <caption>Response headers emitted only when the request opted in via Debug</caption>
 *   <tr><th>Header</th><th>Source</th></tr>
 *   <tr><td>{@code X-Snapshot-Path}</td><td>Body {@code snapshotPath}</td></tr>
 *   <tr><td>{@code X-Query-Hash}</td><td>Body {@code queryHash}</td></tr>
 *   <tr><td>{@code X-Tenant-Id}</td><td>{@code ctx.tenantId()}</td></tr>
 *   <tr><td>{@code X-Role}</td><td>{@code ctx.scope().roleSlug()}</td></tr>
 * </table>
 */
final class HttpEnvelope {

    private HttpEnvelope() {}

    /**
     * Parsed view of inbound HTTP headers. Body parsing remains in the caller
     * because it depends on demo-specific fields (connectorMode, mockUserId).
     */
    record RequestHeaders(
            boolean wantsJson,
            String userIdHeader,    // nullable — header absent or blank
            Duration maxStaleness,  // nullable — Cache-Control absent or malformed
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

    /**
     * Writes a successful JSON response: status 200, all applicable headers, JSON body.
     */
    static void writeSuccessJson(
            HttpExchange ex,
            RequestHeaders reqHeaders,
            RequestContext ctx,
            JsonObject body)
            throws IOException {
        applySuccessHeaders(ex.getResponseHeaders(), reqHeaders, ctx, body);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
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
            HttpExchange ex,
            RequestHeaders reqHeaders,
            RequestContext ctx,
            JsonObject body,
            String html)
            throws IOException {
        applySuccessHeaders(ex.getResponseHeaders(), reqHeaders, ctx, body);
        ex.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        byte[] bytes = html.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    /**
     * Writes a failure response. Adds debug headers if requested + ctx is non-null,
     * then delegates status / Retry-After / body envelope to {@link ErrorResponder}.
     *
     * @param ctx nullable — may be absent for parse-time failures (e.g. malformed JSON)
     *            that fail before {@link RequestContext} is built
     */
    static void writeFailure(
            HttpExchange ex,
            RequestHeaders reqHeaders,
            RequestContext ctx,
            ResponseContext.Outcome.Failure failure)
            throws IOException {
        applyFailureRateLimitHeaders(ex.getResponseHeaders(), failure);
        if (reqHeaders.debug() && ctx != null) {
            applyDebugContextHeaders(ex.getResponseHeaders(), ctx);
        }
        ErrorResponder.writeFailure(ex, reqHeaders.wantsJson(), reqHeaders.traceId(), failure);
    }

    // -------- helpers (package-private for unit testing) --------

    /**
     * Applies the always-on success headers ({@code X-Trace-Id}, {@code X-Cache-Status})
     * plus debug headers when requested. Caller sets {@code Content-Type} separately.
     */
    static void applySuccessHeaders(
            Headers responseHeaders,
            RequestHeaders reqHeaders,
            RequestContext ctx,
            JsonObject body) {
        responseHeaders.add("X-Trace-Id", reqHeaders.traceId());
        applyCacheStatusHeader(responseHeaders, body);
        applyFreshnessHeader(responseHeaders, body);
        applySuccessRateLimitStatusHeader(responseHeaders, body);
        if (reqHeaders.debug()) {
            applyDebugBodyHeaders(responseHeaders, body);
            if (ctx != null) {
                applyDebugContextHeaders(responseHeaders, ctx);
            }
        }
    }

    /** {@code X-Cache-Status: HIT | MISS} from body {@code cacheHit}. No-op if absent. */
    static void applyCacheStatusHeader(Headers responseHeaders, JsonObject body) {
        if (body == null || !body.has("cacheHit") || body.get("cacheHit").isJsonNull()) {
            return;
        }
        responseHeaders.add(
                "X-Cache-Status", body.get("cacheHit").getAsBoolean() ? "HIT" : "MISS");
    }

    /**
     * {@code X-Freshness-Ms: <ms>} from body {@code freshness_ms}. Matches the body
     * field 1:1 (same unit, same value). No-op when the field is absent or null
     * (zero-row responses, responses that touched no chunks).
     */
    static void applyFreshnessHeader(Headers responseHeaders, JsonObject body) {
        if (body == null
                || !body.has("freshness_ms")
                || body.get("freshness_ms").isJsonNull()) {
            return;
        }
        responseHeaders.add("X-Freshness-Ms", String.valueOf(body.get("freshness_ms").getAsLong()));
    }

    /**
     * {@code X-RateLimit-Status: OK | EXHAUSTED} on success responses, read from body
     * {@code rate_limit_status}. Defaults to {@code OK} when the field is absent (the
     * service-layer always sets it today, but we don't want a missing field to drop the
     * header silently).
     */
    static void applySuccessRateLimitStatusHeader(Headers responseHeaders, JsonObject body) {
        String status = "OK";
        if (body != null
                && body.has("rate_limit_status")
                && !body.get("rate_limit_status").isJsonNull()) {
            status = body.get("rate_limit_status").getAsString();
        }
        responseHeaders.add("X-RateLimit-Status", status);
    }

    /**
     * On failure responses, emits {@code X-RateLimit-Status} (always, derived from code) and
     * {@code X-RateLimit-Scope} (only on {@code RATE_LIMIT_EXHAUSTED} failures that carry a
     * non-null {@code violatedScope}).
     */
    static void applyFailureRateLimitHeaders(
            Headers responseHeaders, ResponseContext.Outcome.Failure failure) {
        boolean isRateLimit = failure.code() == ErrorCode.RATE_LIMIT_EXHAUSTED;
        responseHeaders.add("X-RateLimit-Status", isRateLimit ? "EXHAUSTED" : "OK");
        if (isRateLimit && failure.violatedScope() != null && !failure.violatedScope().isBlank()) {
            responseHeaders.add("X-RateLimit-Scope", failure.violatedScope());
        }
    }

    /** Body-derived debug headers ({@code X-Snapshot-Path}, {@code X-Query-Hash}). */
    static void applyDebugBodyHeaders(Headers responseHeaders, JsonObject body) {
        if (body == null) {
            return;
        }
        if (body.has("snapshotPath") && !body.get("snapshotPath").isJsonNull()) {
            responseHeaders.add("X-Snapshot-Path", body.get("snapshotPath").getAsString());
        }
        if (body.has("queryHash") && !body.get("queryHash").isJsonNull()) {
            responseHeaders.add("X-Query-Hash", body.get("queryHash").getAsString());
        }
    }

    /** Context-derived debug headers ({@code X-Tenant-Id}, {@code X-Role}). */
    static void applyDebugContextHeaders(Headers responseHeaders, RequestContext ctx) {
        if (ctx == null) {
            return;
        }
        if (ctx.tenantId() != null && !ctx.tenantId().isBlank()) {
            responseHeaders.add("X-Tenant-Id", ctx.tenantId());
        }
        if (ctx.scope() != null && ctx.scope().roleSlug() != null
                && !ctx.scope().roleSlug().isBlank()) {
            responseHeaders.add("X-Role", ctx.scope().roleSlug());
        }
    }
}
