package org.emathp.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpPrincipal;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import org.emathp.auth.UserContext;
import org.emathp.cache.QueryCacheScope;
import org.emathp.query.ErrorCode;
import org.emathp.query.RequestContext;
import org.emathp.query.ResponseContext;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link HttpEnvelope}. Covers:
 *
 * <ul>
 *   <li>{@code parse()} — request header reads (Content-Type, X-User-Id, Cache-Control, Debug)
 *   <li>{@code parseCacheControlMaxAge()} — the standalone Cache-Control parser
 *   <li>Header-applying helpers — what gets written into the response Headers map
 * </ul>
 *
 * <p>Doesn't exercise the full {@code writeSuccess*}/{@code writeFailure} methods because they
 * touch real IO ({@code sendResponseHeaders}, OutputStream); the header logic they
 * delegate to is fully covered by the helper tests.
 */
class HttpEnvelopeTest {

    // ---------- parse() ----------

    @Test
    void parse_minimal_request_generatesTraceId_setsDefaults() {
        HttpEnvelope.RequestHeaders parsed = HttpEnvelope.parse(fakeExchange(new Headers()));

        assertFalse(parsed.wantsJson());
        assertNull(parsed.userIdHeader());
        assertNull(parsed.maxStaleness());
        assertFalse(parsed.debug());
        assertNotNull(parsed.traceId());
        assertFalse(parsed.traceId().isBlank());
    }

    @Test
    void parse_jsonContentType_setsWantsJson() {
        Headers h = new Headers();
        h.add("Content-Type", "application/json; charset=utf-8");

        assertTrue(HttpEnvelope.parse(fakeExchange(h)).wantsJson());
    }

    @Test
    void parse_formContentType_doesNotSetWantsJson() {
        Headers h = new Headers();
        h.add("Content-Type", "application/x-www-form-urlencoded");

        assertFalse(HttpEnvelope.parse(fakeExchange(h)).wantsJson());
    }

    @Test
    void parse_xUserIdHeader_becomesUserIdHeader() {
        Headers h = new Headers();
        h.add("X-User-Id", "alice");

        assertEquals("alice", HttpEnvelope.parse(fakeExchange(h)).userIdHeader());
    }

    @Test
    void parse_xUserIdHeader_trimmed() {
        Headers h = new Headers();
        h.add("X-User-Id", "  alice  ");

        assertEquals("alice", HttpEnvelope.parse(fakeExchange(h)).userIdHeader());
    }

    @Test
    void parse_xUserIdHeader_emptyOrBlank_nullified() {
        Headers h = new Headers();
        h.add("X-User-Id", "   ");

        assertNull(HttpEnvelope.parse(fakeExchange(h)).userIdHeader());
    }

    @Test
    void parse_cacheControl_maxAge_becomesDuration() {
        Headers h = new Headers();
        h.add("Cache-Control", "max-age=300");

        assertEquals(Duration.ofSeconds(300), HttpEnvelope.parse(fakeExchange(h)).maxStaleness());
    }

    @Test
    void parse_debugHeaderTrue_setsFlag() {
        Headers h = new Headers();
        h.add("Debug", "true");

        assertTrue(HttpEnvelope.parse(fakeExchange(h)).debug());
    }

    @Test
    void parse_debugHeaderTrue_caseInsensitive() {
        Headers h = new Headers();
        h.add("Debug", "TRUE");

        assertTrue(HttpEnvelope.parse(fakeExchange(h)).debug());
    }

    @Test
    void parse_debugHeaderOtherValues_doesNotSetFlag() {
        for (String val : new String[]{"false", "1", "yes", ""}) {
            Headers h = new Headers();
            h.add("Debug", val);
            assertFalse(
                    HttpEnvelope.parse(fakeExchange(h)).debug(),
                    "Debug value '" + val + "' should not trip the flag");
        }
    }

    // ---------- parseCacheControlMaxAge() ----------

    @Test
    void parseCacheControlMaxAge_null_returnsNull() {
        assertNull(HttpEnvelope.parseCacheControlMaxAge(null));
    }

    @Test
    void parseCacheControlMaxAge_maxAgeOnly_parsesValue() {
        assertEquals(Duration.ofSeconds(60), HttpEnvelope.parseCacheControlMaxAge("max-age=60"));
    }

    @Test
    void parseCacheControlMaxAge_amongOtherDirectives() {
        assertEquals(
                Duration.ofSeconds(120),
                HttpEnvelope.parseCacheControlMaxAge("private, max-age=120, no-transform"));
    }

    @Test
    void parseCacheControlMaxAge_malformed_returnsNull() {
        assertNull(HttpEnvelope.parseCacheControlMaxAge("max-age=abc"));
    }

    @Test
    void parseCacheControlMaxAge_negative_returnsNull() {
        assertNull(HttpEnvelope.parseCacheControlMaxAge("max-age=-10"));
    }

    @Test
    void parseCacheControlMaxAge_noMaxAgeDirective_returnsNull() {
        assertNull(HttpEnvelope.parseCacheControlMaxAge("no-cache, no-store"));
    }

    // ---------- applyCacheStatusHeader() ----------

    @Test
    void applyCacheStatus_cacheHitTrue_emitsHIT() {
        Headers h = new Headers();
        JsonObject body = new JsonObject();
        body.addProperty("cacheHit", true);

        HttpEnvelope.applyCacheStatusHeader(h, body);

        assertEquals("HIT", h.getFirst("X-Cache-Status"));
    }

    @Test
    void applyCacheStatus_cacheHitFalse_emitsMISS() {
        Headers h = new Headers();
        JsonObject body = new JsonObject();
        body.addProperty("cacheHit", false);

        HttpEnvelope.applyCacheStatusHeader(h, body);

        assertEquals("MISS", h.getFirst("X-Cache-Status"));
    }

    @Test
    void applyCacheStatus_fieldAbsent_emitsNothing() {
        Headers h = new Headers();
        JsonObject body = new JsonObject();
        body.addProperty("ok", true);  // unrelated field

        HttpEnvelope.applyCacheStatusHeader(h, body);

        assertNull(h.getFirst("X-Cache-Status"));
    }

    // ---------- applyFreshnessHeader() ----------

    @Test
    void applyFreshness_fieldPresent_emitsHeader() {
        Headers h = new Headers();
        JsonObject body = new JsonObject();
        body.addProperty("freshness_ms", 3450);

        HttpEnvelope.applyFreshnessHeader(h, body);

        assertEquals("3450", h.getFirst("X-Freshness-Ms"));
    }

    @Test
    void applyFreshness_fieldNull_emitsNothing() {
        Headers h = new Headers();
        JsonObject body = new JsonObject();
        body.add("freshness_ms", com.google.gson.JsonNull.INSTANCE);

        HttpEnvelope.applyFreshnessHeader(h, body);

        assertNull(h.getFirst("X-Freshness-Ms"));
    }

    @Test
    void applyFreshness_fieldAbsent_emitsNothing() {
        Headers h = new Headers();
        JsonObject body = new JsonObject();
        body.addProperty("ok", true);

        HttpEnvelope.applyFreshnessHeader(h, body);

        assertNull(h.getFirst("X-Freshness-Ms"));
    }

    // ---------- applySuccessRateLimitStatusHeader() ----------

    @Test
    void applySuccessRateLimit_bodyOK_emitsOK() {
        Headers h = new Headers();
        JsonObject body = new JsonObject();
        body.addProperty("rate_limit_status", "OK");

        HttpEnvelope.applySuccessRateLimitStatusHeader(h, body);

        assertEquals("OK", h.getFirst("X-RateLimit-Status"));
    }

    @Test
    void applySuccessRateLimit_bodyExhausted_emitsExhausted() {
        Headers h = new Headers();
        JsonObject body = new JsonObject();
        body.addProperty("rate_limit_status", "EXHAUSTED");

        HttpEnvelope.applySuccessRateLimitStatusHeader(h, body);

        assertEquals("EXHAUSTED", h.getFirst("X-RateLimit-Status"));
    }

    @Test
    void applySuccessRateLimit_fieldAbsent_defaultsToOK() {
        Headers h = new Headers();
        JsonObject body = new JsonObject();
        body.addProperty("ok", true);

        HttpEnvelope.applySuccessRateLimitStatusHeader(h, body);

        assertEquals("OK", h.getFirst("X-RateLimit-Status"));
    }

    // ---------- applyFailureRateLimitHeaders() ----------

    @Test
    void applyFailureRateLimit_nonRateLimitFailure_statusOK_noScope() {
        Headers h = new Headers();
        ResponseContext.Outcome.Failure failure =
                new ResponseContext.Outcome.Failure(ErrorCode.BAD_QUERY, "bad sql", null, null);

        HttpEnvelope.applyFailureRateLimitHeaders(h, failure);

        assertEquals("OK", h.getFirst("X-RateLimit-Status"));
        assertNull(h.getFirst("X-RateLimit-Scope"));
    }

    @Test
    void applyFailureRateLimit_rateLimitWithScope_emitsExhaustedAndScope() {
        Headers h = new Headers();
        ResponseContext.Outcome.Failure failure =
                new ResponseContext.Outcome.Failure(
                        ErrorCode.RATE_LIMIT_EXHAUSTED, "throttled", 30000L, "USER");

        HttpEnvelope.applyFailureRateLimitHeaders(h, failure);

        assertEquals("EXHAUSTED", h.getFirst("X-RateLimit-Status"));
        assertEquals("USER", h.getFirst("X-RateLimit-Scope"));
    }

    @Test
    void applyFailureRateLimit_rateLimitWithoutScope_emitsExhaustedNoScope() {
        Headers h = new Headers();
        ResponseContext.Outcome.Failure failure =
                new ResponseContext.Outcome.Failure(
                        ErrorCode.RATE_LIMIT_EXHAUSTED, "throttled", 30000L, null);

        HttpEnvelope.applyFailureRateLimitHeaders(h, failure);

        assertEquals("EXHAUSTED", h.getFirst("X-RateLimit-Status"));
        assertNull(h.getFirst("X-RateLimit-Scope"));
    }

    // ---------- applyDebugBodyHeaders() ----------

    @Test
    void applyDebugBody_snapshotPath_emits() {
        Headers h = new Headers();
        JsonObject body = new JsonObject();
        body.addProperty("snapshotPath", "/tmp/snap/abc");

        HttpEnvelope.applyDebugBodyHeaders(h, body);

        assertEquals("/tmp/snap/abc", h.getFirst("X-Snapshot-Path"));
    }

    @Test
    void applyDebugBody_queryHash_emits() {
        Headers h = new Headers();
        JsonObject body = new JsonObject();
        body.addProperty("queryHash", "deadbeef");

        HttpEnvelope.applyDebugBodyHeaders(h, body);

        assertEquals("deadbeef", h.getFirst("X-Query-Hash"));
    }

    @Test
    void applyDebugBody_neitherField_emitsNothing() {
        Headers h = new Headers();
        JsonObject body = new JsonObject();
        body.addProperty("ok", true);

        HttpEnvelope.applyDebugBodyHeaders(h, body);

        assertNull(h.getFirst("X-Snapshot-Path"));
        assertNull(h.getFirst("X-Query-Hash"));
    }

    // ---------- applyDebugContextHeaders() ----------

    @Test
    void applyDebugContext_setsTenantAndRole() {
        Headers h = new Headers();
        RequestContext ctx = ctxWith("t1", "hr");

        HttpEnvelope.applyDebugContextHeaders(h, ctx);

        assertEquals("t1", h.getFirst("X-Tenant-Id"));
        assertEquals("hr", h.getFirst("X-Role"));
    }

    @Test
    void applyDebugContext_nullTenant_skipsTenantHeader() {
        Headers h = new Headers();
        RequestContext ctx = ctxWith(null, "hr");

        HttpEnvelope.applyDebugContextHeaders(h, ctx);

        assertNull(h.getFirst("X-Tenant-Id"));
        assertEquals("hr", h.getFirst("X-Role"));
    }

    @Test
    void applyDebugContext_nullCtx_noOp() {
        Headers h = new Headers();

        HttpEnvelope.applyDebugContextHeaders(h, null);

        assertNull(h.getFirst("X-Tenant-Id"));
        assertNull(h.getFirst("X-Role"));
    }

    // ---------- applySuccessHeaders() (integration of the above) ----------

    @Test
    void applySuccessHeaders_alwaysSetsTraceIdCacheStatusFreshnessAndRateLimitStatus() {
        Headers h = new Headers();
        HttpEnvelope.RequestHeaders reqHeaders =
                new HttpEnvelope.RequestHeaders(true, null, null, false, "trace-123");
        JsonObject body = new JsonObject();
        body.addProperty("cacheHit", true);
        body.addProperty("freshness_ms", 3450);
        body.addProperty("rate_limit_status", "OK");

        HttpEnvelope.applySuccessHeaders(h, reqHeaders, ctxWith("t1", "hr"), body);

        assertEquals("trace-123", h.getFirst("X-Trace-Id"));
        assertEquals("HIT", h.getFirst("X-Cache-Status"));
        assertEquals("3450", h.getFirst("X-Freshness-Ms"));
        assertEquals("OK", h.getFirst("X-RateLimit-Status"));
        // Debug=false, so these are not emitted
        assertNull(h.getFirst("X-Snapshot-Path"));
        assertNull(h.getFirst("X-Tenant-Id"));
        assertNull(h.getFirst("X-Role"));
    }

    @Test
    void applySuccessHeaders_debugTrue_addsAllDebugHeaders() {
        Headers h = new Headers();
        HttpEnvelope.RequestHeaders reqHeaders =
                new HttpEnvelope.RequestHeaders(true, null, null, true, "trace-456");
        JsonObject body = new JsonObject();
        body.addProperty("cacheHit", false);
        body.addProperty("freshness_ms", 12);
        body.addProperty("rate_limit_status", "OK");
        body.addProperty("snapshotPath", "/tmp/snap/abc");
        body.addProperty("queryHash", "deadbeef");

        HttpEnvelope.applySuccessHeaders(h, reqHeaders, ctxWith("t1", "hr"), body);

        assertEquals("trace-456", h.getFirst("X-Trace-Id"));
        assertEquals("MISS", h.getFirst("X-Cache-Status"));
        assertEquals("12", h.getFirst("X-Freshness-Ms"));
        assertEquals("OK", h.getFirst("X-RateLimit-Status"));
        assertEquals("/tmp/snap/abc", h.getFirst("X-Snapshot-Path"));
        assertEquals("deadbeef", h.getFirst("X-Query-Hash"));
        assertEquals("t1", h.getFirst("X-Tenant-Id"));
        assertEquals("hr", h.getFirst("X-Role"));
    }

    // ---------- helpers ----------

    private static RequestContext ctxWith(String tenantId, String roleSlug) {
        UserContext user = new UserContext("test-user");
        QueryCacheScope scope =
                new QueryCacheScope(
                        "test-user",
                        tenantId != null ? tenantId : "_anon",
                        roleSlug,
                        QueryCacheScope.CURRENT_KEY_SCHEMA);
        return new RequestContext("trace-test", user, scope, tenantId, Instant.now());
    }

    private static HttpExchange fakeExchange(Headers requestHeaders) {
        return new MinimalHttpExchange(requestHeaders);
    }

    /** Bare HttpExchange stub — only getRequestHeaders() is real; rest throws or returns null. */
    private static final class MinimalHttpExchange extends HttpExchange {
        private final Headers requestHeaders;

        MinimalHttpExchange(Headers requestHeaders) {
            this.requestHeaders = requestHeaders;
        }

        @Override public Headers getRequestHeaders() { return requestHeaders; }
        @Override public Headers getResponseHeaders() { throw new UnsupportedOperationException(); }
        @Override public URI getRequestURI() { throw new UnsupportedOperationException(); }
        @Override public String getRequestMethod() { return "POST"; }
        @Override public HttpContext getHttpContext() { throw new UnsupportedOperationException(); }
        @Override public void close() {}
        @Override public InputStream getRequestBody() { throw new UnsupportedOperationException(); }
        @Override public OutputStream getResponseBody() { throw new UnsupportedOperationException(); }
        @Override public void sendResponseHeaders(int rCode, long responseLength) {}
        @Override public InetSocketAddress getRemoteAddress() { throw new UnsupportedOperationException(); }
        @Override public int getResponseCode() { return 0; }
        @Override public InetSocketAddress getLocalAddress() { throw new UnsupportedOperationException(); }
        @Override public String getProtocol() { return "HTTP/1.1"; }
        @Override public Object getAttribute(String name) { return null; }
        @Override public void setAttribute(String name, Object value) {}
        @Override public void setStreams(InputStream i, OutputStream o) {}
        @Override public HttpPrincipal getPrincipal() { return null; }
    }
}
