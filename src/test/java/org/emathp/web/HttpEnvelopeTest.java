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
import org.emathp.query.DebugResponseContext;
import org.emathp.query.ErrorCode;
import org.emathp.query.ResponseContext;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link HttpEnvelope}. Covers:
 *
 * <ul>
 *   <li>{@code parse()} — request header reads (Content-Type, X-User-Id, Cache-Control, Debug)
 *   <li>{@code parseCacheControlMaxAge()} — the standalone Cache-Control parser
 *   <li>Header-applying helpers — what gets written into the response Headers map, all sourced
 *       from {@link ResponseContext} (cache status, freshness, rate-limit status, debug fields)
 *   <li>{@code applySuccessHeaders()} integration — all helpers together with a full
 *       {@link ResponseContext} fixture
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
    void applyCacheStatus_cacheStatusHit_emitsHIT() {
        Headers h = new Headers();
        ResponseContext rc = successRc("HIT", null, "OK", null);

        HttpEnvelope.applyCacheStatusHeader(h, rc);

        assertEquals("HIT", h.getFirst("X-Cache-Status"));
    }

    @Test
    void applyCacheStatus_cacheStatusMiss_emitsMISS() {
        Headers h = new Headers();
        ResponseContext rc = successRc("MISS", null, "OK", null);

        HttpEnvelope.applyCacheStatusHeader(h, rc);

        assertEquals("MISS", h.getFirst("X-Cache-Status"));
    }

    @Test
    void applyCacheStatus_cacheStatusNull_emitsNothing() {
        Headers h = new Headers();
        ResponseContext rc = successRc(null, null, "OK", null);

        HttpEnvelope.applyCacheStatusHeader(h, rc);

        assertNull(h.getFirst("X-Cache-Status"));
    }

    @Test
    void applyCacheStatus_rcNull_emitsNothing() {
        Headers h = new Headers();

        HttpEnvelope.applyCacheStatusHeader(h, null);

        assertNull(h.getFirst("X-Cache-Status"));
    }

    // ---------- applyFreshnessHeader() ----------

    @Test
    void applyFreshness_fieldPresent_emitsHeader() {
        Headers h = new Headers();
        ResponseContext rc = successRc("HIT", 3450L, "OK", null);

        HttpEnvelope.applyFreshnessHeader(h, rc);

        assertEquals("3450", h.getFirst("X-Freshness-Ms"));
    }

    @Test
    void applyFreshness_fieldNull_emitsNothing() {
        Headers h = new Headers();
        ResponseContext rc = successRc("HIT", null, "OK", null);

        HttpEnvelope.applyFreshnessHeader(h, rc);

        assertNull(h.getFirst("X-Freshness-Ms"));
    }

    @Test
    void applyFreshness_rcNull_emitsNothing() {
        Headers h = new Headers();

        HttpEnvelope.applyFreshnessHeader(h, null);

        assertNull(h.getFirst("X-Freshness-Ms"));
    }

    // ---------- applyRateLimitStatusHeader() ----------

    @Test
    void applyRateLimitStatus_okValue_emitsOK() {
        Headers h = new Headers();
        HttpEnvelope.applyRateLimitStatusHeader(h, "OK");
        assertEquals("OK", h.getFirst("X-RateLimit-Status"));
    }

    @Test
    void applyRateLimitStatus_exhaustedValue_emitsExhausted() {
        Headers h = new Headers();
        HttpEnvelope.applyRateLimitStatusHeader(h, "EXHAUSTED");
        assertEquals("EXHAUSTED", h.getFirst("X-RateLimit-Status"));
    }

    @Test
    void applyRateLimitStatus_nullValue_defaultsToOK() {
        Headers h = new Headers();
        HttpEnvelope.applyRateLimitStatusHeader(h, null);
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

    // ---------- applyDebugHeaders() ----------

    @Test
    void applyDebug_allFieldsSet_emitsAll() {
        Headers h = new Headers();
        DebugResponseContext debug =
                new DebugResponseContext("/tmp/snap/abc", "deadbeef", "t1", "hr");

        HttpEnvelope.applyDebugHeaders(h, debug);

        assertEquals("/tmp/snap/abc", h.getFirst("X-Snapshot-Path"));
        assertEquals("deadbeef", h.getFirst("X-Query-Hash"));
        assertEquals("t1", h.getFirst("X-Tenant-Id"));
        assertEquals("hr", h.getFirst("X-Role"));
    }

    @Test
    void applyDebug_partialFields_emitsOnlyPresent() {
        Headers h = new Headers();
        DebugResponseContext debug =
                new DebugResponseContext("/tmp/snap/abc", null, "t1", null);

        HttpEnvelope.applyDebugHeaders(h, debug);

        assertEquals("/tmp/snap/abc", h.getFirst("X-Snapshot-Path"));
        assertNull(h.getFirst("X-Query-Hash"));
        assertEquals("t1", h.getFirst("X-Tenant-Id"));
        assertNull(h.getFirst("X-Role"));
    }

    @Test
    void applyDebug_blankTenantOrRole_skipped() {
        Headers h = new Headers();
        DebugResponseContext debug =
                new DebugResponseContext(null, null, "  ", "");

        HttpEnvelope.applyDebugHeaders(h, debug);

        assertNull(h.getFirst("X-Tenant-Id"));
        assertNull(h.getFirst("X-Role"));
    }

    @Test
    void applyDebug_nullDebug_noOp() {
        Headers h = new Headers();

        HttpEnvelope.applyDebugHeaders(h, null);

        assertNull(h.getFirst("X-Snapshot-Path"));
        assertNull(h.getFirst("X-Query-Hash"));
        assertNull(h.getFirst("X-Tenant-Id"));
        assertNull(h.getFirst("X-Role"));
    }

    // ---------- applySuccessHeaders() (integration of the above) ----------

    @Test
    void applySuccessHeaders_debugFalse_setsCoreOnlyNoDebugHeaders() {
        Headers h = new Headers();
        HttpEnvelope.RequestHeaders reqHeaders =
                new HttpEnvelope.RequestHeaders(true, null, null, false, "trace-123");
        ResponseContext rc =
                successRc(
                        "HIT",
                        3450L,
                        "OK",
                        new DebugResponseContext("/tmp/snap/abc", "deadbeef", "t1", "hr"));

        HttpEnvelope.applySuccessHeaders(h, reqHeaders, rc);

        assertEquals("trace-123", h.getFirst("X-Trace-Id"));
        assertEquals("HIT", h.getFirst("X-Cache-Status"));
        assertEquals("3450", h.getFirst("X-Freshness-Ms"));
        assertEquals("OK", h.getFirst("X-RateLimit-Status"));
        // Debug=false in request → no debug headers even though rc.debug() is non-null.
        assertNull(h.getFirst("X-Snapshot-Path"));
        assertNull(h.getFirst("X-Query-Hash"));
        assertNull(h.getFirst("X-Tenant-Id"));
        assertNull(h.getFirst("X-Role"));
    }

    @Test
    void applySuccessHeaders_debugTrue_emitsDebugHeadersToo() {
        Headers h = new Headers();
        HttpEnvelope.RequestHeaders reqHeaders =
                new HttpEnvelope.RequestHeaders(true, null, null, true, "trace-456");
        ResponseContext rc =
                successRc(
                        "MISS",
                        12L,
                        "OK",
                        new DebugResponseContext("/tmp/snap/abc", "deadbeef", "t1", "hr"));

        HttpEnvelope.applySuccessHeaders(h, reqHeaders, rc);

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

    /** Builds a success ResponseContext fixture with given header fields. */
    private static ResponseContext successRc(
            String cacheStatus, Long freshnessMs, String rateLimitStatus, DebugResponseContext debug) {
        return new ResponseContext(
                "trace-fixture",
                10L,
                freshnessMs,
                rateLimitStatus,
                cacheStatus,
                debug,
                new ResponseContext.Outcome.Success(new JsonObject()));
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
