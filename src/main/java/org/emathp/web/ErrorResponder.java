package org.emathp.web;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.emathp.query.ResponseContext;

/**
 * Maps {@link ResponseContext.Outcome.Failure} to an HTTP response: status code from {@link
 * org.emathp.query.ErrorCode#httpStatus()}, {@code Retry-After} header when the failure carries a
 * retry hint, and a uniform JSON envelope. Centralizing here means every error path emits the
 * same shape — clients can branch on {@code code} rather than parsing free-form messages.
 */
final class ErrorResponder {

    private static final Gson GSON = new Gson();

    private ErrorResponder() {}

    /** Writes an HTTP response derived from {@code failure}. Always closes the exchange. */
    static void writeFailure(
            HttpExchange ex, boolean wantsJson, String traceId, ResponseContext.Outcome.Failure failure)
            throws IOException {
        int status = failure.code().httpStatus();
        if (failure.retryAfterMs() != null) {
            long seconds = Math.max(1L, (failure.retryAfterMs() + 999L) / 1000L);
            ex.getResponseHeaders().add("Retry-After", String.valueOf(seconds));
        }
        ex.getResponseHeaders().add("X-Trace-Id", traceId);
        JsonObject envelope = envelopeJson(traceId, failure);
        if (wantsJson) {
            byte[] body = GSON.toJson(envelope).getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            ex.sendResponseHeaders(status, body.length);
            ex.getResponseBody().write(body);
        } else {
            String html =
                    "<!DOCTYPE html><html><body><h2>"
                            + failure.code().name()
                            + "</h2><pre>"
                            + esc(GSON.toJson(envelope))
                            + "</pre></body></html>";
            byte[] body = html.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            ex.sendResponseHeaders(status, body.length);
            ex.getResponseBody().write(body);
        }
        ex.close();
    }

    static JsonObject envelopeJson(String traceId, ResponseContext.Outcome.Failure failure) {
        JsonObject o = new JsonObject();
        o.addProperty("ok", false);
        o.addProperty("code", failure.code().name());
        o.addProperty("message", failure.message());
        o.addProperty("traceId", traceId);
        o.addProperty(
                "rate_limit_status",
                failure.code() == org.emathp.query.ErrorCode.RATE_LIMIT_EXHAUSTED
                        ? "EXHAUSTED"
                        : "OK");
        if (failure.retryAfterMs() != null) {
            o.addProperty("retryAfterMs", failure.retryAfterMs());
        }
        if (failure.violatedScope() != null) {
            o.addProperty("violatedScope", failure.violatedScope());
        }
        return o;
    }

    private static String esc(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}
