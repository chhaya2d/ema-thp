package org.emathp.web;

import com.google.gson.JsonObject;
import java.time.Duration;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.emathp.query.FederatedQueryRequest;

/**
 * HTTP-layer extraction of query-payload parameters. Identity / scope is built separately by the
 * server into {@link org.emathp.query.RequestContext}, so this record carries only the
 * "what data, sliced how" fields.
 */
public record HttpQueryPayload(
        String sql,
        Integer pageNumber,
        String logicalCursorOffset,
        Integer requestPageSize,
        Duration maxStaleness) {

    private static final Pattern SHORTHAND_DURATION =
            Pattern.compile("^(\\d+)\\s*(ms|s|sec|secs|m|min|mins|h|hr|hrs)$");

    public FederatedQueryRequest toRequest() {
        return new FederatedQueryRequest(sql, pageNumber, logicalCursorOffset, requestPageSize, maxStaleness);
    }

    public static HttpQueryPayload parseJson(JsonObject o) {
        String sql = o.has("sql") && !o.get("sql").isJsonNull() ? o.get("sql").getAsString() : null;
        Integer pageNumber = null;
        if (o.has("pageNumber") && !o.get("pageNumber").isJsonNull()) {
            pageNumber = o.get("pageNumber").getAsInt();
        }
        String cursor = null;
        if (o.has("cursor") && !o.get("cursor").isJsonNull()) {
            cursor = o.get("cursor").getAsString();
        }
        Integer pageSizeReq = null;
        if (o.has("pageSize") && !o.get("pageSize").isJsonNull()) {
            pageSizeReq = o.get("pageSize").getAsInt();
        }
        Duration maxStaleness = null;
        if (o.has("maxStaleness") && !o.get("maxStaleness").isJsonNull()) {
            maxStaleness = parseHumanDuration(o.get("maxStaleness").getAsString());
        }
        return new HttpQueryPayload(sql, pageNumber, cursor, pageSizeReq, maxStaleness);
    }

    public static HttpQueryPayload parseForm(Map<String, String> form) {
        String sql = form.get("sql");
        String cursor = form.get("cursor");
        Integer pageNumber = null;
        String pn = form.get("pageNumber");
        if (pn != null && !pn.isBlank()) {
            pageNumber = Integer.parseInt(pn.trim());
        }
        Integer pageSizeReq = null;
        String psForm = form.get("pageSize");
        if (psForm != null && !psForm.isBlank()) {
            pageSizeReq = Integer.parseInt(psForm.trim());
        }
        Duration maxStaleness = null;
        String ms = form.get("maxStaleness");
        if (ms != null && !ms.isBlank()) {
            maxStaleness = parseHumanDuration(ms);
        }
        return new HttpQueryPayload(sql, pageNumber, cursor, pageSizeReq, maxStaleness);
    }

    /**
     * Accepts both ISO-8601 ({@code PT10M}, {@code PT30S}) and shorthand ({@code 10m},
     * {@code 30s}, {@code 1h}, {@code 500ms}). Returns {@code null} on blank input; throws
     * {@link IllegalArgumentException} on anything else that does not match.
     */
    public static Duration parseHumanDuration(String raw) {
        if (raw == null) return null;
        String s = raw.trim();
        if (s.isEmpty()) return null;
        // ISO-8601 first: "PT...", "P...", "-PT..."
        if (s.startsWith("P") || s.startsWith("p") || s.startsWith("-P") || s.startsWith("-p")) {
            try {
                return Duration.parse(s.toUpperCase().replace("p", "P"));
            } catch (RuntimeException ignored) {
                // fall through to shorthand
            }
        }
        Matcher m = SHORTHAND_DURATION.matcher(s.toLowerCase());
        if (!m.matches()) {
            throw new IllegalArgumentException(
                    "Cannot parse duration '" + raw + "'. Use shorthand (5m, 30s, 1h, 500ms) or ISO (PT5M).");
        }
        long n = Long.parseLong(m.group(1));
        String unit = m.group(2);
        return switch (unit) {
            case "ms" -> Duration.ofMillis(n);
            case "s", "sec", "secs" -> Duration.ofSeconds(n);
            case "m", "min", "mins" -> Duration.ofMinutes(n);
            case "h", "hr", "hrs" -> Duration.ofHours(n);
            default -> throw new IllegalArgumentException("Unknown duration unit: " + unit);
        };
    }
}
