package org.emathp.web;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * Append-only trace file for web {@code /api/query} responses: planner summaries already embedded in
 * JSON are mirrored here in a readable form. Default path: {@code logs/web-query-trace.log}
 * (under the process working directory).
 */
public final class WebQueryTraceLogger {

    private static final Path DEFAULT_LOG = Path.of("logs", "web-query-trace.log");
    private static final Object LOCK = new Object();

    private WebQueryTraceLogger() {}

    public static void appendTrace(String connectorMode, String sqlPreview, JsonObject result) {
        try {
            synchronized (LOCK) {
                Files.createDirectories(DEFAULT_LOG.getParent());
                StringBuilder sb = new StringBuilder();
                sb.append(Instant.now())
                        .append(" | connectorMode=")
                        .append(connectorMode)
                        .append(" | ok=")
                        .append(okString(result))
                        .append('\n');
                if (sqlPreview != null && !sqlPreview.isBlank()) {
                    String oneLine = sqlPreview.replace("\r", " ").replace("\n", " ").trim();
                    if (oneLine.length() > 400) {
                        oneLine = oneLine.substring(0, 400) + "…";
                    }
                    sb.append("  sql: ").append(oneLine).append('\n');
                }
                appendSnapshotHeader(sb, result);
                String kind = kind(result);
                if ("join".equals(kind)) {
                    appendJoinSummary(sb, result);
                } else if ("single".equals(kind)) {
                    appendSingleSides(sb, result);
                }
                sb.append("----\n");
                Files.writeString(
                        DEFAULT_LOG,
                        sb.toString(),
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND);
            }
        } catch (IOException e) {
            System.err.println("WebQueryTraceLogger: " + e.getMessage());
        }
    }

    private static String okString(JsonObject result) {
        if (!result.has("ok") || result.get("ok").isJsonNull()) {
            return "?";
        }
        var ok = result.get("ok");
        if (ok.isJsonPrimitive() && ok.getAsJsonPrimitive().isBoolean()) {
            return String.valueOf(ok.getAsBoolean());
        }
        return ok.toString();
    }

    private static String kind(JsonObject result) {
        if (result.has("kind") && !result.get("kind").isJsonNull()) {
            return result.get("kind").getAsString();
        }
        return "";
    }

    private static void appendSnapshotHeader(StringBuilder sb, JsonObject result) {
        put(sb, "  queryHash", result, "queryHash");
        put(sb, "  snapshotPath", result, "snapshotPath");
        put(sb, "  freshnessDecision", result, "freshnessDecision");
        put(sb, "  snapshotBacked", result, "snapshotBacked");
        if (result.has("fullMaterializationReuse") && !result.get("fullMaterializationReuse").isJsonNull()) {
            sb.append("  fullMaterializationReuse=").append(result.get("fullMaterializationReuse").getAsString()).append('\n');
        }
    }

    private static void appendJoinSummary(StringBuilder sb, JsonObject result) {
        if (!result.has("pages") || result.get("pages").isJsonNull()) {
            return;
        }
        JsonArray pages = result.getAsJsonArray("pages");
        if (pages.isEmpty()) {
            return;
        }
        JsonObject p0 = pages.get(0).getAsJsonObject();
        put(sb, "  join upstreamRowCount", p0, "upstreamRowCount");
        put(sb, "  join stoppedAtLimit", p0, "stoppedAtLimit");
        put(sb, "  join nextCursor", p0, "nextCursor");
    }

    private static void appendSingleSides(StringBuilder sb, JsonObject result) {
        if (!result.has("sides") || !result.get("sides").isJsonArray()) {
            return;
        }
        for (JsonElement el : result.getAsJsonArray("sides")) {
            if (!el.isJsonObject()) {
                continue;
            }
            JsonObject side = el.getAsJsonObject();
            String connector = side.has("connector") ? side.get("connector").getAsString() : "?";
            sb.append("  --- side: ").append(connector).append('\n');
            put(sb, "    pushedSummary", side, "pushedSummary");
            put(sb, "    pending (not pushed)", side, "pending");
            put(sb, "    residual", side, "residual");
            put(sb, "    snapshotReuseNoProviderCall", side, "snapshotReuseNoProviderCall");
            put(sb, "    providerFetchesThisRequest", side, "providerFetchesThisRequest");
            put(sb, "    continuationFetchesThisRequest", side, "continuationFetchesThisRequest");
            put(sb, "    connectorSnapshotDir", side, "connectorSnapshotDir");
            if (side.has("execution") && side.get("execution").isJsonObject()) {
                JsonObject ex = side.getAsJsonObject("execution");
                put(sb, "    rowsFromConnector", ex, "rowsFromConnector");
                put(sb, "    residualApplied", ex, "residualApplied");
                put(sb, "    finalNextCursor", ex, "finalNextCursor");
                if (ex.has("calls") && ex.get("calls").isJsonArray()) {
                    int i = 0;
                    for (JsonElement c : ex.getAsJsonArray("calls")) {
                        if (c.isJsonObject()) {
                            JsonObject call = c.getAsJsonObject();
                            sb.append("    fetch[")
                                    .append(i++)
                                    .append("] cursor=")
                                    .append(str(call, "cursor"))
                                    .append(" rowsReturned=")
                                    .append(str(call, "rowsReturned"))
                                    .append(" nextCursor=")
                                    .append(str(call, "nextCursor"))
                                    .append('\n');
                        }
                    }
                }
            }
            if (side.has("chunkFilesCreatedThisRequest")
                    && side.get("chunkFilesCreatedThisRequest").isJsonArray()) {
                JsonArray chunks = side.getAsJsonArray("chunkFilesCreatedThisRequest");
                if (!chunks.isEmpty()) {
                    sb.append("    chunkFilesCreatedThisRequest: ");
                    for (JsonElement ch : chunks) {
                        sb.append(ch.getAsString()).append("; ");
                    }
                    sb.append('\n');
                }
            }
        }
    }

    private static void put(StringBuilder sb, String label, JsonObject o, String key) {
        if (!o.has(key) || o.get(key).isJsonNull()) {
            return;
        }
        JsonElement e = o.get(key);
        sb.append(label).append("=").append(stringifyElement(e)).append('\n');
    }

    private static String str(JsonObject o, String key) {
        if (!o.has(key) || o.get(key).isJsonNull()) {
            return "";
        }
        return stringifyElement(o.get(key));
    }

    private static String stringifyElement(JsonElement e) {
        if (e.isJsonNull()) {
            return "null";
        }
        if (e.isJsonPrimitive()) {
            return e.getAsString();
        }
        return e.toString();
    }
}
