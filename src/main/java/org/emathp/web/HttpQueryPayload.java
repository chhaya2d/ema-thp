package org.emathp.web;

import com.google.gson.JsonObject;
import java.time.Duration;
import java.util.Map;
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
            maxStaleness = Duration.parse(o.get("maxStaleness").getAsString().trim());
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
            maxStaleness = Duration.parse(ms.trim());
        }
        return new HttpQueryPayload(sql, pageNumber, cursor, pageSizeReq, maxStaleness);
    }
}
