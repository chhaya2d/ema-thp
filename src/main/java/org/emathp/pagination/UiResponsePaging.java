package org.emathp.pagination;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import org.emathp.config.WebDefaults;

/**
 * HTTP/demo UI offset window over serialized JSON bodies. Provider / engine cursors remain on the
 * payload; these helpers only shrink visible row arrays for the browser.
 */
public final class UiResponsePaging {

    private UiResponsePaging() {}

    public static int parseUiOffset(String uiCursorOffset) {
        if (uiCursorOffset == null || uiCursorOffset.isBlank()) {
            return 0;
        }
        String t = uiCursorOffset.trim();
        try {
            int v = Integer.parseInt(t, 10);
            return Math.max(0, v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "ui cursor must be a non-negative decimal integer offset, got: " + uiCursorOffset);
        }
    }

    /** Slices {@code sides[*].execution.rows} for each connector column. */
    public static void applyToSingleConnectorSides(JsonObject root, int uiOffset, int uiPageSize) {
        String kind = root.get("kind").getAsString();
        if (!"single".equals(kind)) {
            root.addProperty("uiPagingSupported", false);
            return;
        }
        root.addProperty("uiPagingSupported", true);
        JsonArray sides = root.getAsJsonArray("sides");
        int maxTotal = 0;
        boolean anyNext = false;
        JsonObject totals = new JsonObject();
        for (JsonElement el : sides) {
            JsonObject side = el.getAsJsonObject();
            JsonObject exec = side.getAsJsonObject("execution");
            JsonArray rows = exec.getAsJsonArray("rows");
            int n = rows.size();
            maxTotal = Math.max(maxTotal, n);
            String conn = side.get("connector").getAsString();
            totals.addProperty(conn, n);
            int from = Math.min(uiOffset, n);
            int to = Math.min(uiOffset + uiPageSize, n);
            JsonArray window = new JsonArray();
            for (int i = from; i < to; i++) {
                window.add(rows.get(i));
            }
            exec.remove("rows");
            exec.add("rows", window);
            exec.addProperty("uiRowTotal", n);
            if (uiOffset + uiPageSize < n) {
                anyNext = true;
            }
        }
        root.addProperty("uiOffset", uiOffset);
        root.addProperty("uiPageSize", uiPageSize);
        root.addProperty("uiPageNumber", uiPageSize > 0 ? uiOffset / uiPageSize : 0);
        root.add("uiRowTotalsByConnector", totals);
        int pageIndex = uiPageSize > 0 ? uiOffset / uiPageSize + 1 : 1;
        root.addProperty("uiPageIndex", pageIndex);
        int approxPages = uiPageSize > 0 ? Math.max(1, (maxTotal + uiPageSize - 1) / uiPageSize) : 1;
        root.addProperty("uiApproxPageCount", approxPages);
        if (anyNext) {
            root.addProperty("uiNextCursor", Integer.toString(uiOffset + uiPageSize));
        } else {
            root.add("uiNextCursor", JsonNull.INSTANCE);
        }
        if (uiOffset > 0) {
            root.addProperty("uiPrevCursor", Integer.toString(Math.max(0, uiOffset - uiPageSize)));
        } else {
            root.add("uiPrevCursor", JsonNull.INSTANCE);
        }
        root.addProperty(
                "uiPagingTradeoffNote",
                "Snapshot layer tries disk first (freshness); on miss it runs QueryExecutor once and caches. "
                        + "UI paging (pageNumber/pageSize) is separate from provider cursors.");
    }

    /** Slices {@code pages[0].<rowsMember>} (row array lives on first page block). */
    public static void applyToFirstPageRows(JsonObject root, String rowsMember, int uiOffset, int uiPageSize) {
        if (!root.has("pages") || root.getAsJsonArray("pages").isEmpty()) {
            root.addProperty("uiPagingSupported", false);
            return;
        }
        root.addProperty("uiPagingSupported", true);
        JsonObject pg = root.getAsJsonArray("pages").get(0).getAsJsonObject();
        JsonArray rows = pg.getAsJsonArray(rowsMember);
        int n = rows.size();
        int from = Math.min(uiOffset, n);
        int to = Math.min(uiOffset + uiPageSize, n);
        JsonArray window = new JsonArray();
        for (int i = from; i < to; i++) {
            window.add(rows.get(i));
        }
        pg.remove(rowsMember);
        pg.add(rowsMember, window);
        pg.addProperty("uiRowTotal", n);

        root.addProperty("uiOffset", uiOffset);
        root.addProperty("uiPageSize", uiPageSize);
        root.addProperty("uiPageNumber", uiPageSize > 0 ? uiOffset / uiPageSize : 0);
        root.addProperty("uiPageIndex", uiPageSize > 0 ? uiOffset / uiPageSize + 1 : 1);
        int approxPages = uiPageSize > 0 ? Math.max(1, (n + uiPageSize - 1) / uiPageSize) : 1;
        root.addProperty("uiApproxPageCount", approxPages);
        if (uiOffset + uiPageSize < n) {
            root.addProperty("uiNextCursor", Integer.toString(uiOffset + uiPageSize));
        } else {
            root.add("uiNextCursor", JsonNull.INSTANCE);
        }
        if (uiOffset > 0) {
            root.addProperty("uiPrevCursor", Integer.toString(Math.max(0, uiOffset - uiPageSize)));
        } else {
            root.add("uiPrevCursor", JsonNull.INSTANCE);
        }
        root.addProperty(
                "uiPagingTradeoffNote",
                "UI paging shrinks serialized row arrays only; federation cursors on the payload are unchanged.");
    }

    public static int clampClientPageSize(int requestPageSize) {
        return Math.min(requestPageSize, WebDefaults.UI_QUERY_PAGE_SIZE_CLIENT_MAX);
    }
}
