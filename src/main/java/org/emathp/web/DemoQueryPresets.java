package org.emathp.web;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Built-in SQL for the demo web playground. Single-source queries use {@code FROM resources} and
 * run against every registered connector (compare {@code google-drive} vs {@code notion} sides in
 * JSON). Pagination is driven by the UI page size, not SQL {@code LIMIT} (LIMIT here only caps
 * engine materialization).
 */
public final class DemoQueryPresets {

    private DemoQueryPresets() {}

    /**
     * Same SQL for both “Google” and “Notion” presets — labels indicate which JSON side to inspect
     * (Drive pushes ORDER BY; Notion applies ORDER BY in the engine).
     */
    public static final String SINGLE_SOURCE_ORDER_BY =
            """
            SELECT title, updatedAt
            FROM resources
            WHERE updatedAt > '2026-01-01'
            ORDER BY updatedAt DESC
            LIMIT 100
            """
                    .strip();

    public static final String JOIN_ON_TITLE =
            """
            SELECT g.title AS drive_title, n.title AS notion_title, g.updatedAt AS drive_updated
            FROM google g
            JOIN notion n ON g.title = n.title
            WHERE g.updatedAt > '2026-01-01'
            ORDER BY g.updatedAt DESC
            LIMIT 50
            """
                    .strip();

    /** Keys match {@code <option value>} in the playground. */
    public static Map<String, String> presetMap() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("singleGoogle", SINGLE_SOURCE_ORDER_BY);
        m.put("singleNotion", SINGLE_SOURCE_ORDER_BY);
        m.put("join", JOIN_ON_TITLE);
        return m;
    }
}
