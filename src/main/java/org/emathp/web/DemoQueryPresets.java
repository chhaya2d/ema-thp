package org.emathp.web;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Built-in SQL for the browser demo: Drive-oriented presets use {@code FROM google}, Notion-oriented
 * ones use {@code FROM notion}, so only that connector runs; {@code d_g_3} is a join; {@code live_1}
 * uses {@code FROM resources} to exercise both connectors in the live stack.
 */
public final class DemoQueryPresets {

    private DemoQueryPresets() {}

    public static final String D_G_1 =
            """
            SELECT title, updatedAt
            FROM google
            WHERE updatedAt > '2026-01-01'
            ORDER BY updatedAt DESC
            LIMIT 100
            """
                    .strip();

    public static final String D_G_2 =
            """
            SELECT title, updatedAt
            FROM google
            WHERE title LIKE '%JoinKey%'
            ORDER BY updatedAt DESC
            LIMIT 100
            """
                    .strip();

    public static final String D_G_3 =
            """
            SELECT g.title AS drive_title, n.title AS notion_title, g.updatedAt AS drive_updated
            FROM google g
            JOIN notion n ON g.title = n.title
            WHERE g.updatedAt > '2026-01-01'
            ORDER BY g.updatedAt DESC
            LIMIT 50
            """
                    .strip();

    public static final String D_N_1 =
            """
            SELECT title, updatedAt
            FROM notion
            WHERE updatedAt > '2026-01-01'
            ORDER BY updatedAt DESC
            LIMIT 100
            """
                    .strip();

    public static final String D_N_2 =
            """
            SELECT title, updatedAt
            FROM notion
            WHERE title LIKE '%Extra%'
            ORDER BY updatedAt DESC
            LIMIT 100
            """
                    .strip();

    public static final String D_N_3 =
            """
            SELECT title, updatedAt
            FROM notion
            WHERE updatedAt > '2026-04-01'
            ORDER BY title ASC
            LIMIT 100
            """
                    .strip();

    /** Live Google + Notion (mock Notion in stack): classic date filter. */
    public static final String LIVE_1 =
            """
            SELECT title, updatedAt
            FROM resources
            WHERE updatedAt > '2020-01-01'
            ORDER BY updatedAt DESC
            LIMIT 50
            """
                    .strip();

    /** Keys match {@code <option value>} in the playground. */
    public static Map<String, String> presetMap() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("d_g_1", D_G_1);
        m.put("d_g_2", D_G_2);
        m.put("d_g_3", D_G_3);
        m.put("d_n_1", D_N_1);
        m.put("d_n_2", D_N_2);
        m.put("d_n_3", D_N_3);
        m.put("live_1", LIVE_1);
        return m;
    }
}
