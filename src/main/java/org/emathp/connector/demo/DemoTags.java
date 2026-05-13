package org.emathp.connector.demo;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Fixture tags for demo Drive / Notion rows (visible to {@link org.emathp.engine.policy.TagRowFilter}). */
public final class DemoTags {

    private static final Map<String, List<String>> BY_DRIVE_ID =
            Map.ofEntries(
                    Map.entry("demo-g-a1", List.of("shared", "finance")),
                    Map.entry("demo-g-a2", List.of("shared", "finance")),
                    Map.entry("demo-g-a3", List.of("shared", "engineering")),
                    Map.entry("demo-g-a4", List.of("finance")),
                    Map.entry("demo-g-b1", List.of("shared", "engineering")),
                    Map.entry("demo-g-b2", List.of("shared", "engineering")),
                    Map.entry("demo-g-b3", List.of("engineering")),
                    Map.entry("demo-g-b4", List.of("engineering")));

    private static final Map<String, List<String>> BY_NOTION_ID =
            Map.ofEntries(
                    Map.entry("demo-n-a1", List.of("shared", "finance")),
                    Map.entry("demo-n-a2", List.of("shared", "finance")),
                    Map.entry("demo-n-a3", List.of("shared", "engineering")),
                    Map.entry("demo-n-a4", List.of("finance")),
                    Map.entry("demo-n-b1", List.of("shared", "engineering")),
                    Map.entry("demo-n-b2", List.of("shared", "engineering")),
                    Map.entry("demo-n-b3", List.of("engineering")),
                    Map.entry("demo-n-b4", List.of("engineering")));

    private DemoTags() {}

    public static List<String> forDriveFileId(String id) {
        return BY_DRIVE_ID.getOrDefault(normalizeId(id), List.of());
    }

    public static List<String> forNotionPageId(String id) {
        return BY_NOTION_ID.getOrDefault(normalizeId(id), List.of());
    }

    private static String normalizeId(String id) {
        return id == null ? "" : id.trim().toLowerCase(Locale.ROOT);
    }
}
