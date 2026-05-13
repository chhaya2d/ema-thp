package org.emathp.connector.notion.demo;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.emathp.config.DemoConnectorDefaults;
import org.emathp.connector.notion.api.NotionPage;
import org.emathp.connector.notion.api.NotionSearchRequest;
import org.emathp.connector.notion.api.NotionSearchResponse;

/**
 * Demo Notion pages aligned with {@link org.emathp.connector.google.demo.DemoGoogleDriveApi} titles
 * for join UI experiments. The full eight-page corpus is returned for every user; tags + role policy
 * gate visibility in the engine (same delay source as Google demo).
 */
public final class DemoNotionApi {

    private static final List<NotionPage> DEMO_PAGES = List.of(
            new NotionPage(
                    "demo-n-a1",
                    "JoinKeyAlpha",
                    Instant.parse("2026-06-10T14:00:00Z"),
                    "https://www.notion.so/demo-n-a1"),
            new NotionPage(
                    "demo-n-a2",
                    "JoinKeyAlpha",
                    Instant.parse("2026-06-09T14:00:00Z"),
                    "https://www.notion.so/demo-n-a2"),
            new NotionPage(
                    "demo-n-a3",
                    "JoinKeyAlpha",
                    Instant.parse("2026-06-08T14:00:00Z"),
                    "https://www.notion.so/demo-n-a3"),
            new NotionPage(
                    "demo-n-a4",
                    "AliceNotionExtra",
                    Instant.parse("2026-06-07T14:00:00Z"),
                    "https://www.notion.so/demo-n-a4"),
            new NotionPage(
                    "demo-n-b1",
                    "JoinKeyBeta",
                    Instant.parse("2026-05-10T14:00:00Z"),
                    "https://www.notion.so/demo-n-b1"),
            new NotionPage(
                    "demo-n-b2",
                    "JoinKeyBeta",
                    Instant.parse("2026-05-09T14:00:00Z"),
                    "https://www.notion.so/demo-n-b2"),
            new NotionPage(
                    "demo-n-b3",
                    "BobNotionExtra1",
                    Instant.parse("2026-05-08T14:00:00Z"),
                    "https://www.notion.so/demo-n-b3"),
            new NotionPage(
                    "demo-n-b4",
                    "BobNotionExtra2",
                    Instant.parse("2026-05-07T14:00:00Z"),
                    "https://www.notion.so/demo-n-b4"));

    /**
     * Index-aligned with {@link #DEMO_PAGES}: same order, same length (validated when building {@link
     * #TAGS_BY_PAGE_ID}).
     */
    private static final List<List<String>> DEMO_PAGE_TAGS =
            List.of(
                    List.of("hr", "engineering"),
                    List.of("hr", "engineering"),
                    List.of("hr", "engineering"),
                    List.of("hr"),
                    List.of("hr", "engineering"),
                    List.of("hr", "engineering"),
                    List.of("engineering"),
                    List.of("engineering"));

    private static final Map<String, List<String>> TAGS_BY_PAGE_ID = indexTagsByPageId();

    private static Map<String, List<String>> indexTagsByPageId() {
        if (DEMO_PAGES.size() != DEMO_PAGE_TAGS.size()) {
            throw new IllegalStateException(
                    "demo page/tag count mismatch: " + DEMO_PAGES.size() + " vs " + DEMO_PAGE_TAGS.size());
        }
        Map<String, List<String>> m = new LinkedHashMap<>();
        for (int i = 0; i < DEMO_PAGES.size(); i++) {
            m.put(DEMO_PAGES.get(i).pageId(), List.copyOf(DEMO_PAGE_TAGS.get(i)));
        }
        return Map.copyOf(m);
    }

    public static List<String> tagsForPageId(String pageId) {
        return TAGS_BY_PAGE_ID.getOrDefault(pageId, List.of());
    }

    private static final Pattern TITLE_EQ =
            Pattern.compile("^\\s*title\\s*=\\s*'((?:[^'\\\\]|\\\\.)*)'\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern TITLE_CONTAINS =
            Pattern.compile("^\\s*title\\s+contains\\s+'((?:[^'\\\\]|\\\\.)*)'\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern EDITED_EQ =
            Pattern.compile("^\\s*lastEditedTime\\s*=\\s*'([^']+)'\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern EDITED_GT =
            Pattern.compile("^\\s*lastEditedTime\\s*>\\s*'([^']+)'\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern EDITED_LT =
            Pattern.compile("^\\s*lastEditedTime\\s*<\\s*'([^']+)'\\s*$", Pattern.CASE_INSENSITIVE);

    /**
     * @param actingUserId ignored; the demo corpus is shared for every principal (access via {@code
     *     tags} + role policy).
     */
    public NotionSearchResponse search(NotionSearchRequest request, String actingUserId) {
        sleepDemoDelay();
        List<NotionPage> corpus = sharedCorpus();
        List<NotionPage> filtered = corpus.stream().filter(p -> matches(p, request.filter())).toList();

        int total = filtered.size();
        int start = parseCursor(request.startCursor());
        if (start > total) {
            start = total;
        }
        int pageSize = request.pageSize() == null ? Integer.MAX_VALUE : Math.max(0, request.pageSize());
        int end = (int) Math.min((long) start + pageSize, total);

        List<NotionPage> page = filtered.subList(start, end);
        boolean hasMore = end < total;
        String nextCursor = hasMore ? ("cursor_" + end) : null;
        return new NotionSearchResponse(page, nextCursor, hasMore);
    }

    private static void sleepDemoDelay() {
        int ms = DemoConnectorDefaults.SEARCH_DELAY_MILLIS;
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static List<NotionPage> sharedCorpus() {
        return DEMO_PAGES;
    }

    private static int parseCursor(String cursor) {
        if (cursor == null || cursor.isBlank()) {
            return 0;
        }
        String t = cursor.trim();
        try {
            if (t.startsWith("cursor_")) {
                return Math.max(0, Integer.parseInt(t.substring("cursor_".length())));
            }
            return Math.max(0, Integer.parseInt(t));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private boolean matches(NotionPage page, String filter) {
        if (filter == null || filter.isBlank()) {
            return true;
        }
        String trimmed = filter.trim();

        Matcher mEq = TITLE_EQ.matcher(trimmed);
        if (mEq.matches()) {
            return page.title().equals(unescape(mEq.group(1)));
        }
        Matcher mContains = TITLE_CONTAINS.matcher(trimmed);
        if (mContains.matches()) {
            String needle = unescape(mContains.group(1)).toLowerCase(Locale.ROOT);
            return page.title().toLowerCase(Locale.ROOT).contains(needle);
        }
        Matcher mEditedEq = EDITED_EQ.matcher(trimmed);
        if (mEditedEq.matches()) {
            return page.lastEditedTime().equals(Instant.parse(mEditedEq.group(1)));
        }
        Matcher mGt = EDITED_GT.matcher(trimmed);
        if (mGt.matches()) {
            return page.lastEditedTime().isAfter(Instant.parse(mGt.group(1)));
        }
        Matcher mLt = EDITED_LT.matcher(trimmed);
        if (mLt.matches()) {
            return page.lastEditedTime().isBefore(Instant.parse(mLt.group(1)));
        }
        return true;
    }

    private static String unescape(String s) {
        return s.replace("\\'", "'");
    }
}
