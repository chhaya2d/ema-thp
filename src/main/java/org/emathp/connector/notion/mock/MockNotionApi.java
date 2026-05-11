package org.emathp.connector.notion.mock;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.emathp.connector.notion.api.NotionPage;
import org.emathp.connector.notion.api.NotionSearchRequest;
import org.emathp.connector.notion.api.NotionSearchResponse;

/**
 * In-memory mock of Notion's pages search with simulated cursor pagination.
 *
 * @implNote Cursors are plain numeric start indices (e.g. {@code "2"}) for readability. Real
 *           Notion cursors are opaque UUIDs; clients must treat them as opaque regardless.
 * @implNote LIMIT semantics: this mock honors only {@code page_size} per call and has no
 *           total-result cap parameter. That mirrors the real Notion search API. The connector
 *           contract intentionally has no LIMIT slot at all (see ADR-0003): LIMIT is always
 *           engine-enforced by {@link org.emathp.engine.QueryExecutor} across pages.
 * @implNote The filter DSL is regex-based for simplicity. It accepts only the exact predicates
 *           {@link NotionQueryTranslator} emits. Hand-written inputs that deviate from those shapes
 *           silently fall through and match every page.
 */
public final class MockNotionApi {

    private static final List<NotionPage> SAMPLE_PAGES = List.of(
            new NotionPage(
                    "page-01",
                    "OAuth Architecture Notes",
                    Instant.parse("2026-04-15T10:00:00Z"),
                    "https://www.notion.so/page-01"),
            new NotionPage(
                    "page-02",
                    "Hiring Rubric",
                    Instant.parse("2026-05-25T16:30:00Z"),
                    "https://www.notion.so/page-02"),
            new NotionPage(
                    "page-03",
                    "OAuth Production Checklist",
                    Instant.parse("2026-05-10T09:45:00Z"),
                    "https://www.notion.so/page-03"),
            new NotionPage(
                    "page-04",
                    "Onboarding Wiki",
                    Instant.parse("2026-04-22T08:30:00Z"),
                    "https://www.notion.so/page-04"),
            new NotionPage(
                    "page-05",
                    "Sprint Retro Notes",
                    Instant.parse("2026-05-12T17:15:00Z"),
                    "https://www.notion.so/page-05"),
            new NotionPage(
                    "page-06",
                    "Design Review Notes",
                    Instant.parse("2026-03-20T14:00:00Z"),
                    "https://www.notion.so/page-06"),
            // NOTE: page-07 / page-08 mirror Google's file-07 / file-08 by title for the JOIN
            // demo. lastEditedTime values are old enough to avoid disturbing Demos 1-3's top-N
            // ordering.
            new NotionPage(
                    "page-07",
                    "Q4 Roadmap",
                    Instant.parse("2026-01-20T10:00:00Z"),
                    "https://www.notion.so/page-07"),
            new NotionPage(
                    "page-08",
                    "Security Review",
                    Instant.parse("2026-02-15T14:00:00Z"),
                    "https://www.notion.so/page-08"));

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

    public NotionSearchResponse search(NotionSearchRequest request) {
        List<NotionPage> filtered = SAMPLE_PAGES.stream()
                .filter(p -> matches(p, request.filter()))
                .toList();

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
