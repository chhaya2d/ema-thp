package org.emathp.connector.google.demo;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.emathp.connector.google.api.GoogleDriveFile;
import org.emathp.connector.google.api.GoogleSearchRequest;
import org.emathp.connector.google.api.GoogleSearchResponse;
import org.emathp.config.DemoConnectorDefaults;

/**
 * In-memory Google Drive mock for <strong>demo</strong> mode: two principals ({@code alice},
 * {@code bob}), four files each (eight total). Three {@code alice} files share one title with
 * Notion demo pages; two {@code bob} files share another title for join experiments.
 *
 * <p>Uses the same query DSL regex layer as {@link org.emathp.connector.google.mock.MockGoogleDriveApi}.
 */
public final class DemoGoogleDriveApi {

    /** Three shared titles for alice + notion; two for bob + notion. */
    private static final List<GoogleDriveFile> DEMO_FILES = List.of(
            new GoogleDriveFile(
                    "demo-g-a1",
                    "JoinKeyAlpha",
                    "alice",
                    List.of("alice"),
                    Instant.parse("2026-03-01T10:00:00Z"),
                    Instant.parse("2026-06-10T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-a1/view"),
            new GoogleDriveFile(
                    "demo-g-a2",
                    "JoinKeyAlpha",
                    "alice",
                    List.of("alice"),
                    Instant.parse("2026-03-02T10:00:00Z"),
                    Instant.parse("2026-06-09T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-a2/view"),
            new GoogleDriveFile(
                    "demo-g-a3",
                    "JoinKeyAlpha",
                    "alice",
                    List.of("alice"),
                    Instant.parse("2026-03-03T10:00:00Z"),
                    Instant.parse("2026-06-08T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-a3/view"),
            new GoogleDriveFile(
                    "demo-g-a4",
                    "AliceDriveExtra",
                    "alice",
                    List.of("alice"),
                    Instant.parse("2026-03-04T10:00:00Z"),
                    Instant.parse("2026-06-07T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-a4/view"),
            new GoogleDriveFile(
                    "demo-g-b1",
                    "JoinKeyBeta",
                    "bob",
                    List.of("bob"),
                    Instant.parse("2026-04-01T10:00:00Z"),
                    Instant.parse("2026-05-10T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-b1/view"),
            new GoogleDriveFile(
                    "demo-g-b2",
                    "JoinKeyBeta",
                    "bob",
                    List.of("bob"),
                    Instant.parse("2026-04-02T10:00:00Z"),
                    Instant.parse("2026-05-09T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-b2/view"),
            new GoogleDriveFile(
                    "demo-g-b3",
                    "BobDriveExtra1",
                    "bob",
                    List.of("bob"),
                    Instant.parse("2026-04-03T10:00:00Z"),
                    Instant.parse("2026-05-08T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-b3/view"),
            new GoogleDriveFile(
                    "demo-g-b4",
                    "BobDriveExtra2",
                    "bob",
                    List.of("bob"),
                    Instant.parse("2026-04-04T10:00:00Z"),
                    Instant.parse("2026-05-07T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-b4/view"));

    private static final Pattern NAME_EQ =
            Pattern.compile("^\\s*name\\s*=\\s*'((?:[^'\\\\]|\\\\.)*)'\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern NAME_CONTAINS =
            Pattern.compile("^\\s*name\\s+contains\\s+'((?:[^'\\\\]|\\\\.)*)'\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern UPDATED_EQ =
            Pattern.compile("^\\s*updatedAt\\s*=\\s*'([^']+)'\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern UPDATED_GT =
            Pattern.compile("^\\s*updatedAt\\s*>\\s*'([^']+)'\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern UPDATED_LT =
            Pattern.compile("^\\s*updatedAt\\s*<\\s*'([^']+)'\\s*$", Pattern.CASE_INSENSITIVE);

    public GoogleSearchResponse search(GoogleSearchRequest request, String actingUserId) {
        sleepDemoDelay();
        List<GoogleDriveFile> corpus = corpusForUser(actingUserId);
        List<GoogleDriveFile> filtered =
                corpus.stream()
                        .filter(f -> matchesQuery(f, request.q()))
                        .sorted(orderComparator(request.orderBy()))
                        .toList();

        int total = filtered.size();
        int start = parseToken(request.pageToken());
        if (start > total) {
            start = total;
        }
        int pageSize = request.pageSize() == null ? Integer.MAX_VALUE : Math.max(0, request.pageSize());
        int end = (int) Math.min((long) start + pageSize, total);

        List<GoogleDriveFile> page = filtered.subList(start, end);
        String nextPageToken = end < total ? ("cursor_" + end) : null;
        return new GoogleSearchResponse(page, nextPageToken);
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

    private List<GoogleDriveFile> corpusForUser(String actingUserId) {
        if (actingUserId == null || actingUserId.isBlank()) {
            return DEMO_FILES;
        }
        String u = actingUserId.trim().toLowerCase(Locale.ROOT);
        if ("alice".equals(u)) {
            return DEMO_FILES.subList(0, 4);
        }
        if ("bob".equals(u)) {
            return DEMO_FILES.subList(4, DEMO_FILES.size());
        }
        return List.of();
    }

    private static int parseToken(String token) {
        if (token == null || token.isBlank()) {
            return 0;
        }
        String t = token.trim();
        try {
            if (t.startsWith("cursor_")) {
                return Math.max(0, Integer.parseInt(t.substring("cursor_".length())));
            }
            return Math.max(0, Integer.parseInt(t));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private boolean matchesQuery(GoogleDriveFile file, String q) {
        if (q == null || q.isBlank()) {
            return true;
        }
        String trimmed = q.trim();

        Matcher mEq = NAME_EQ.matcher(trimmed);
        if (mEq.matches()) {
            return file.name().equals(unescape(mEq.group(1)));
        }
        Matcher mContains = NAME_CONTAINS.matcher(trimmed);
        if (mContains.matches()) {
            String needle = unescape(mContains.group(1)).toLowerCase(Locale.ROOT);
            return file.name().toLowerCase(Locale.ROOT).contains(needle);
        }
        Matcher mUpdatedEq = UPDATED_EQ.matcher(trimmed);
        if (mUpdatedEq.matches()) {
            return file.updatedAt().equals(Instant.parse(mUpdatedEq.group(1)));
        }
        Matcher mGt = UPDATED_GT.matcher(trimmed);
        if (mGt.matches()) {
            return file.updatedAt().isAfter(Instant.parse(mGt.group(1)));
        }
        Matcher mLt = UPDATED_LT.matcher(trimmed);
        if (mLt.matches()) {
            return file.updatedAt().isBefore(Instant.parse(mLt.group(1)));
        }
        return true;
    }

    private static String unescape(String s) {
        return s.replace("\\'", "'");
    }

    private Comparator<GoogleDriveFile> orderComparator(String orderBy) {
        if (orderBy == null || orderBy.isBlank()) {
            return Comparator.comparing(GoogleDriveFile::name, String.CASE_INSENSITIVE_ORDER);
        }
        String[] segments = orderBy.split(",");
        return segments.length == 1 ? comparatorForSegment(segments[0]) : buildComposite(segments);
    }

    private Comparator<GoogleDriveFile> buildComposite(String[] segments) {
        Comparator<GoogleDriveFile> acc = comparatorForSegment(segments[0].trim());
        for (int i = 1; i < segments.length; i++) {
            acc = acc.thenComparing(comparatorForSegment(segments[i].trim()));
        }
        return acc;
    }

    private Comparator<GoogleDriveFile> comparatorForSegment(String segment) {
        String[] parts = segment.trim().split("\\s+");
        String rawField = parts[0];
        boolean desc = parts.length > 1 && parts[1].equalsIgnoreCase("desc");

        String field = rawField.toLowerCase(Locale.ROOT);
        Comparator<GoogleDriveFile> base =
                switch (field) {
                    case "updatedat" -> Comparator.comparing(GoogleDriveFile::updatedAt);
                    case "name" -> Comparator.comparing(GoogleDriveFile::name, String.CASE_INSENSITIVE_ORDER);
                    default -> Comparator.comparing(GoogleDriveFile::name, String.CASE_INSENSITIVE_ORDER);
                };
        return desc ? base.reversed() : base;
    }
}
