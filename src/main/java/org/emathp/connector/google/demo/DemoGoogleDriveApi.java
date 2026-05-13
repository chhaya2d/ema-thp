package org.emathp.connector.google.demo;

import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.emathp.connector.google.api.GoogleDriveFile;
import org.emathp.connector.google.api.GoogleSearchRequest;
import org.emathp.connector.google.api.GoogleSearchResponse;
import org.emathp.config.DemoConnectorDefaults;

/**
 * In-memory Google Drive fixture for <strong>demo</strong> mode: eight files shared by every demo
 * user; row visibility is enforced in the engine via {@code tags} on materialized rows, not by
 * slicing the corpus per {@code UserContext}.
 *
 * <p>Uses the same query DSL regex layer as {@link org.emathp.connector.google.mock.MockGoogleDriveApi}.
 */
public final class DemoGoogleDriveApi {

    /** Join-friendly titles (alpha/beta keys); extras exercise HR-only vs engineering-only tags. */
    private static final List<GoogleDriveFile> DEMO_FILES = List.of(
            new GoogleDriveFile(
                    "demo-g-a1",
                    "JoinKeyAlpha",
                    "tenant-1",
                    List.of(),
                    Instant.parse("2026-03-01T10:00:00Z"),
                    Instant.parse("2026-06-10T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-a1/view"),
            new GoogleDriveFile(
                    "demo-g-a2",
                    "JoinKeyAlpha",
                    "tenant-1",
                    List.of(),
                    Instant.parse("2026-03-02T10:00:00Z"),
                    Instant.parse("2026-06-09T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-a2/view"),
            new GoogleDriveFile(
                    "demo-g-a3",
                    "JoinKeyAlpha",
                    "tenant-1",
                    List.of(),
                    Instant.parse("2026-03-03T10:00:00Z"),
                    Instant.parse("2026-06-08T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-a3/view"),
            new GoogleDriveFile(
                    "demo-g-a4",
                    "AliceDriveExtra",
                    "tenant-1",
                    List.of(),
                    Instant.parse("2026-03-04T10:00:00Z"),
                    Instant.parse("2026-06-07T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-a4/view"),
            new GoogleDriveFile(
                    "demo-g-b1",
                    "JoinKeyBeta",
                    "tenant-1",
                    List.of(),
                    Instant.parse("2026-04-01T10:00:00Z"),
                    Instant.parse("2026-05-10T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-b1/view"),
            new GoogleDriveFile(
                    "demo-g-b2",
                    "JoinKeyBeta",
                    "tenant-1",
                    List.of(),
                    Instant.parse("2026-04-02T10:00:00Z"),
                    Instant.parse("2026-05-09T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-b2/view"),
            new GoogleDriveFile(
                    "demo-g-b3",
                    "BobDriveExtra1",
                    "tenant-1",
                    List.of(),
                    Instant.parse("2026-04-03T10:00:00Z"),
                    Instant.parse("2026-05-08T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-b3/view"),
            new GoogleDriveFile(
                    "demo-g-b4",
                    "BobDriveExtra2",
                    "tenant-1",
                    List.of(),
                    Instant.parse("2026-04-04T10:00:00Z"),
                    Instant.parse("2026-05-07T12:00:00Z"),
                    "https://drive.google.com/file/d/demo-g-b4/view"));

    /**
     * Index-aligned with {@link #DEMO_FILES}: same order, same length (validated when building {@link
     * #TAGS_BY_FILE_ID}).
     */
    private static final List<List<String>> DEMO_FILE_TAGS =
            List.of(
                    List.of("hr", "engineering"),
                    List.of("hr", "engineering"),
                    List.of("hr", "engineering"),
                    List.of("hr"),
                    List.of("hr", "engineering"),
                    List.of("hr", "engineering"),
                    List.of("engineering"),
                    List.of("engineering"));

    private static final Map<String, List<String>> TAGS_BY_FILE_ID = indexTagsByFileId();

    private static Map<String, List<String>> indexTagsByFileId() {
        if (DEMO_FILES.size() != DEMO_FILE_TAGS.size()) {
            throw new IllegalStateException(
                    "demo file/tag count mismatch: " + DEMO_FILES.size() + " vs " + DEMO_FILE_TAGS.size());
        }
        Map<String, List<String>> m = new LinkedHashMap<>();
        for (int i = 0; i < DEMO_FILES.size(); i++) {
            m.put(DEMO_FILES.get(i).id(), List.copyOf(DEMO_FILE_TAGS.get(i)));
        }
        return Map.copyOf(m);
    }

    /** Tags for a fixture file id (after filtering, rows are still keyed by id). */
    public static List<String> tagsForFileId(String fileId) {
        return TAGS_BY_FILE_ID.getOrDefault(fileId, List.of());
    }

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

    /**
     * @param actingUserId ignored; the demo corpus is shared for every principal (access via {@code
     *     tags} + role policy).
     */
    public GoogleSearchResponse search(GoogleSearchRequest request, String actingUserId) {
        sleepDemoDelay();
        List<GoogleDriveFile> corpus = sharedCorpus();
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

    /** Same eight files for every principal; access control is via row {@code tags}, not API slicing. */
    private static List<GoogleDriveFile> sharedCorpus() {
        return DEMO_FILES;
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
