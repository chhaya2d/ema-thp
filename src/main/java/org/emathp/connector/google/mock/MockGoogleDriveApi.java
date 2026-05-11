package org.emathp.connector.google.mock;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.emathp.connector.google.api.GoogleDriveFile;
import org.emathp.connector.google.api.GoogleSearchRequest;
import org.emathp.connector.google.api.GoogleSearchResponse;

/**
 * In-memory mock of Google Drive Files API semantics with simulated cursor pagination.
 *
 * @implNote Pagination tokens here are plain numeric start indices (e.g. {@code "2"}) for
 *           readability. Real Drive tokens are opaque base64 blobs; clients must treat them as
 *           opaque regardless. The mock's choice doesn't change the engine-side abstraction —
 *           the engine never inspects the token's contents.
 * @implNote LIMIT semantics: this mock honors only {@code pageSize} per call and has no
 *           total-result cap parameter. That mirrors the real Google Drive Files API. The
 *           connector contract intentionally has no LIMIT slot at all (see ADR-0003): LIMIT
 *           is always engine-enforced by {@link org.emathp.engine.QueryExecutor} across pages.
 * @implNote The query DSL is regex-based for simplicity. It accepts only the exact predicates
 *           {@link GoogleQueryTranslator} emits. Hand-written inputs that deviate from those shapes
 *           silently fall through and match every row.
 */
public final class MockGoogleDriveApi {

    private static final List<GoogleDriveFile> SAMPLE_FILES = List.of(
            new GoogleDriveFile(
                    "file-01",
                    "OAuth Migration Plan",
                    "alice",
                    List.of("alice", "bob"),
                    Instant.parse("2026-03-01T08:00:00Z"),
                    Instant.parse("2026-04-10T09:30:00Z"),
                    "https://drive.google.com/file/d/file-01/view"),
            new GoogleDriveFile(
                    "file-02",
                    "Quarterly Hiring Plan",
                    "carol",
                    List.of("carol", "dave", "alice"),
                    Instant.parse("2026-05-15T10:00:00Z"),
                    Instant.parse("2026-06-02T14:15:00Z"),
                    "https://drive.google.com/file/d/file-02/view"),
            new GoogleDriveFile(
                    "file-03",
                    "OAuth Rollout Checklist",
                    "bob",
                    List.of("bob"),
                    Instant.parse("2026-04-25T13:30:00Z"),
                    Instant.parse("2026-05-20T11:00:00Z"),
                    "https://drive.google.com/file/d/file-03/view"),
            new GoogleDriveFile(
                    "file-04",
                    "Roadmap Q3",
                    "dave",
                    List.of("dave", "carol"),
                    Instant.parse("2026-05-01T09:00:00Z"),
                    Instant.parse("2026-05-30T16:45:00Z"),
                    "https://drive.google.com/file/d/file-04/view"),
            new GoogleDriveFile(
                    "file-05",
                    "API Contract Draft",
                    "eve",
                    List.of("eve", "alice"),
                    Instant.parse("2026-04-20T11:20:00Z"),
                    Instant.parse("2026-05-05T08:10:00Z"),
                    "https://drive.google.com/file/d/file-05/view"),
            new GoogleDriveFile(
                    "file-06",
                    "Engineering Onboarding",
                    "frank",
                    List.of("frank"),
                    Instant.parse("2026-03-15T07:45:00Z"),
                    Instant.parse("2026-04-25T12:00:00Z"),
                    "https://drive.google.com/file/d/file-06/view"),
            // NOTE: file-07 / file-08 exist to give the JOIN demo something to match against
            // (see Notion's page-07 / page-08 with identical titles). Their updatedAt values
            // are deliberately old enough to avoid disturbing Demos 1-3's top-N results.
            new GoogleDriveFile(
                    "file-07",
                    "Q4 Roadmap",
                    "carol",
                    List.of("carol", "alice"),
                    Instant.parse("2026-01-01T08:00:00Z"),
                    Instant.parse("2026-01-15T10:00:00Z"),
                    "https://drive.google.com/file/d/file-07/view"),
            new GoogleDriveFile(
                    "file-08",
                    "Security Review",
                    "bob",
                    List.of("bob"),
                    Instant.parse("2026-02-01T09:00:00Z"),
                    Instant.parse("2026-02-10T11:00:00Z"),
                    "https://drive.google.com/file/d/file-08/view"));

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

    public GoogleSearchResponse search(GoogleSearchRequest request) {
        List<GoogleDriveFile> filtered = SAMPLE_FILES.stream()
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
