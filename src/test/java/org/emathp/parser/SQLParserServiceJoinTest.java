package org.emathp.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.emathp.model.ComparisonExpr;
import org.emathp.model.JoinPredicate;
import org.emathp.model.JoinQuery;
import org.emathp.model.JoinSide;
import org.emathp.model.JoinWhere;
import org.emathp.model.Operator;
import org.emathp.model.ParsedQuery;
import org.emathp.model.Query;
import org.junit.jupiter.api.Test;

/**
 * Covers the JOIN-aware parser path: happy paths for the column-to-side mapping and
 * canonicalization, plus the v1 validation matrix.
 */
class SQLParserServiceJoinTest {

    private final SQLParserService parser = new SQLParserService();

    // ---- Existing single-source SQL still parses to Query ----

    @Test
    void singleSourceSqlStillParsesToQuery() {
        ParsedQuery pq = parser.parse(
                "SELECT title FROM resources WHERE updatedAt > '2026-01-01' LIMIT 5");
        assertInstanceOf(Query.class, pq);
    }

    // ---- Happy path: aliases, qualified select, canonical ON ----

    @Test
    void joinWithAliasesAndQualifiedSelect() {
        ParsedQuery pq = parser.parse(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title LIMIT 10");

        JoinQuery jq = assertInstanceOf(JoinQuery.class, pq);
        assertEquals(new JoinSide("google", "g"), jq.left());
        assertEquals(new JoinSide("notion", "n"), jq.right());
        assertEquals(new JoinPredicate("g", "title", "n", "title"), jq.on());
        assertEquals(List.of("g.title", "n.title"), jq.select());
        assertEquals(10, jq.limit());
    }

    @Test
    void joinWithoutAliasesUsesTableNamesAsQualifiers() {
        ParsedQuery pq = parser.parse(
                "SELECT google.title, notion.title FROM google JOIN notion "
                        + "ON google.title = notion.title");

        JoinQuery jq = assertInstanceOf(JoinQuery.class, pq);
        assertEquals(new JoinSide("google", "google"), jq.left());
        assertEquals(new JoinSide("notion", "notion"), jq.right());
        assertEquals(new JoinPredicate("google", "title", "notion", "title"), jq.on());
    }

    @Test
    void onPredicateIsCanonicalizedRegardlessOfWriteOrder() {
        // User wrote ON n.title = g.title (right side first). Parser should canonicalize so
        // leftAlias matches the LEFT side ('g').
        ParsedQuery pq = parser.parse(
                "SELECT * FROM google g JOIN notion n ON n.title = g.title");

        JoinQuery jq = assertInstanceOf(JoinQuery.class, pq);
        assertEquals(new JoinPredicate("g", "title", "n", "title"), jq.on());
    }

    @Test
    void selectStarStoredAsEmptyList() {
        ParsedQuery pq = parser.parse("SELECT * FROM google g JOIN notion n ON g.title = n.title");
        JoinQuery jq = assertInstanceOf(JoinQuery.class, pq);
        assertTrue(jq.select().isEmpty(), "SELECT * must yield empty select list (same convention as Query)");
    }

    @Test
    void explicitInnerJoinAccepted() {
        ParsedQuery pq = parser.parse(
                "SELECT g.title FROM google g INNER JOIN notion n ON g.title = n.title");
        assertInstanceOf(JoinQuery.class, pq);
    }

    // ---- Validation matrix ----

    @Test
    void leftJoinRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> parser.parse("SELECT * FROM google g LEFT JOIN notion n ON g.title = n.title"));
        assertTrue(ex.getMessage().contains("INNER JOIN"), ex.getMessage());
    }

    @Test
    void multipleJoinsRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> parser.parse(
                        "SELECT * FROM a x JOIN b y ON x.id = y.id JOIN c z ON y.id = z.id"));
        assertTrue(ex.getMessage().contains("single JOIN"), ex.getMessage());
    }

    @Test
    void onMissingRejected() {
        // Comma-style implicit cross is treated as a CROSS JOIN by JSQLParser; explicit JOIN
        // without ON is a parse error in JSQLParser itself, so we exercise CROSS via the
        // validation path below. Here we ensure NATURAL JOIN, which has no ON, is rejected.
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> parser.parse("SELECT * FROM google g NATURAL JOIN notion n"));
        assertTrue(ex.getMessage().contains("NATURAL"), ex.getMessage());
    }

    @Test
    void crossJoinRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> parser.parse("SELECT * FROM google g CROSS JOIN notion n"));
        assertTrue(ex.getMessage().toUpperCase().contains("CROSS"), ex.getMessage());
    }

    @Test
    void onNotEqualityRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> parser.parse(
                        "SELECT * FROM google g JOIN notion n ON g.title > n.title"));
        assertTrue(ex.getMessage().toLowerCase().contains("equality"), ex.getMessage());
    }

    @Test
    void onUnqualifiedColumnRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> parser.parse(
                        "SELECT * FROM google g JOIN notion n ON title = n.title"));
        assertTrue(ex.getMessage().toLowerCase().contains("qualified"), ex.getMessage());
    }

    @Test
    void onSameSideTwiceRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> parser.parse(
                        "SELECT * FROM google g JOIN notion n ON g.title = g.id"));
        assertTrue(ex.getMessage().contains("both joined sides"), ex.getMessage());
    }

    @Test
    void selectUnqualifiedColumnRejectedInJoin() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> parser.parse(
                        "SELECT title FROM google g JOIN notion n ON g.title = n.title"));
        assertTrue(ex.getMessage().toLowerCase().contains("qualified"), ex.getMessage());
    }

    @Test
    void selectUnknownAliasRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> parser.parse(
                        "SELECT z.title FROM google g JOIN notion n ON g.title = n.title"));
        assertTrue(ex.getMessage().contains("unknown alias 'z'"), ex.getMessage());
    }

    // ---- WHERE on JOIN: single-side accepted, routed to the matching alias ----

    @Test
    void whereTargetingLeftSideRoutedToLeftAlias() {
        ParsedQuery pq = parser.parse(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title "
                        + "WHERE g.updatedAt > '2026-01-01'");

        JoinQuery jq = assertInstanceOf(JoinQuery.class, pq);
        JoinWhere where = jq.where();
        assertEquals("g", where.alias());
        // Stored predicate has unqualified field name so the per-side Query sees a normal WHERE.
        assertEquals(
                new ComparisonExpr("updatedAt", Operator.GT, Instant.parse("2026-01-01T00:00:00Z")),
                where.predicate());
    }

    @Test
    void whereTargetingRightSideRoutedToRightAlias() {
        ParsedQuery pq = parser.parse(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title "
                        + "WHERE n.title LIKE '%Roadmap%'");

        JoinQuery jq = assertInstanceOf(JoinQuery.class, pq);
        assertEquals("n", jq.where().alias());
        assertEquals(
                new ComparisonExpr("title", Operator.LIKE, "%Roadmap%"),
                jq.where().predicate());
    }

    @Test
    void whereOnJoinIsOptional() {
        ParsedQuery pq = parser.parse(
                "SELECT * FROM google g JOIN notion n ON g.title = n.title");
        JoinQuery jq = assertInstanceOf(JoinQuery.class, pq);
        assertEquals(null, jq.where(), "no WHERE clause should leave JoinQuery.where() null");
    }

    @Test
    void whereWithUnqualifiedColumnRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> parser.parse(
                        "SELECT * FROM google g JOIN notion n ON g.title = n.title "
                                + "WHERE updatedAt > '2026-01-01'"));
        assertTrue(ex.getMessage().toLowerCase().contains("qualified"), ex.getMessage());
    }

    @Test
    void whereWithUnknownAliasRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> parser.parse(
                        "SELECT * FROM google g JOIN notion n ON g.title = n.title "
                                + "WHERE z.updatedAt > '2026-01-01'"));
        assertTrue(ex.getMessage().contains("'z'"), ex.getMessage());
    }

    @Test
    void whereWithCrossSidePredicateRejected() {
        // RHS being a column from the other side falls outside literal-supported shapes; the
        // parser must reject (cross-side WHERE is post-join semantics, deferred).
        assertThrows(IllegalArgumentException.class, () -> parser.parse(
                "SELECT * FROM google g JOIN notion n ON g.title = n.title "
                        + "WHERE g.updatedAt > n.updatedAt"));
    }

    @Test
    void whereWithCompoundAndRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> parser.parse(
                        "SELECT * FROM google g JOIN notion n ON g.title = n.title "
                                + "WHERE g.updatedAt > '2026-01-01' AND n.title LIKE '%Roadmap%'"));
        assertTrue(
                ex.getMessage().contains("=/>/</LIKE") || ex.getMessage().toLowerCase().contains("comparison"),
                ex.getMessage());
    }

    @Test
    void orderByOnJoinRejectedInV1() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> parser.parse(
                        "SELECT * FROM google g JOIN notion n ON g.title = n.title "
                                + "ORDER BY g.updatedAt DESC"));
        assertTrue(ex.getMessage().contains("ORDER BY"), ex.getMessage());
    }

    @Test
    void duplicateAliasRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> parser.parse("SELECT * FROM google g JOIN notion g ON g.title = g.title"));
        assertTrue(ex.getMessage().contains("alias"), ex.getMessage());
    }
}
