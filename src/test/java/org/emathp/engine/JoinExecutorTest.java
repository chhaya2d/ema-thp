package org.emathp.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.emathp.auth.UserContext;
import org.emathp.connector.Connector;
import org.emathp.connector.google.mock.GoogleDriveConnector;
import org.emathp.connector.notion.mock.NotionConnector;
import org.emathp.model.ComparisonExpr;
import org.emathp.model.EngineRow;
import org.emathp.model.JoinQuery;
import org.emathp.model.JoinSide;
import org.emathp.model.JoinWhere;
import org.emathp.model.ParsedQuery;
import org.emathp.model.Query;
import org.emathp.federation.JoinRunner;
import org.emathp.federation.MaterializedPage;
import org.emathp.parser.SQLParserService;
import org.emathp.planner.Planner;
import org.emathp.planner.PushdownPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end tests for {@link JoinExecutor} + {@link LimitExecutor} +
 * {@link org.emathp.federation.OffsetCursorPager} via {@link JoinRunner}. Uses the real
 * connectors with their in-memory mocks so the entire
 * pipeline (including predicate-pushdown for per-side WHERE) runs.
 *
 * <p>Reference data — exactly two title overlaps across the mocks:
 * <pre>
 *   "Q4 Roadmap"      - google file-07 (updatedAt 2026-01-15) ↔ notion page-07 (lastEditedTime 2026-01-20)
 *   "Security Review" - google file-08 (updatedAt 2026-02-10) ↔ notion page-08 (lastEditedTime 2026-02-15)
 * </pre>
 *
 * <p>Joined-output order is determined by the LEFT side's iteration order. With no per-side
 * ORDER BY, {@code MockGoogleDriveApi} sorts alphabetically by name. "Q4 Roadmap" precedes
 * "Security Review" alphabetically, so the deterministic sequence is:
 * <ol>
 *   <li>(file-07, page-07) "Q4 Roadmap"</li>
 *   <li>(file-08, page-08) "Security Review"</li>
 * </ol>
 *
 * @implNote The dataset deliberately has only 2 overlapping titles. Adding more would force
 *           date-rebalancing across Demos 1-3 (their top-N expectations are sensitive to the
 *           Google updatedAt distribution). The tests cover all code paths even with 2 rows;
 *           pagination uses pageSize=1 to exercise multi-page threading.
 */
class JoinExecutorTest {

    private SQLParserService parser;
    private JoinExecutor joinExecutor;
    private Map<String, Connector> connectors;

    @BeforeEach
    void setUp() {
        parser = new SQLParserService();
        Planner planner = new Planner();
        QueryExecutor singleExecutor = new QueryExecutor();
        joinExecutor = new JoinExecutor(planner, singleExecutor);
        connectors = Map.of(
                "google", new GoogleDriveConnector(),
                "notion", new NotionConnector());
    }

    private JoinQuery parseJoin(String sql) {
        ParsedQuery pq = parser.parse(sql);
        if (!(pq instanceof JoinQuery jq)) {
            throw new IllegalStateException("Expected JoinQuery, got " + pq.getClass().getSimpleName());
        }
        return jq;
    }

    private static List<String> titles(MaterializedPage r) {
        return r.rows().stream()
                .map(row -> row.fields().get("g.title").toString())
                .toList();
    }

    private MaterializedPage run(JoinQuery jq) {
        return JoinRunner.run(joinExecutor, UserContext.anonymous(), connectors, jq);
    }

    // ---- Happy path ----

    @Test
    void joinOnTitleProducesBothOverlaps() {
        JoinQuery jq = parseJoin(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title");

        MaterializedPage result = run(jq);

        assertEquals(2, result.upstreamRowCount());
        assertEquals(List.of("Q4 Roadmap", "Security Review"), titles(result));
        assertNull(result.nextCursor(), "no LIMIT / pageSize should mean no nextCursor");
        // Equi-predicate must hold: each joined row carries identical titles on both sides.
        for (EngineRow jr : result.rows()) {
            assertEquals(jr.fields().get("g.title"), jr.fields().get("n.title"));
        }
    }

    // ---- WHERE on each side ----

    @Test
    void whereOnLeftSidePrunesBeforeJoin() {
        // file-07 has updatedAt 2026-01-15 (excluded), file-08 has 2026-02-10 (kept).
        // → 1 joined row.
        JoinQuery jq = parseJoin(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title "
                        + "WHERE g.updatedAt > '2026-01-25'");

        MaterializedPage result = run(jq);

        assertEquals(1, result.upstreamRowCount());
        assertEquals(List.of("Security Review"), titles(result));
    }

    @Test
    void whereOnRightSidePrunesBeforeJoin() {
        // page-07 has lastEditedTime 2026-01-20 (excluded), page-08 has 2026-02-15 (kept).
        // The logical column name 'updatedAt' is mapped to Notion's 'lastEditedTime' by the
        // translator, so this exercises the predicate-pushdown path on Notion's side.
        JoinQuery jq = parseJoin(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title "
                        + "WHERE n.updatedAt > '2026-02-01'");

        MaterializedPage result = run(jq);

        assertEquals(1, result.upstreamRowCount());
        assertEquals(List.of("Security Review"), titles(result));
    }

    @Test
    void whereThatEmptiesOneSideYieldsZeroRows() {
        JoinQuery jq = parseJoin(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title "
                        + "WHERE g.updatedAt > '2099-01-01'");

        MaterializedPage result = run(jq);

        assertEquals(0, result.upstreamRowCount());
        assertTrue(result.rows().isEmpty());
        assertNull(result.nextCursor());
    }

    // ---- Pagination on the joined output ----

    @Test
    void paginationProducesPagesAndThreadsCursor() {
        JoinQuery first = parseJoin(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title")
                .withPagination(null, 1);

        MaterializedPage page1 = run(first);
        assertEquals(List.of("Q4 Roadmap"), titles(page1));
        assertEquals("1", page1.nextCursor());
        assertEquals(2, page1.upstreamRowCount());

        JoinQuery second = first.withPagination(page1.nextCursor(), 1);
        MaterializedPage page2 = run(second);
        assertEquals(List.of("Security Review"), titles(page2));
        assertNull(page2.nextCursor(), "exhausted joined output → no further cursor");
    }

    @Test
    void paginationExactBoundaryDoesNotEmitTrailingCursor() {
        // pageSize equals total joined rows: one page, no nextCursor.
        JoinQuery jq = parseJoin(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title")
                .withPagination(null, 2);

        MaterializedPage result = run(jq);

        assertEquals(2, result.rows().size());
        assertNull(result.nextCursor());
    }

    @Test
    void paginationCursorPastEndReturnsEmpty() {
        JoinQuery jq = parseJoin(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title")
                .withPagination("99", 2);

        MaterializedPage result = run(jq);

        assertTrue(result.rows().isEmpty());
        assertNull(result.nextCursor());
        assertEquals(2, result.upstreamRowCount(), "upstreamRowCount reflects full materialization, not the page");
    }

    // ---- LIMIT (federated cap on the joined output) ----

    @Test
    void limitTruncatesJoinedOutput() {
        JoinQuery jq = parseJoin(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title LIMIT 1");

        MaterializedPage result = run(jq);

        assertEquals(1, result.rows().size());
        assertEquals(List.of("Q4 Roadmap"), titles(result));
        assertNull(result.nextCursor());
        assertTrue(result.stoppedAtLimit(),
                "stoppedAtLimit must flag when LIMIT (not pageSize) caused truncation");
        assertEquals(2, result.upstreamRowCount(), "upstreamRowCount is the pre-LIMIT count");
    }

    @Test
    void limitGreaterThanJoinedSizeIsNoOp() {
        JoinQuery jq = parseJoin(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title LIMIT 999");

        MaterializedPage result = run(jq);

        assertEquals(2, result.rows().size());
        assertNull(result.nextCursor());
        // stoppedAtLimit only when LIMIT actually truncated. Here it didn't.
        assertFalse(result.stoppedAtLimit());
    }

    @Test
    void limitEqualToJoinedSizeIsNoOp() {
        // Edge: LIMIT == totalJoined. The result is fully delivered with no further page; this
        // pins the spec "stoppedAtLimit iff LIMIT actually truncated" - LIMIT=2 and totalJoined=2
        // is NOT a truncation.
        JoinQuery jq = parseJoin(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title LIMIT 2");

        MaterializedPage result = run(jq);

        assertEquals(2, result.rows().size());
        assertNull(result.nextCursor());
        assertFalse(result.stoppedAtLimit());
    }

    // ---- LIMIT + pagination interaction ----

    @Test
    void limitAndPaginationStopAtLimitOnFirstPage() {
        // LIMIT 1, pageSize 1: first page is the only page. stoppedAtLimit on page 1 because
        // limited (1) < totalJoined (2).
        JoinQuery first = parseJoin(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title LIMIT 1")
                .withPagination(null, 1);

        MaterializedPage page1 = run(first);

        assertEquals(List.of("Q4 Roadmap"), titles(page1));
        assertNull(page1.nextCursor(), "LIMIT 1 caps at row 1 — no page 2 even with pageSize=1");
        assertTrue(page1.stoppedAtLimit());
    }

    // ---- WHERE + pagination together ----

    @Test
    void whereAndPaginationCompose() {
        // WHERE narrows joined output to 1 row. pageSize=1 → single page, no further cursor.
        JoinQuery first = parseJoin(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title "
                        + "WHERE g.updatedAt > '2026-01-25'")
                .withPagination(null, 1);

        MaterializedPage page = run(first);

        assertEquals(List.of("Security Review"), titles(page));
        assertNull(page.nextCursor());
        assertEquals(1, page.upstreamRowCount());
    }

    // ---- Validation ----

    @Test
    void unknownConnectorNameRaises() {
        // Map only knows 'google' / 'notion'; reference 'mystery' on the right side.
        JoinQuery jq = parseJoin(
                "SELECT g.title, m.title FROM google g JOIN mystery m ON g.title = m.title");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> run(jq));
        assertTrue(ex.getMessage().contains("mystery"), ex.getMessage());
    }

    @Test
    void invalidCursorRaises() {
        JoinQuery jq = parseJoin(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title")
                .withPagination("not-a-number", 2);

        assertThrows(IllegalArgumentException.class, () -> run(jq));
    }

    // ---- Sanity: per-side WHERE actually pushes when capabilities allow ----

    @Test
    void whereOnLeftSideGetsPushedToConnectorWhenSupported() {
        // updatedAt + GT are in Google's supportedOperators / supportedFields, so Google's per-
        // side plan must push the WHERE rather than residualize it — checked via {@link Planner}.
        JoinQuery jq = parseJoin(
                "SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title "
                        + "WHERE g.updatedAt > '2026-01-25'");
        Planner planner = new Planner();
        Connector google = connectors.get("google");
        Connector notion = connectors.get("notion");
        PushdownPlan googlePlan = planner.plan(google, joinSideQuery(jq.left(), jq.where()));
        PushdownPlan notionPlan = planner.plan(notion, joinSideQuery(jq.right(), jq.where()));

        assertNotNull(googlePlan.pushedQuery().where(), "Google should push the per-side WHERE to the connector");
        assertTrue(googlePlan.residualOps().isEmpty(), "no residual on Google for a WHERE it can natively serve");
        assertNull(
                notionPlan.pushedQuery().where(),
                "Notion side has no WHERE because the join's WHERE targeted only 'g'");
    }

    private static Query joinSideQuery(JoinSide side, JoinWhere where) {
        ComparisonExpr sideWhere =
                (where != null && where.alias().equals(side.alias())) ? where.predicate() : null;
        return new Query(List.of(), sideWhere, List.of(), null, null, null);
    }
}
