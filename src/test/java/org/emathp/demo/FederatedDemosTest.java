package org.emathp.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.emathp.auth.UserContext;
import org.emathp.connector.Connector;
import org.emathp.connector.google.mock.GoogleDriveConnector;
import org.emathp.connector.notion.mock.NotionConnector;
import org.emathp.engine.JoinExecutor;
import org.emathp.engine.QueryExecutor;
import org.emathp.federation.JoinRunner;
import org.emathp.federation.MaterializedPage;
import org.emathp.model.ComparisonExpr;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.EngineRow;
import org.emathp.model.JoinQuery;
import org.emathp.model.JoinSide;
import org.emathp.model.JoinWhere;
import org.emathp.model.ParsedQuery;
import org.emathp.model.Query;
import org.emathp.parser.SQLParserService;
import org.emathp.planner.Planner;
import org.emathp.planner.PushdownPlan;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for the federated scenarios previously printed by {@link org.emathp.Main}.
 * Asserts planner pushdown (connector query shape + residual flags) and executor row outcomes
 * against the stock Google + Notion mock datasets.
 */
class FederatedDemosTest {

    private SQLParserService parser;
    private Planner planner;
    private QueryExecutor executor;
    private JoinExecutor joinExecutor;
    private Connector google;
    private Connector notion;
    private UserContext user;

    @BeforeEach
    void setUp() {
        parser = new SQLParserService();
        planner = new Planner();
        executor = new QueryExecutor();
        joinExecutor = new JoinExecutor(planner, executor);
        google = new GoogleDriveConnector();
        notion = new NotionConnector();
        user = UserContext.anonymous();
    }

    @Test
    void demo1_google_fullPushdown_topFourByUpdatedAtDesc() {
        Query q = parseSingleSourceDemo1();
        PushdownPlan plan = planner.plan(google, q);
        assertTrue(plan.residualOps().isEmpty());
        assertTrue(plan.pendingOperations().isEmpty());
        assertPushed(plan.pushedQuery(), "WHERE", "ORDER BY", "PROJECTION", "PAGINATION");

        QueryExecutor.ExecutionResult exec =
                executor.execute(user, google, plan.pushedQuery(), plan.residualOps(), q.limit());
        assertFalse(exec.residualApplied());
        assertEquals(4, exec.rows().size());
        assertEquals(
                List.of(
                        "Quarterly Hiring Plan",
                        "Roadmap Q3",
                        "OAuth Rollout Checklist",
                        "API Contract Draft"),
                exec.rows().stream().map(FederatedDemosTest::titleCell).toList());
    }

    @Test
    void demo1_notion_wherePushed_orderByAndPaginationResidual() {
        Query q = parseSingleSourceDemo1();
        PushdownPlan plan = planner.plan(notion, q);
        assertNotNull(plan.pushedQuery().where());
        assertTrue(plan.pushedQuery().orderBy().isEmpty());
        assertNull(plan.pushedQuery().cursor());
        assertNull(plan.pushedQuery().pageSize());
        assertEquals(
                List.of("ORDER BY", "PROJECTION", "PAGINATION"),
                plan.pendingOperations(),
                "Notion does not support projection; select list stays pending");
        assertNull(plan.residualOps().where());
        assertFalse(plan.residualOps().orderBy().isEmpty());

        QueryExecutor.ExecutionResult exec =
                executor.execute(user, notion, plan.pushedQuery(), plan.residualOps(), q.limit());
        assertTrue(exec.residualApplied());
        assertEquals(4, exec.rows().size());
        assertEquals(
                List.of(
                        "Hiring Rubric",
                        "Sprint Retro Notes",
                        "OAuth Production Checklist",
                        "Onboarding Wiki"),
                exec.rows().stream().map(FederatedDemosTest::titleCell).toList());
    }

    @Test
    void demo2_google_ltUnsupported_fullResidualOrdering() {
        Query q = parseSingleSourceDemo2();
        PushdownPlan plan = planner.plan(google, q);
        assertNull(plan.pushedQuery().where());
        assertEquals(List.of("WHERE", "ORDER BY", "PAGINATION"), plan.pendingOperations());
        assertNotNull(plan.residualOps().where());

        QueryExecutor.ExecutionResult exec =
                executor.execute(user, google, plan.pushedQuery(), plan.residualOps(), q.limit());
        assertTrue(exec.residualApplied());
        assertEquals(3, exec.rows().size());
        assertEquals(
                List.of("API Contract Draft", "Engineering Onboarding", "OAuth Migration Plan"),
                exec.rows().stream().map(FederatedDemosTest::titleCell).toList());
    }

    @Test
    void demo2_notion_ltPushed_orderByResidual() {
        Query q = parseSingleSourceDemo2();
        PushdownPlan plan = planner.plan(notion, q);
        assertNotNull(plan.pushedQuery().where());
        assertTrue(plan.pushedQuery().orderBy().isEmpty());
        assertEquals(
                List.of("ORDER BY", "PROJECTION", "PAGINATION"),
                plan.pendingOperations());

        QueryExecutor.ExecutionResult exec =
                executor.execute(user, notion, plan.pushedQuery(), plan.residualOps(), q.limit());
        assertTrue(exec.residualApplied());
        assertEquals(3, exec.rows().size());
        assertEquals(
                List.of(
                        "Sprint Retro Notes",
                        "OAuth Production Checklist",
                        "Onboarding Wiki"),
                exec.rows().stream().map(FederatedDemosTest::titleCell).toList());
    }

    @Test
    void demo3_google_ownerResidual_metadataMatchesAlice() {
        Query q = parseSingleSourceDemo3();
        PushdownPlan plan = planner.plan(google, q);
        assertNull(plan.pushedQuery().where());
        assertNotNull(plan.residualOps().where());

        QueryExecutor.ExecutionResult exec =
                executor.execute(user, google, plan.pushedQuery(), plan.residualOps(), q.limit());
        assertTrue(exec.residualApplied());
        assertEquals(1, exec.rows().size());
        EngineRow row0 = exec.rows().get(0);
        assertEquals("OAuth Migration Plan", titleCell(row0));
        assertEquals("alice", row0.fields().get("owner"));
    }

    @Test
    void demo3_notion_ownerResidual_emptyMetadata_yieldsNoRows() {
        Query q = parseSingleSourceDemo3();
        PushdownPlan plan = planner.plan(notion, q);
        assertNull(plan.pushedQuery().where());

        QueryExecutor.ExecutionResult exec =
                executor.execute(user, notion, plan.pushedQuery(), plan.residualOps(), q.limit());
        assertTrue(exec.residualApplied());
        assertTrue(exec.rows().isEmpty());
    }

    @Test
    void demo4_join_twoMatchingTitles_paginationAcrossPages() {
        ParsedQuery pq = parser.parse(
                """
                SELECT g.title, n.title
                FROM google g
                JOIN notion n ON g.title = n.title
                WHERE g.updatedAt > '2026-01-01'
                LIMIT 5
                """);
        JoinQuery parsed = (JoinQuery) pq;
        JoinQuery page1Query = parsed.withPagination(null, 1);

        Map<String, Connector> connectorsByName = new LinkedHashMap<>();
        connectorsByName.put("google", google);
        connectorsByName.put("notion", notion);

        MaterializedPage p1 =
                JoinRunner.run(joinExecutor, user, connectorsByName, page1Query);
        assertEquals(2, p1.upstreamRowCount());
        assertEquals(1, p1.rows().size());
        assertNotNull(p1.nextCursor());

        JoinQuery page2Query =
                page1Query.withPagination(p1.nextCursor(), page1Query.pageSize());
        MaterializedPage p2 = JoinRunner.run(joinExecutor, user, connectorsByName, page2Query);
        assertEquals(1, p2.rows().size());
        assertNull(p2.nextCursor());

        var titlesLeft =
                Set.of(
                        String.valueOf(p1.rows().get(0).fields().get("g.title")),
                        String.valueOf(p2.rows().get(0).fields().get("g.title")));
        assertEquals(Set.of("Q4 Roadmap", "Security Review"), titlesLeft);

        PushdownPlan googlePlan = planner.plan(google, joinSideQuery(parsed.left(), parsed.where()));
        assertTrue(googlePlan.residualOps().isEmpty());
        assertNotNull(googlePlan.pushedQuery().where());
    }

    private static Query joinSideQuery(JoinSide side, JoinWhere where) {
        ComparisonExpr sideWhere =
                (where != null && where.alias().equals(side.alias())) ? where.predicate() : null;
        return new Query(List.of(), sideWhere, List.of(), null, null, null);
    }

    private Query parseSingleSourceDemo1() {
        ParsedQuery pq = parser.parse(
                """
                SELECT title, updatedAt
                FROM resources
                WHERE updatedAt > '2026-01-01'
                ORDER BY updatedAt DESC
                LIMIT 4
                """);
        Query raw = (Query) pq;
        return raw.withPagination(null, 2);
    }

    private Query parseSingleSourceDemo2() {
        ParsedQuery pq = parser.parse(
                """
                SELECT title, updatedAt
                FROM resources
                WHERE updatedAt < '2026-05-15'
                ORDER BY updatedAt DESC
                LIMIT 3
                """);
        Query raw = (Query) pq;
        return raw.withPagination(null, 2);
    }

    private Query parseSingleSourceDemo3() {
        ParsedQuery pq = parser.parse(
                """
                SELECT title, updatedAt
                FROM resources
                WHERE owner = 'alice'
                ORDER BY updatedAt DESC
                LIMIT 3
                """);
        Query raw = (Query) pq;
        return raw.withPagination(null, 2);
    }

    private static String titleCell(EngineRow r) {
        return String.valueOf(r.fields().get("title"));
    }

    /**
     * Mirrors {@link org.emathp.Main#printPlan} / {@code pushedOps}: labels for non-empty slices of
     * {@link ConnectorQuery}.
     */
    private static void assertPushed(ConnectorQuery cq, String... expectedLabels) {
        assertEquals(Set.of(expectedLabels), Set.copyOf(pushedLabels(cq)), "pushed connector shape");
    }

    private static List<String> pushedLabels(ConnectorQuery cq) {
        List<String> ops = new ArrayList<>();
        if (cq.where() != null) {
            ops.add("WHERE");
        }
        if (!cq.orderBy().isEmpty()) {
            ops.add("ORDER BY");
        }
        if (!cq.projection().isEmpty()) {
            ops.add("PROJECTION");
        }
        if (cq.pageSize() != null || cq.cursor() != null) {
            ops.add("PAGINATION");
        }
        return ops;
    }
}
