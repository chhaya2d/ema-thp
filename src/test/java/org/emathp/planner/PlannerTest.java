package org.emathp.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.emathp.auth.UserContext;
import org.emathp.connector.CapabilitySet;
import org.emathp.connector.Connector;
import org.emathp.model.ComparisonExpr;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.Direction;
import org.emathp.model.Operator;
import org.emathp.model.OrderBy;
import org.emathp.model.Query;
import org.emathp.model.EngineRow;
import org.emathp.model.SearchResult;

import org.junit.jupiter.api.Test;

/**
 * Exercises the planner's predicate-pushdown gates ({@code supportsFiltering} +
 * {@code supportedOperators} + {@code supportedFields}) and the residual cascade rule
 * (anything logically downstream of a residual clause also falls residual).
 */
class PlannerTest {

    private static Query query(ComparisonExpr where, List<OrderBy> orderBy) {
        return new Query(List.of(), where, orderBy, 4, null, 2, null);
    }

    private static ComparisonExpr titleEq() {
        return new ComparisonExpr("title", Operator.EQ, "Roadmap");
    }

    private static ComparisonExpr updatedAtGt() {
        return new ComparisonExpr("updatedAt", Operator.GT, "2026-01-01");
    }

    private static List<OrderBy> sortByUpdatedAtDesc() {
        return List.of(new OrderBy("updatedAt", Direction.DESC));
    }

    @Test
    void wherePushedWhenOperatorAndFieldSupported() {
        Connector c = stubConnector(true, true, true,
                Set.of("title", "updatedAt"),
                Set.of(Operator.EQ, Operator.GT));
        PushdownPlan plan = new Planner().plan(c, query(updatedAtGt(), sortByUpdatedAtDesc()));

        assertNotNull(plan.pushedQuery().where());
        assertTrue(plan.pendingOperations().isEmpty());
        assertTrue(plan.residualOps().isEmpty());
    }

    @Test
    void whereResidualWhenOperatorUnsupported() {
        Connector c = stubConnector(true, true, true,
                Set.of("title", "updatedAt"),
                Set.of(Operator.EQ));
        PushdownPlan plan = new Planner().plan(c, query(updatedAtGt(), List.of()));

        assertNull(plan.pushedQuery().where(), "GT should not be pushed when only EQ is supported");
        // PAGINATION cascades because the query has pageSize=2 and whereOk=false.
        assertEquals(List.of("WHERE", "PAGINATION"), plan.pendingOperations());
        assertNotNull(plan.residualOps().where());
    }

    @Test
    void whereResidualWhenFieldUnsupported() {
        Connector c = stubConnector(true, true, true,
                Set.of("title"),
                Set.of(Operator.EQ, Operator.GT));
        PushdownPlan plan = new Planner().plan(c, query(updatedAtGt(), List.of()));

        assertNull(plan.pushedQuery().where(), "predicate on unsupported field must not be pushed");
        assertEquals(List.of("WHERE", "PAGINATION"), plan.pendingOperations());
        assertNotNull(plan.residualOps().where());
    }

    @Test
    void residualWhereCascadesOrderByAndPagination() {
        // Operator unsupported -> WHERE residual -> ORDER BY and PAGINATION must also fall residual,
        // even though sorting and pagination are independently supported. This is the SQL logical-order
        // dependency rule: pushing a sort over an unfiltered universe produces wrong page boundaries.
        Connector c = stubConnector(true, true, true,
                Set.of("title", "updatedAt"),
                Set.of(Operator.EQ));
        PushdownPlan plan = new Planner().plan(c, query(updatedAtGt(), sortByUpdatedAtDesc()));

        assertNull(plan.pushedQuery().where());
        assertTrue(plan.pushedQuery().orderBy().isEmpty(),
                "ORDER BY must not be pushed when WHERE is residual");
        assertNull(plan.pushedQuery().cursor());
        assertNull(plan.pushedQuery().pageSize());
        assertEquals(List.of("WHERE", "ORDER BY", "PAGINATION"), plan.pendingOperations());
    }

    @Test
    void filteringDisabledIsAlsoEnoughToBlockPushdown() {
        Connector c = stubConnector(false, true, true,
                Set.of("title", "updatedAt"),
                Set.of(Operator.EQ, Operator.GT));
        PushdownPlan plan = new Planner().plan(c, query(titleEq(), List.of()));

        assertNull(plan.pushedQuery().where());
        // PAGINATION cascades because the query() helper sets pageSize=2 and WHERE is residual.
        assertEquals(List.of("WHERE", "PAGINATION"), plan.pendingOperations());
    }

    private static Connector stubConnector(
            boolean supportsFiltering,
            boolean supportsSorting,
            boolean supportsPagination,
            Set<String> supportedFields,
            Set<Operator> supportedOperators) {
        CapabilitySet caps = new CapabilitySet(
                supportsFiltering,
                /* supportsProjection */ false,
                supportsSorting,
                supportsPagination,
                supportedFields,
                supportedOperators);
        return new Connector() {
            @Override
            public String source() {
                return "stub";
            }

            @Override
            public CapabilitySet capabilities() {
                return caps;
            }

            @Override
            public SearchResult<EngineRow> search(UserContext userContext, ConnectorQuery query) {
                throw new UnsupportedOperationException("stub does not run");
            }
        };
    }
}
