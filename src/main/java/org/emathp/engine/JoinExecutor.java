package org.emathp.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.emathp.connector.Connector;
import org.emathp.engine.internal.RowFields;
import org.emathp.model.ComparisonExpr;
import org.emathp.model.EngineRow;
import org.emathp.model.JoinPredicate;
import org.emathp.model.JoinQuery;
import org.emathp.model.JoinSide;
import org.emathp.model.JoinWhere;
import org.emathp.model.Query;
import org.emathp.planner.Planner;
import org.emathp.query.RequestContext;

/**
 * Hash-join federation: resolves each side via planner + {@link QueryExecutor}, then joins in
 * memory. Returns flattened {@link EngineRow}s only (qualified field names). Does <strong>not</strong>
 * attach planner/connector traces.
 *
 * <p>Does not apply federated {@code LIMIT} or cursor pagination — use {@link LimitExecutor} and
 * {@link org.emathp.federation.OffsetCursorPager}.
 */
public final class JoinExecutor {

    private final Planner planner;
    private final QueryExecutor singleExecutor;

    public JoinExecutor(Planner planner, QueryExecutor singleExecutor) {
        this.planner = planner;
        this.singleExecutor = singleExecutor;
    }

    /** Full join (every match); ignores {@link JoinQuery#limit()}, cursor, {@code pageSize} for sizing. */
    public List<EngineRow> materialize(
            RequestContext ctx, Map<String, Connector> connectorsByName, JoinQuery jq) {
        Connector leftConn = lookup(connectorsByName, jq.left());
        Connector rightConn = lookup(connectorsByName, jq.right());
        List<EngineRow> leftRows = executeSide(ctx, leftConn, jq.left(), jq.where());
        List<EngineRow> rightRows = executeSide(ctx, rightConn, jq.right(), jq.where());
        return joinRows(jq, leftRows, rightRows);
    }

    /** Hash-join materialized side rows (e.g. after snapshot side resolution). */
    public List<EngineRow> joinRows(JoinQuery jq, List<EngineRow> leftSideRows, List<EngineRow> rightSideRows) {
        return hashJoin(
                leftSideRows,
                rightSideRows,
                jq.on(),
                jq.left().alias(),
                jq.right().alias());
    }

    private static Connector lookup(Map<String, Connector> bySqlName, JoinSide side) {
        Connector c = bySqlName.get(side.connectorName());
        if (c == null) {
            throw new IllegalArgumentException(
                    "No connector registered for SQL identifier '" + side.connectorName()
                            + "' (referenced by " + side.alias() + ")");
        }
        return c;
    }

    private static Query perSideQuery(JoinSide side, JoinWhere where) {
        ComparisonExpr sideWhere =
                (where != null && where.alias().equals(side.alias())) ? where.predicate() : null;
        return new Query(List.of(), sideWhere, List.of(), null, null, null, null);
    }

    private List<EngineRow> executeSide(
            RequestContext ctx, Connector connector, JoinSide side, JoinWhere where) {
        Query q = perSideQuery(side, where);
        var plan = planner.plan(connector, q);
        return singleExecutor
                .execute(ctx, connector, plan.pushedQuery(), plan.residualOps(), null)
                .rows();
    }

    private static List<EngineRow> hashJoin(
            List<EngineRow> left,
            List<EngineRow> right,
            JoinPredicate on,
            String leftAlias,
            String rightAlias) {

        Map<Object, List<EngineRow>> rightByKey = new HashMap<>();
        for (EngineRow r : right) {
            Object key = RowFields.get(r, on.rightField());
            if (key == null) {
                continue;
            }
            rightByKey.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
        }

        List<EngineRow> out = new ArrayList<>();
        for (EngineRow l : left) {
            Object key = RowFields.get(l, on.leftField());
            if (key == null) {
                continue;
            }
            List<EngineRow> matches = rightByKey.get(key);
            if (matches == null) {
                continue;
            }
            for (EngineRow r : matches) {
                out.add(
                        EngineRow.merge(
                                EngineRow.qualify(leftAlias, l),
                                EngineRow.qualify(rightAlias, r)));
            }
        }
        return out;
    }
}
