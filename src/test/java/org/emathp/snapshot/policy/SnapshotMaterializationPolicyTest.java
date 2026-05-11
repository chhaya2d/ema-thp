package org.emathp.snapshot.policy;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.emathp.model.JoinPredicate;
import org.emathp.model.JoinQuery;
import org.emathp.model.JoinSide;
import org.emathp.model.ParsedQuery;
import org.emathp.model.ResidualOps;
import org.emathp.model.OrderBy;
import org.emathp.model.ComparisonExpr;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.Direction;
import org.emathp.model.Operator;
import org.emathp.parser.SQLParserService;
import org.emathp.planner.PushdownPlan;
import org.junit.jupiter.api.Test;

class SnapshotMaterializationPolicyTest {

    @Test
    void standaloneLeg_persistsChunksOnlyWhenResidual() {
        ResidualOps withOrderBy = new ResidualOps(null, List.of(new OrderBy("updatedAt", Direction.DESC)));
        PushdownPlan withRes =
                new PushdownPlan(
                        new ConnectorQuery(List.of(), null, List.of(), null, null),
                        List.of(),
                        withOrderBy,
                        true);

        PushdownPlan noRes =
                new PushdownPlan(
                        new ConnectorQuery(List.of(), null, List.of(), null, null),
                        List.of(),
                        ResidualOps.NONE,
                        true);

        assertTrue(SnapshotMaterializationPolicy.persistConnectorSideChunks(withRes));
        assertFalse(SnapshotMaterializationPolicy.persistConnectorSideChunks(noRes));
    }

    @Test
    void persistedFlagOff_neverStandaloneChunks() {
        PushdownPlan p =
                new PushdownPlan(
                        new ConnectorQuery(List.of(), null, List.of(), null, null),
                        List.of(),
                        new ResidualOps(
                                new ComparisonExpr("title", Operator.EQ, "x"), List.of()),
                        false);

        assertFalse(SnapshotMaterializationPolicy.persistConnectorSideChunks(p));
    }

    @Test
    void requiresFullMaterialization_trueForJoin_falseForSingle() throws Exception {
        SQLParserService parser = new SQLParserService();
        ParsedQuery join =
                parser.parse("SELECT g.title, n.title FROM google g JOIN notion n ON g.title = n.title");
        assertTrue(SnapshotMaterializationPolicy.requiresFullMaterialization(join));

        ParsedQuery single = parser.parse("SELECT title FROM google");
        assertFalse(SnapshotMaterializationPolicy.requiresFullMaterialization(single));
    }

    @Test
    void requiresFullMaterialization_joinSideOnlyStillJoinQuery() {
        JoinQuery jq =
                new JoinQuery(
                        new JoinSide("google", "g"),
                        new JoinSide("notion", "n"),
                        new JoinPredicate("google", "title", "notion", "title"),
                        null,
                        List.of(),
                        null,
                        null,
                        null);
        assertTrue(SnapshotMaterializationPolicy.requiresFullMaterialization(jq));
    }
}
