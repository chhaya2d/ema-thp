package org.emathp.connector.google.mock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.util.List;
import org.emathp.auth.UserContext;
import org.emathp.model.ComparisonExpr;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.Direction;
import org.emathp.model.Operator;
import org.emathp.model.OrderBy;
import org.emathp.model.EngineRow;
import org.emathp.model.SearchResult;
import org.junit.jupiter.api.Test;

class GoogleDriveConnectorTest {

    @Test
    void searchFiltersAndSorts() {
        // NOTE: ConnectorQuery has no limit field by design (ADR-0003); LIMIT is exclusively
        // engine-side. This test exercises only the connector boundary (filter + sort + page),
        // which is the connector's full contract.
        var connector = new GoogleDriveConnector();
        var query = new ConnectorQuery(
                List.of("title", "updatedAt"),
                new ComparisonExpr("updatedAt", Operator.GT, Instant.parse("2026-05-07T00:00:00Z")),
                List.of(new OrderBy("updatedAt", Direction.DESC)),
                null,
                null);

        SearchResult<EngineRow> result = connector.search(UserContext.anonymous(), query);

        List<EngineRow> rows = result.rows();
        assertEquals(3, rows.size());
        assertEquals("Quarterly Hiring Plan", rows.get(0).fields().get("title"));
        assertEquals("Roadmap Q3", rows.get(1).fields().get("title"));
        assertEquals("OAuth Rollout Checklist", rows.get(2).fields().get("title"));
        assertNull(result.nextCursor());
    }
}
