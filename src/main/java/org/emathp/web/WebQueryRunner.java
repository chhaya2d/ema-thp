package org.emathp.web;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.emathp.auth.UserContext;
import org.emathp.cache.QueryCacheScope;
import org.emathp.config.WebDefaults;
import org.emathp.connector.Connector;
import org.emathp.engine.JoinExecutor;
import org.emathp.engine.QueryExecutor;
import org.emathp.model.ParsedQuery;
import org.emathp.parser.SQLParserService;
import org.emathp.planner.Planner;
import org.emathp.pagination.UiResponsePaging;
import org.emathp.snapshot.api.SnapshotQueryService;
import org.emathp.snapshot.model.SnapshotEnvironment;

/**
 * Parses SQL and delegates single/join JSON responses to {@link UnifiedSnapshotWebRunner}. Snapshot
 * disk IO is gated by {@code EMA_PUSHDOWN_SNAPSHOT_RUN} and {@link org.emathp.planner.PushdownPlan}.
 */
public final class WebQueryRunner {

    private final SQLParserService parser;
    private final UnifiedSnapshotWebRunner unified;
    private final UserContext user;

    public WebQueryRunner(
            SQLParserService parser,
            Planner planner,
            QueryExecutor executor,
            JoinExecutor joinExecutor,
            List<Connector> connectors,
            Map<String, Connector> connectorsByName,
            UserContext user,
            SnapshotQueryService snapshotQueryService,
            SnapshotEnvironment snapshotEnv,
            int uiPageSize) {
        this.parser = parser;
        this.user = user;
        this.unified =
                new UnifiedSnapshotWebRunner(
                        planner,
                        joinExecutor,
                        connectors,
                        connectorsByName,
                        user,
                        snapshotQueryService,
                        snapshotEnv,
                        uiPageSize);
    }

    public JsonObject run(String sql) {
        return run(sql, null, null, null, null, QueryCacheScope.from(user));
    }

    public JsonObject run(String sql, QueryCacheScope scope) {
        return run(sql, null, null, null, null, scope);
    }

    public JsonObject run(
            String sql,
            Integer pageNumber,
            String uiCursorOffset,
            Integer requestPageSize,
            Duration maxStaleness,
            QueryCacheScope scope) {
        try {
            ParsedQuery parsed = parser.parse(sql);
            return unified.runParsed(parsed, pageNumber, uiCursorOffset, requestPageSize, maxStaleness);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public QueryCacheScope cacheScope() {
        return QueryCacheScope.from(user);
    }

    public static int parseUiOffset(String uiCursorOffset) {
        return UiResponsePaging.parseUiOffset(uiCursorOffset);
    }
}
