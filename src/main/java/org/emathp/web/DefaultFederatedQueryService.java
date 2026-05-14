package org.emathp.web;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.emathp.auth.UserContext;
import org.emathp.authz.PrincipalRegistry;
import org.emathp.cache.QueryCacheScope;
import org.emathp.connector.Connector;
import org.emathp.engine.JoinExecutor;
import org.emathp.engine.QueryExecutor;
import org.emathp.model.ParsedQuery;
import org.emathp.parser.SQLParserService;
import org.emathp.planner.Planner;
import org.emathp.query.ApiException;
import org.emathp.query.ErrorCode;
import org.emathp.query.FederatedQueryRequest;
import org.emathp.query.FederatedQueryService;
import org.emathp.query.RequestContext;
import org.emathp.query.ResponseContext;
import org.emathp.ratelimit.RateLimitedException;
import org.emathp.snapshot.api.SnapshotQueryService;
import org.emathp.snapshot.model.SnapshotEnvironment;

/**
 * Default {@link FederatedQueryService}: parses SQL, delegates to {@link UnifiedSnapshotWebRunner},
 * and translates any boundary exception into a {@link ResponseContext.Outcome.Failure} so the HTTP
 * layer sees a uniform shape.
 */
public final class DefaultFederatedQueryService implements FederatedQueryService {

    private final SQLParserService parser;
    private final UnifiedSnapshotWebRunner coordinator;
    private final UserContext constructionUser;

    public DefaultFederatedQueryService(
            SQLParserService parser,
            Planner planner,
            QueryExecutor executor,
            JoinExecutor joinExecutor,
            List<Connector> connectors,
            Map<String, Connector> connectorsByName,
            UserContext constructionUser,
            SnapshotQueryService snapshotQueryService,
            SnapshotEnvironment snapshotEnv,
            int logicalPageSize,
            PrincipalRegistry principals) {
        this.parser = parser;
        this.constructionUser = constructionUser;
        this.coordinator =
                new UnifiedSnapshotWebRunner(
                        planner,
                        joinExecutor,
                        connectors,
                        connectorsByName,
                        snapshotQueryService,
                        snapshotEnv,
                        logicalPageSize,
                        principals);
    }

    @Override
    public ResponseContext execute(RequestContext ctx, FederatedQueryRequest request) {
        Objects.requireNonNull(ctx, "ctx");
        Objects.requireNonNull(request, "request");
        long t0 = System.nanoTime();
        try {
            ParsedQuery parsed = parser.parse(request.sql());
            JsonObject body = coordinator.runParsed(ctx, parsed, request);
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            body.addProperty("serverElapsedMs", elapsedMs);
            body.addProperty("traceId", ctx.traceId());
            return new ResponseContext(
                    ctx.traceId(), elapsedMs, new ResponseContext.Outcome.Success(body));
        } catch (RateLimitedException e) {
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            String scope = e.violatedScope() != null ? e.violatedScope().name() : null;
            return new ResponseContext(
                    ctx.traceId(),
                    elapsedMs,
                    new ResponseContext.Outcome.Failure(
                            ErrorCode.RATE_LIMIT_EXHAUSTED, e.getMessage(), e.retryAfterMs(), scope));
        } catch (ApiException e) {
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            return new ResponseContext(
                    ctx.traceId(),
                    elapsedMs,
                    new ResponseContext.Outcome.Failure(
                            e.code(), e.getMessage(), e.retryAfterMs(), e.violatedScope()));
        } catch (IllegalArgumentException e) {
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            return new ResponseContext(
                    ctx.traceId(),
                    elapsedMs,
                    new ResponseContext.Outcome.Failure(
                            ErrorCode.BAD_QUERY, e.getMessage(), null, null));
        } catch (IOException | RuntimeException e) {
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            String message = Objects.toString(e.getMessage(), e.getClass().getSimpleName());
            return new ResponseContext(
                    ctx.traceId(),
                    elapsedMs,
                    new ResponseContext.Outcome.Failure(ErrorCode.INTERNAL, message, null, null));
        }
    }

    @Override
    public QueryCacheScope defaultCacheScope() {
        return QueryCacheScope.from(constructionUser);
    }
}
