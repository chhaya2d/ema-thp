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
import org.emathp.metrics.Metrics;
import org.emathp.model.ParsedQuery;
import org.emathp.parser.SQLParserService;
import org.emathp.planner.Planner;
import org.emathp.query.ApiException;
import org.emathp.query.ErrorCode;
import org.emathp.query.FederatedQueryRequest;
import org.emathp.query.FederatedQueryService;
import org.emathp.query.RequestContext;
import org.emathp.query.ResponseContext;
import org.emathp.ratelimit.RateLimitPolicy;
import org.emathp.ratelimit.RateLimitResult;
import org.emathp.ratelimit.RateLimitedException;
import org.emathp.snapshot.api.SnapshotQueryService;
import org.emathp.snapshot.model.SnapshotEnvironment;

/**
 * Default {@link FederatedQueryService}: parses SQL, delegates to {@link UnifiedSnapshotWebRunner},
 * and translates any boundary exception into a {@link ResponseContext.Outcome.Failure} so the HTTP
 * layer sees a uniform shape.
 *
 * <p>This is the <strong>service-layer</strong> rate-limit gate: every request — including cache
 * hits — debits user + tenant buckets at entry, before any work. Honors Ema's own SLO and
 * per-user fairness independent of whether the request reaches an upstream provider. The
 * connector-layer rate limit (upstream protection) sits separately inside the engine page loop.
 */
public final class DefaultFederatedQueryService implements FederatedQueryService {

    private final SQLParserService parser;
    private final UnifiedSnapshotWebRunner coordinator;
    private final UserContext constructionUser;
    private final RateLimitPolicy serviceLimiter;

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
        this(parser, planner, executor, joinExecutor, connectors, connectorsByName,
                constructionUser, snapshotQueryService, snapshotEnv, logicalPageSize, principals,
                RateLimitPolicy.UNLIMITED);
    }

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
            PrincipalRegistry principals,
            RateLimitPolicy serviceLimiter) {
        this.parser = parser;
        this.constructionUser = constructionUser;
        this.serviceLimiter =
                serviceLimiter == null ? RateLimitPolicy.UNLIMITED : serviceLimiter;
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
            // Service-layer rate limit: debit user + tenant buckets BEFORE parsing or touching
            // the snapshot. Cache hits don't escape this check — every request consumes Ema's
            // own SLO budget. Anon callers (no tenantId) bypass.
            if (!ctx.isAnonymous()) {
                RateLimitResult sr =
                        serviceLimiter.tryAcquire(
                                new org.emathp.ratelimit.RequestContext(
                                        ctx.tenantId(),
                                        ctx.user().userId(),
                                        // service-layer keys don't depend on connector — pass a
                                        // sentinel so RequestContext's non-blank validation is
                                        // satisfied.
                                        "_service_"));
                if (!sr.allowed()) {
                    throw new RateLimitedException(sr);
                }
            }
            ParsedQuery parsed = parser.parse(request.sql());
            JsonObject body = coordinator.runParsed(ctx, parsed, request);
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            body.addProperty("serverElapsedMs", elapsedMs);
            body.addProperty("traceId", ctx.traceId());
            body.addProperty("rate_limit_status", "OK");
            Long freshnessMs = extractFreshnessMs(body);
            if (freshnessMs != null) {
                Metrics.RESPONSE_FRESHNESS.observe(freshnessMs.doubleValue());
            }
            return new ResponseContext(
                    ctx.traceId(),
                    elapsedMs,
                    freshnessMs,
                    "OK",
                    new ResponseContext.Outcome.Success(body));
        } catch (RateLimitedException e) {
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            String scope = e.violatedScope() != null ? e.violatedScope().name() : null;
            Metrics.QUERY_ERRORS.inc(ErrorCode.RATE_LIMIT_EXHAUSTED.name());
            return new ResponseContext(
                    ctx.traceId(),
                    elapsedMs,
                    null,
                    "EXHAUSTED",
                    new ResponseContext.Outcome.Failure(
                            ErrorCode.RATE_LIMIT_EXHAUSTED, e.getMessage(), e.retryAfterMs(), scope));
        } catch (ApiException e) {
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            String rls =
                    e.code() == ErrorCode.RATE_LIMIT_EXHAUSTED ? "EXHAUSTED" : "OK";
            Metrics.QUERY_ERRORS.inc(e.code().name());
            return new ResponseContext(
                    ctx.traceId(),
                    elapsedMs,
                    null,
                    rls,
                    new ResponseContext.Outcome.Failure(
                            e.code(), e.getMessage(), e.retryAfterMs(), e.violatedScope()));
        } catch (IllegalArgumentException e) {
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            Metrics.QUERY_ERRORS.inc(ErrorCode.BAD_QUERY.name());
            return new ResponseContext(
                    ctx.traceId(),
                    elapsedMs,
                    null,
                    "OK",
                    new ResponseContext.Outcome.Failure(
                            ErrorCode.BAD_QUERY, e.getMessage(), null, null));
        } catch (IOException | RuntimeException e) {
            long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
            String message = Objects.toString(e.getMessage(), e.getClass().getSimpleName());
            Metrics.QUERY_ERRORS.inc(ErrorCode.INTERNAL.name());
            return new ResponseContext(
                    ctx.traceId(),
                    elapsedMs,
                    null,
                    "OK",
                    new ResponseContext.Outcome.Failure(ErrorCode.INTERNAL, message, null, null));
        }
    }

    /**
     * Reads {@code freshness_ms} out of the runner's JSON body — set by {@code
     * UnifiedSnapshotWebRunner} as the age of the freshest used snapshot data. Returns {@code
     * null} when absent or JSON-null (e.g., a zero-row response that touched no chunks).
     */
    private static Long extractFreshnessMs(JsonObject body) {
        if (!body.has("freshness_ms") || body.get("freshness_ms").isJsonNull()) {
            return null;
        }
        return body.get("freshness_ms").getAsLong();
    }

    @Override
    public QueryCacheScope defaultCacheScope() {
        return QueryCacheScope.from(constructionUser);
    }
}
