package org.emathp.snapshot.api;

import java.nio.file.Path;
import java.time.Duration;
import org.emathp.authz.TagAccessPolicy;
import org.emathp.connector.Connector;
import org.emathp.model.Query;
import org.emathp.planner.PushdownPlan;
import org.emathp.query.RequestContext;

/**
 * One connector side + shared query snapshot directory + client freshness bound + optional tag
 * RBAC + optional pre-computed pushdown plan.
 *
 * <p>{@link RequestContext} carries identity (user, tenantId, traceId) the pipeline forwards to
 * {@link org.emathp.engine.QueryExecutor}. Pipeline-only fields (connector, planner query,
 * snapshot path, freshness hint, persistence flag, tag policy) stay on the request.
 *
 * <p>{@code plan} is an optimization: callers that already know the {@link PushdownPlan} (e.g.
 * the web runner computes it to decide {@code persistConnectorSnapshot} and to format the
 * response JSON) pass it here so the pipeline doesn't re-plan. When {@code null}, the pipeline
 * plans itself — back-compat for tests and CLI callers.
 */
public record SidePageRequest(
        RequestContext ctx,
        Connector connector,
        Query plannerQuery,
        Path queryRoot,
        Duration maxStaleness,
        boolean persistConnectorSnapshot,
        TagAccessPolicy tagPolicy,
        PushdownPlan plan) {

    public SidePageRequest(
            RequestContext ctx,
            Connector connector,
            Query plannerQuery,
            Path queryRoot,
            Duration maxStaleness,
            boolean persistConnectorSnapshot,
            TagAccessPolicy tagPolicy) {
        this(ctx, connector, plannerQuery, queryRoot, maxStaleness, persistConnectorSnapshot, tagPolicy, null);
    }

    public SidePageRequest(
            RequestContext ctx,
            Connector connector,
            Query plannerQuery,
            Path queryRoot,
            Duration maxStaleness,
            boolean persistConnectorSnapshot) {
        this(
                ctx,
                connector,
                plannerQuery,
                queryRoot,
                maxStaleness,
                persistConnectorSnapshot,
                TagAccessPolicy.unrestricted(),
                null);
    }

    public SidePageRequest(
            RequestContext ctx,
            Connector connector,
            Query plannerQuery,
            Path queryRoot,
            Duration maxStaleness) {
        this(ctx, connector, plannerQuery, queryRoot, maxStaleness, true);
    }
}
