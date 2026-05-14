package org.emathp.snapshot.api;

import java.nio.file.Path;
import java.time.Duration;
import org.emathp.authz.TagAccessPolicy;
import org.emathp.connector.Connector;
import org.emathp.model.Query;
import org.emathp.query.RequestContext;

/**
 * One connector side + shared query snapshot directory + client freshness bound + optional tag
 * RBAC.
 *
 * <p>{@link RequestContext} carries identity (user, tenantId, traceId) the pipeline forwards to
 * {@link org.emathp.engine.QueryExecutor}. Pipeline-only fields (connector, planner query,
 * snapshot path, freshness hint, persistence flag, tag policy) stay on the request.
 */
public record SidePageRequest(
        RequestContext ctx,
        Connector connector,
        Query plannerQuery,
        Path queryRoot,
        Duration maxStaleness,
        boolean persistConnectorSnapshot,
        TagAccessPolicy tagPolicy) {

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
                TagAccessPolicy.unrestricted());
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
