package org.emathp.snapshot.api;

import java.nio.file.Path;
import java.time.Duration;
import org.emathp.auth.UserContext;
import org.emathp.connector.Connector;
import org.emathp.engine.policy.TagAccessPolicy;
import org.emathp.model.Query;

/** One connector side + shared query snapshot directory + client freshness bound + optional tag RBAC. */
public record SidePageRequest(
        UserContext user,
        Connector connector,
        Query plannerQuery,
        Path queryRoot,
        Duration maxStaleness,
        boolean persistConnectorSnapshot,
        TagAccessPolicy tagPolicy) {

    public SidePageRequest(
            UserContext user,
            Connector connector,
            Query plannerQuery,
            Path queryRoot,
            Duration maxStaleness,
            boolean persistConnectorSnapshot) {
        this(
                user,
                connector,
                plannerQuery,
                queryRoot,
                maxStaleness,
                persistConnectorSnapshot,
                TagAccessPolicy.unrestricted());
    }

    public SidePageRequest(UserContext user, Connector connector, Query plannerQuery, Path queryRoot, Duration maxStaleness) {
        this(user, connector, plannerQuery, queryRoot, maxStaleness, true);
    }
}
