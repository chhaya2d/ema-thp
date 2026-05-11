package org.emathp.snapshot.api;

import java.nio.file.Path;
import java.time.Duration;
import org.emathp.auth.UserContext;
import org.emathp.connector.Connector;
import org.emathp.model.Query;

/** One connector side + shared query snapshot directory + client freshness bound. */
public record SidePageRequest(
        UserContext user,
        Connector connector,
        Query plannerQuery,
        Path queryRoot,
        Duration maxStaleness,
        /**
         * When false, {@link org.emathp.snapshot.pipeline.SingleSourceSidePipeline} executes live and
         * skips connector chunk IO (used for join legs and for pure pushdown single-source legs).
         */
        boolean persistConnectorSnapshot) {

    public SidePageRequest(
            UserContext user,
            Connector connector,
            Query plannerQuery,
            Path queryRoot,
            Duration maxStaleness) {
        this(user, connector, plannerQuery, queryRoot, maxStaleness, true);
    }
}
