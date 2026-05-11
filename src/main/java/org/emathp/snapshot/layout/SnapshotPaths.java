package org.emathp.snapshot.layout;

import java.nio.file.Path;
import org.emathp.snapshot.model.SnapshotEnvironment;

public final class SnapshotPaths {

    private SnapshotPaths() {}

    /** Full engine-composed materialisation (see {@code SnapshotMaterializationPolicy}). */
    public static final String MATERIALIZED_QUERY_SEGMENT = "_materialized";

    public static String safeConnectorDirSegment(String connectorSource) {
        return connectorSource.replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    /** {@code data/<env>/} */
    public static Path environmentRoot(Path baseDataDir, SnapshotEnvironment env) {
        return baseDataDir.resolve(env.dirName());
    }
}
