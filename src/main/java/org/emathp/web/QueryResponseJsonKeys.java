package org.emathp.web;

/** Stable JSON member names produced by {@link UnifiedSnapshotWebRunner}. */
final class QueryResponseJsonKeys {

    private QueryResponseJsonKeys() {}

    /** Rows under {@code pages[0]} for multi-source federated payloads. */
    static final String PAGE_ROWS = "rows";

    /** True when a full-materialisation snapshot was served from disk without re-running providers. */
    static final String FULL_MATERIALIZATION_REUSE = "fullMaterializationReuse";
}
