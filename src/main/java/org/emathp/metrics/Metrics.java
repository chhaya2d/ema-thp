package org.emathp.metrics;

/**
 * Process-wide Prometheus-style metrics. All metrics are static so call sites stay terse
 * ({@code Metrics.PROVIDER_CALLS.inc(connector, "ok")}). {@link #exposition()} renders the
 * full text payload for the {@code /metrics} endpoint.
 *
 * <p>Naming follows Prometheus conventions: snake_case, unit suffix on histograms
 * ({@code _seconds}, {@code _ms}), {@code _total} suffix on counters.
 */
public final class Metrics {

    private Metrics() {}

    // ---- Provider / connector ----

    public static final Counter PROVIDER_CALLS =
            new Counter(
                    "emathp_provider_calls_total",
                    "Provider search() calls grouped by outcome",
                    "connector",
                    "outcome");

    public static final Histogram PROVIDER_CALL_DURATION =
            new Histogram(
                    "emathp_provider_call_duration_seconds",
                    "Wall-clock duration of connector.search() calls",
                    Histogram.DEFAULT_SECONDS_BUCKETS,
                    "connector");

    // ---- Rate limiter ----

    public static final Counter RATE_LIMIT_DENIED =
            new Counter(
                    "emathp_rate_limit_denied_total",
                    "Hierarchical rate-limit denials grouped by violated scope",
                    "scope");

    // ---- Snapshot / freshness ----

    public static final Counter SNAPSHOT_CACHE_HITS =
            new Counter(
                    "emathp_snapshot_cache_hits_total",
                    "Per-side snapshot cache hits (serveMode=cached)",
                    "connector");

    public static final Counter SNAPSHOT_CACHE_MISSES =
            new Counter(
                    "emathp_snapshot_cache_misses_total",
                    "Per-side snapshot cache misses that ran the executor live",
                    "connector");

    public static final Counter SNAPSHOT_STALE_RESTARTS =
            new Counter(
                    "emathp_snapshot_stale_restarts_total",
                    "Query snapshot trees rebuilt because chunks exceeded maxStaleness");

    public static final Histogram RESPONSE_FRESHNESS =
            new Histogram(
                    "emathp_response_freshness_ms",
                    "Age of served data in ms (now − oldest chunk createdAt)",
                    Histogram.DEFAULT_MS_BUCKETS);

    // ---- Planner ----

    public static final Counter PLANNER_PUSHED =
            new Counter(
                    "emathp_planner_pushdown_total",
                    "Operations pushed to the connector",
                    "connector",
                    "op");

    public static final Counter PLANNER_RESIDUAL =
            new Counter(
                    "emathp_planner_residual_total",
                    "Operations that cascaded to residual execution in the engine",
                    "connector",
                    "op");

    // ---- Error vocabulary ----

    public static final Counter QUERY_ERRORS =
            new Counter(
                    "emathp_query_errors_total",
                    "Query failures grouped by ErrorCode",
                    "code");

    // ---- Authz / RLS ----

    public static final Counter TAG_FILTER_DROPS =
            new Counter(
                    "emathp_rows_filtered_by_tag_policy_total",
                    "Row drops by the tag policy (RLS) per role",
                    "role");

    /** Returns the Prometheus text exposition format for all metrics in this registry. */
    public static String exposition() {
        StringBuilder sb = new StringBuilder(4096);
        PROVIDER_CALLS.writeTo(sb);
        PROVIDER_CALL_DURATION.writeTo(sb);
        RATE_LIMIT_DENIED.writeTo(sb);
        SNAPSHOT_CACHE_HITS.writeTo(sb);
        SNAPSHOT_CACHE_MISSES.writeTo(sb);
        SNAPSHOT_STALE_RESTARTS.writeTo(sb);
        RESPONSE_FRESHNESS.writeTo(sb);
        PLANNER_PUSHED.writeTo(sb);
        PLANNER_RESIDUAL.writeTo(sb);
        QUERY_ERRORS.writeTo(sb);
        TAG_FILTER_DROPS.writeTo(sb);
        return sb.toString();
    }
}
