package org.emathp.query;

/**
 * Server-populated observability fields surfaced under {@code Debug: true} request mode.
 *
 * <p>Always populated by the service layer (option A) — the HTTP layer decides whether to emit
 * them as response headers based on the caller's {@code Debug} request header. Service-layer
 * callers (CLI, tests) see these fields unconditionally and can assert on them directly without
 * firing HTTP.
 *
 * <p>Header mapping:
 * <ul>
 *   <li>{@link #snapshotPath()} → {@code X-Snapshot-Path}
 *   <li>{@link #queryHash()} → {@code X-Query-Hash}
 *   <li>{@link #tenantId()} → {@code X-Tenant-Id}
 *   <li>{@link #roleSlug()} → {@code X-Role}
 * </ul>
 *
 * <p>Any field may be {@code null} when unavailable (e.g. anonymous request → no tenant/role,
 * failure path → no snapshot path).
 *
 * @param snapshotPath filesystem path where chunks for this query live
 * @param queryHash    short hash linking the request to its log lines and cache directory
 * @param tenantId     tenant resolved from the principal lookup
 * @param roleSlug     active role for this request
 */
public record DebugResponseContext(
        String snapshotPath, String queryHash, String tenantId, String roleSlug) {}
