package org.emathp.connector;

/**
 * Declares how connector-returned data varies with caller identity. Drives snapshot cache keying
 * — two callers whose data is invariant under a wider scope can safely share the same cache
 * directory.
 *
 * <p>For the THP prototype the snapshot pipeline consumes {@link Connector#isUserScopedData()}
 * (binary). Promotion to the full enum (e.g. per-tenant shared caches across roles for public
 * directories) is left as a follow-up — declaring the field now keeps the production architecture
 * legible in the interface.
 *
 * <ul>
 *   <li>{@link #USER} — each caller sees private data (personal Drive, Gmail, Notion workspace,
 *       Slack DMs). Cache must include {@code userId}.</li>
 *   <li>{@link #TENANT_ROLE} — tenant-wide data filtered by role (Salesforce with role hierarchy,
 *       Zendesk tickets, HRIS, Jira issues, GitHub org repos). Cache shared across users with the
 *       same {@code (tenant, role)}.</li>
 *   <li>{@link #TENANT} — tenant-wide data with no role filtering. Cache shared across all users
 *       in the tenant.</li>
 *   <li>{@link #PUBLIC} — caller-agnostic (public APIs, marketplaces). One shared cache.</li>
 * </ul>
 */
public enum DataScope {
    USER,
    TENANT_ROLE,
    TENANT,
    PUBLIC
}
