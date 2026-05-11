package org.emathp.connector;

import java.util.Set;
import org.emathp.model.Operator;

/**
 * What a connector can push to its provider, expressed as a fixed set of booleans plus an
 * advertised set of supported predicate fields and operators.
 *
 * <p>{@code supportsLimit} is intentionally absent: the connector contract is cursor-pagination
 * only, and engine-side LIMIT enforcement is mandatory regardless. See ADR-0003.
 *
 * @param supportedFields fields the connector can accept in a <em>pushed predicate</em>
 *                        (i.e. inside a WHERE clause). Scope is deliberately narrow: this set
 *                        does not yet gate ORDER BY or PROJECTION fields. SaaS-style providers
 *                        commonly have different filterable vs. sortable vs. selectable field
 *                        sets, so this set will likely be split (e.g. into {@code filterable-},
 *                        {@code sortable-}, {@code projectableFields}) when a connector lands
 *                        whose sets actually differ. Until then, predicate pushdown is the only
 *                        consumer.
 * @param supportedOperators operators the connector can accept in a pushed predicate. Same
 *                           scope: WHERE only.
 */
public record CapabilitySet(
        boolean supportsFiltering,
        boolean supportsProjection,
        boolean supportsSorting,
        boolean supportsPagination,
        Set<String> supportedFields,
        Set<Operator> supportedOperators) {}
