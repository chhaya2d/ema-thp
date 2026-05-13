package org.emathp.snapshot.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Informational metadata only for a query snapshot directory — no continuation or freshness fields.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record QueryInfo(String queryHash, String scopeSegment, String normalizedQuery, String createdAt) {}
