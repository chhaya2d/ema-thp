package org.emathp.connector.google.api;

public record GoogleSearchRequest(
        String q,
        String orderBy,
        Integer pageSize,
        String fields,
        String pageToken) {}
