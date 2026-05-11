package org.emathp.connector.notion.api;

public record NotionSearchRequest(String filter, String startCursor, Integer pageSize) {}
