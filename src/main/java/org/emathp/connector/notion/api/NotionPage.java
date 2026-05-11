package org.emathp.connector.notion.api;

import java.time.Instant;

public record NotionPage(String pageId, String title, Instant lastEditedTime, String url) {}
