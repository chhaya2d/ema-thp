package org.emathp.connector.notion.mock;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.emathp.auth.UserContext;
import org.emathp.connector.CapabilitySet;
import org.emathp.connector.Connector;
import org.emathp.connector.mock.MockConnectorDevSettings;
import org.emathp.connector.notion.api.NotionPage;
import org.emathp.connector.notion.api.NotionSearchResponse;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.EngineRow;
import org.emathp.model.Operator;
import org.emathp.model.SearchResult;

/**
 * Mock Notion {@link Connector}: in-memory {@link MockNotionApi} + {@link NotionQueryTranslator}.
 * Filtering only at the planner level; pagination is natively supported by the provider but the
 * planner won't push it without a pushed sort, so {@code SearchResult.nextCursor} comes through as
 * null in practice for this engine.
 */
public final class NotionConnector implements Connector {

    // NOTE: supportsPagination=true even though Notion's pagination won't actually be pushed
    // by the planner — Notion can't push ORDER BY (supportsSorting=false), and the planner's
    // pagination rule requires a pushed sort to avoid non-deterministic page boundaries. The
    // capability reflects "the underlying API can paginate"; the planner derives whether
    // pushdown is *useful*.
    // NOTE: no supportsLimit field — connector contract is cursor-only (ADR-0003). LIMIT is
    // always engine-enforced.
    private static final CapabilitySet CAPABILITIES = new CapabilitySet(
            /* supportsFiltering  */ true,
            /* supportsProjection */ false,
            /* supportsSorting    */ false,
            /* supportsPagination */ true,
            Set.of("title", "updatedAt"),
            Set.of(Operator.EQ, Operator.GT, Operator.LT, Operator.LIKE));

    private final MockNotionApi api;
    private final NotionQueryTranslator translator = new NotionQueryTranslator();
    private final MockConnectorDevSettings dev;

    public NotionConnector() {
        this(new MockNotionApi(), MockConnectorDevSettings.none());
    }

    public NotionConnector(MockConnectorDevSettings dev) {
        this(new MockNotionApi(), dev);
    }

    public NotionConnector(MockNotionApi api) {
        this(api, MockConnectorDevSettings.none());
    }

    public NotionConnector(MockNotionApi api, MockConnectorDevSettings dev) {
        this.api = api;
        this.dev = dev;
    }

    @Override
    public String source() {
        return "notion";
    }

    @Override
    public int defaultFetchPageSize() {
        return 4;
    }

    @Override
    public CapabilitySet capabilities() {
        return CAPABILITIES;
    }

    @Override
    public SearchResult<EngineRow> search(UserContext userContext, ConnectorQuery query) {
        dev.sleepBeforeMockSearch();
        var nativeRequest = translator.translate(query);
        NotionSearchResponse response = api.search(nativeRequest, userContext.userId());
        List<EngineRow> rows = response.pages().stream().map(this::toEngineRow).toList();
        return new SearchResult<>(rows, response.nextCursor());
    }

    private EngineRow toEngineRow(NotionPage page) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", page.pageId());
        m.put("source", source());
        m.put("title", page.title());
        m.put("updatedAt", page.lastEditedTime());
        m.put("url", page.url());
        return new EngineRow(m);
    }
}
