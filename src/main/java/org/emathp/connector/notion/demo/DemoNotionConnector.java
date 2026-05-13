package org.emathp.connector.notion.demo;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.emathp.auth.UserContext;
import org.emathp.connector.CapabilitySet;
import org.emathp.connector.Connector;
import org.emathp.config.DemoConnectorDefaults;
import org.emathp.connector.demo.DemoTags;
import org.emathp.connector.notion.api.NotionPage;
import org.emathp.connector.notion.api.NotionSearchResponse;
import org.emathp.connector.notion.mock.NotionQueryTranslator;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.EngineRow;
import org.emathp.model.Operator;
import org.emathp.model.SearchResult;

/**
 * Demo Notion {@link Connector}: aligned titles with {@link org.emathp.connector.google.demo.DemoGoogleDriveConnector}.
 */
public final class DemoNotionConnector implements Connector {

    private static final CapabilitySet CAPABILITIES = new CapabilitySet(
            true,
            false,
            false,
            true,
            Set.of("title", "updatedAt"),
            Set.of(Operator.EQ, Operator.GT, Operator.LT, Operator.LIKE));

    private final DemoNotionApi api = new DemoNotionApi();
    private final NotionQueryTranslator translator = new NotionQueryTranslator();

    @Override
    public String source() {
        return "notion";
    }

    @Override
    public int defaultFetchPageSize() {
        return DemoConnectorDefaults.PROVIDER_PAGE_SIZE;
    }

    @Override
    public CapabilitySet capabilities() {
        return CAPABILITIES;
    }

    @Override
    public SearchResult<EngineRow> search(UserContext userContext, ConnectorQuery query) {
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
        m.put("tags", DemoTags.forNotionPageId(page.pageId()));
        return new EngineRow(m);
    }
}
