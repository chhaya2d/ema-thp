package org.emathp.connector.google.demo;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.emathp.auth.UserContext;
import org.emathp.connector.CapabilitySet;
import org.emathp.connector.Connector;
import org.emathp.config.DemoConnectorDefaults;
import org.emathp.connector.google.api.GoogleDriveFile;
import org.emathp.connector.google.api.GoogleSearchResponse;
import org.emathp.connector.google.mock.GoogleQueryTranslator;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.EngineRow;
import org.emathp.model.Operator;
import org.emathp.model.SearchResult;

/**
 * Demo Google Drive {@link Connector}: join-oriented fixture + fixed delay from {@link
 * DemoConnectorDefaults}. Unit tests use {@link org.emathp.connector.google.mock.GoogleDriveConnector}.
 */
public final class DemoGoogleDriveConnector implements Connector {

    private static final CapabilitySet CAPABILITIES = new CapabilitySet(
            true,
            true,
            true,
            true,
            Set.of("title", "updatedAt"),
            Set.of(Operator.EQ, Operator.GT, Operator.LIKE));

    private final DemoGoogleDriveApi api = new DemoGoogleDriveApi();
    private final GoogleQueryTranslator translator = new GoogleQueryTranslator();

    @Override
    public String source() {
        return "google-drive";
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
        GoogleSearchResponse response = api.search(nativeRequest, userContext.userId());
        List<EngineRow> rows = response.files().stream().map(this::toEngineRow).toList();
        return new SearchResult<>(rows, response.nextPageToken());
    }

    private EngineRow toEngineRow(GoogleDriveFile file) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", file.id());
        m.put("source", source());
        m.put("title", file.name());
        m.put("updatedAt", file.updatedAt());
        m.put("url", file.webViewLink());
        m.put("owner", file.owner());
        m.put("createdAt", file.createdAt());
        m.put("modifiers", file.modifiers());
        return new EngineRow(m);
    }
}
