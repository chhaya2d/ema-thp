package org.emathp.connector.google.mock;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.emathp.auth.UserContext;
import org.emathp.connector.CapabilitySet;
import org.emathp.connector.Connector;
import org.emathp.connector.google.api.GoogleDriveFile;
import org.emathp.connector.google.api.GoogleSearchResponse;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.EngineRow;
import org.emathp.model.Operator;
import org.emathp.model.SearchResult;

/**
 * Mock Google Drive {@link Connector}: in-memory {@link MockGoogleDriveApi} + {@link
 * GoogleQueryTranslator}. For OAuth-backed production Drive access use {@link
 * org.emathp.connector.google.real.RealGoogleDriveConnector}.
 */
public final class GoogleDriveConnector implements Connector {

    // NOTE: no supportsLimit field — connector contract is cursor-only (ADR-0003). LIMIT is
    // always engine-enforced regardless of provider capability, so there is no decision to
    // advertise. supportsPagination=true reflects that Drive's Files API natively paginates
    // (pageToken + pageSize); the engine maps that to its normalized cursor protocol.
    private static final CapabilitySet CAPABILITIES = new CapabilitySet(
            /* supportsFiltering  */ true,
            /* supportsProjection */ true,
            /* supportsSorting    */ true,
            /* supportsPagination */ true,
            Set.of("title", "updatedAt"),
            Set.of(Operator.EQ, Operator.GT, Operator.LIKE));

    private final MockGoogleDriveApi api;
    private final GoogleQueryTranslator translator = new GoogleQueryTranslator();

    public GoogleDriveConnector() {
        this(new MockGoogleDriveApi());
    }

    public GoogleDriveConnector(MockGoogleDriveApi api) {
        this.api = api;
    }

    @Override
    public String source() {
        return "google-drive";
    }

    @Override
    public int defaultFetchPageSize() {
        return 6;
    }

    @Override
    public CapabilitySet capabilities() {
        return CAPABILITIES;
    }

    @Override
    public SearchResult<EngineRow> search(UserContext userContext, ConnectorQuery query) {
        var nativeRequest = translator.translate(query);
        GoogleSearchResponse response = api.search(nativeRequest);
        List<EngineRow> rows = response.files().stream().map(this::toEngineRow).toList();
        return new SearchResult<>(rows, response.nextPageToken());
    }

    /** Flat API-shaped row; federation adds join aliases when merging sides. */
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
