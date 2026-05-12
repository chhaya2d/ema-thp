package org.emathp.web;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.emathp.auth.ConnectorAccount;
import org.emathp.cache.QueryCacheScope;
import java.nio.file.Path;
import java.time.Duration;
import org.emathp.snapshot.adapters.fs.FsSnapshotStore;
import org.emathp.snapshot.adapters.time.SystemClock;
import org.emathp.snapshot.api.SnapshotQueryService;
import org.emathp.snapshot.model.SnapshotEnvironment;
import org.emathp.config.OAuthDefaults;
import org.emathp.config.RuntimeEnv;
import org.emathp.config.WebDefaults;
import org.emathp.auth.UserContext;
import org.emathp.connector.Connector;
import org.emathp.connector.google.demo.DemoGoogleDriveConnector;
import org.emathp.connector.google.mock.GoogleDriveConnector;
import org.emathp.connector.mock.MockConnectorDevSettings;
import org.emathp.connector.mock.MockDemoUsers;
import org.emathp.connector.google.real.GoogleApiClient;
import org.emathp.connector.google.real.GoogleTokenStore;
import org.emathp.connector.google.real.RealGoogleDriveConnector;
import org.emathp.connector.notion.demo.DemoNotionConnector;
import org.emathp.connector.notion.mock.NotionConnector;
import org.emathp.engine.JoinExecutor;
import org.emathp.engine.QueryExecutor;
import org.emathp.oauth.GoogleOAuthService;
import org.emathp.oauth.Pkce;
import org.emathp.oauth.TokenEncryptor;
import org.emathp.parser.SQLParserService;
import org.emathp.planner.Planner;
import org.h2.jdbcx.JdbcDataSource;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * Embedded HTTP server: hybrid SQL playground (optional live Google + OAuth, mock Google, demo
 * connectors). Live stack starts at boot when {@code CONNECTOR_TOKEN_KEY} and Google OAuth client
 * env vars are set and the token database is reachable; otherwise mock and demo still work.
 * Binds to loopback only.
 */
public final class DemoWebServer {

    private static final Gson GSON = new Gson();

    private static final String LIVE_STACK_CONFIG_HELP =
            "Live mode requires CONNECTOR_TOKEN_KEY, GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, "
                    + "and a working token database (Postgres or EMA_DEV_H2=true). Set them in OS env or .env and "
                    + "restart the server.";

    private record LiveWebStack(
            WebQueryRunner runner,
            GoogleTokenStore store,
            GoogleOAuthService oauth,
            TokenEncryptor encryptor) {}

    private static boolean liveConnectorEnvLooksComplete() {
        String key = RuntimeEnv.getOrNull("CONNECTOR_TOKEN_KEY");
        String cid = RuntimeEnv.getOrNull("GOOGLE_OAUTH_CLIENT_ID");
        String sec = RuntimeEnv.getOrNull("GOOGLE_OAUTH_CLIENT_SECRET");
        return key != null
                && !key.isBlank()
                && cid != null
                && !cid.isBlank()
                && sec != null
                && !sec.isBlank();
    }

    /**
     * Builds the live Google {@link WebQueryRunner} when env and DB init succeed; otherwise {@code
     * null} (mock and demo remain available).
     */
    private static LiveWebStack tryCreateLiveWebStack(WebEnv env, SQLParserService parser) {
        if (!liveConnectorEnvLooksComplete()) {
            return null;
        }
        try {
            TokenEncryptor encryptor = TokenEncryptor.fromConnectEnv();
            DataSource ds = createDataSource(env);
            GoogleTokenStore store = new GoogleTokenStore(ds);
            store.ensureTable();
            GoogleOAuthService oauth =
                    new GoogleOAuthService(env.googleClientId(), env.googleClientSecret(), env.oauthRedirectUri());
            RealGoogleDriveConnector realGoogle =
                    new RealGoogleDriveConnector(store, encryptor, oauth, new GoogleApiClient());
            Map<String, Connector> connectorsByName = new LinkedHashMap<>();
            connectorsByName.put("google", realGoogle);
            MockConnectorDevSettings mockDev = MockConnectorDevSettings.compiled();
            connectorsByName.put("notion", new NotionConnector(mockDev));
            Planner planner = new Planner();
            QueryExecutor executor = new QueryExecutor();
            JoinExecutor joinExecutor = new JoinExecutor(planner, executor);
            UserContext user = new UserContext(env.demoUserId());
            SnapshotQueryService snapshots =
                    new SnapshotQueryService(
                            planner,
                            executor,
                            new FsSnapshotStore(Path.of("data")),
                            new SystemClock(),
                            WebDefaults.snapshotChunkFreshness());
            WebQueryRunner liveQueryRunner =
                    new WebQueryRunner(
                            parser,
                            planner,
                            executor,
                            joinExecutor,
                            List.copyOf(connectorsByName.values()),
                            connectorsByName,
                            user,
                            snapshots,
                            SnapshotEnvironment.PROD,
                            WebDefaults.UI_QUERY_PAGE_SIZE);
            return new LiveWebStack(liveQueryRunner, store, oauth, encryptor);
        } catch (Exception e) {
            System.err.println("Live Google stack init failed (mock and demo still work): " + e.getMessage());
            e.printStackTrace(System.err);
            return null;
        }
    }

    private static void sendLiveStackUnavailable(HttpExchange ex, boolean json) throws IOException {
        if (json) {
            JsonObject hint = new JsonObject();
            hint.addProperty("ok", false);
            hint.addProperty("error", LIVE_STACK_CONFIG_HELP);
            sendJson(ex, 503, hint);
        } else {
            sendBytes(ex, 503, "text/html; charset=utf-8", "<p>" + esc(LIVE_STACK_CONFIG_HELP) + "</p>");
        }
    }

    private static void liveOAuthDisabled(HttpExchange exchange) throws IOException {
        sendBytes(
                exchange,
                503,
                "text/html; charset=utf-8",
                "<p>" + esc(LIVE_STACK_CONFIG_HELP) + "</p><p><a href=\"/\">Home</a></p>");
    }

    private static String mockUserSelectSection() {
        return "<p><label>Mock / demo user <select id=\"mockUserSelect\">"
                + MockDemoUsers.htmlOptionElements()
                + "</select></label> <span style=\"color:#555\">(same ids for mock and demo connectors)</span></p>\n";
    }

    private static String hybridConnectorModeRadios(boolean liveConfigured) {
        if (liveConfigured) {
            return "<p><label><input type=\"radio\" name=\"connectorMode\" value=\"live\" checked> "
                    + "Live (OAuth Google + mock Notion)</label> "
                    + "<label><input type=\"radio\" name=\"connectorMode\" value=\"mock\"> "
                    + "Mock Google + mock Notion</label> "
                    + "<label><input type=\"radio\" name=\"connectorMode\" value=\"demo\"> "
                    + "Demo Google + demo Notion (compiled ~5s search delay per connector)</label></p>\n";
        }
        return "<p><label><input type=\"radio\" name=\"connectorMode\" value=\"live\" disabled "
                + "title=\"Set CONNECTOR_TOKEN_KEY, GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, and DB; restart.\"> "
                + "Live (unavailable)</label> "
                + "<label><input type=\"radio\" name=\"connectorMode\" value=\"mock\" checked> "
                + "Mock Google + mock Notion</label> "
                + "<label><input type=\"radio\" name=\"connectorMode\" value=\"demo\"> "
                + "Demo Google + demo Notion (compiled ~5s search delay per connector)</label></p>\n";
    }

    private static final String PAYLOAD_MOCK_USER_JS =
            """
                    var sel = document.getElementById('mockUserSelect');
                    if (sel && sel.value) payload.mockUserId = sel.value;
            """;

    private static final String PAYLOAD_HYBRID_MODE_JS =
            """
                    var modeEl = document.querySelector('input[name="connectorMode"]:checked');
                    if (modeEl) payload.connectorMode = modeEl.value;
            """;

    private static final String DEMO_PRESET_BOOTSTRAP_JS =
            """
                          (function(){
                            var el = document.getElementById('ema-demo-presets');
                            if (!el) return;
                            var presets = JSON.parse(el.textContent);
                            var sel = document.getElementById('demoQueryPreset');
                            if (!sel) return;
                            sel.addEventListener('change', function(){
                              var k = this.value;
                              if (!k || !presets[k]) return;
                              var ta = document.getElementById('sql');
                              if (ta) ta.value = presets[k];
                              var r = document.querySelector('input[name="connectorMode"][value="demo"]');
                              if (r) r.checked = true;
                              if (typeof applyModePageSizeDefault === 'function') applyModePageSizeDefault();
                            });
                          })();
                    """;

    private static String demoQueryPresetsSection() {
        String json = GSON.toJson(DemoQueryPresets.presetMap());
        return "<p><label>Demo SQL presets <select id=\"demoQueryPreset\"><option value=\"\">— custom SQL —</option>"
                + "<option value=\"singleGoogle\">Demo: ORDER BY — inspect <strong>google-drive</strong> side</option>"
                + "<option value=\"singleNotion\">Demo: ORDER BY — inspect <strong>notion</strong> side (engine sort)</option>"
                + "<option value=\"join\">Demo: join on matching title</option>"
                + "</select></label> "
                + "<span style=\"color:#555\">Sets SQL + Demo mode; pagination uses UI page size below.</span></p>\n"
                + "<script type=\"application/json\" id=\"ema-demo-presets\">"
                + json
                + "</script>\n";
    }

    /**
     * Home page: SQL playground with prev/next UI paging (POST JSON to /api/query). Placeholders:
     * ${H1}, ${INTRO}, ${DEFAULT_SQL}, ${DEFAULT_UI_PS}, ${MAX_UI_PS}, ${CONNECTOR_CONTROLS},
     * ${CONNECTOR_PAYLOAD_LINES}, ${DEMO_QUERY_PRESETS}, ${MOCK_UI_PS}, ${DEMO_UI_PS}, ${LIVE_UI_PS},
     * ${HYBRID_PAGE_SIZE_WIRE}, ${DEMO_PRESET_BOOTSTRAP}.
     */
    private static final String FEDERATION_QUERY_PLAYGROUND_TEMPLATE =
            """
            <!DOCTYPE html>
            <html><head><meta charset="utf-8"><title>EmaTHP demo</title></head><body>
            <h1>${H1}</h1>
            ${INTRO}
            <h2>Run SQL</h2>
            <p>Uses <code>fetch</code> to POST JSON to <code>/api/query</code> with optional <code>cursor</code> (row offset),
            <code>pageSize</code>, and <code>maxStaleness</code> (ISO-8601 duration, e.g. <code>PT10M</code>) to bound snapshot reuse.
            The snapshot layer tries disk first; on miss it runs <code>QueryExecutor</code> once and caches the full side result.</p>
            <label>SQL<br><textarea id="sql" rows="12" cols="100" wrap="off">${DEFAULT_SQL}</textarea></label>
            ${CONNECTOR_CONTROLS}
            ${DEMO_QUERY_PRESETS}
            <p><label>UI page size <input type="number" id="pageSize" min="1" max="${MAX_UI_PS}" value="${DEFAULT_UI_PS}"></label>
            <span style="color:#555">(defaults: mock <code>${MOCK_UI_PS}</code>, demo <code>${DEMO_UI_PS}</code>, live <code>${LIVE_UI_PS}</code>)</span></p>
            <p>
              <button type="button" id="runBtn">Run</button>
              <button type="button" id="prevBtn" disabled>Prev</button>
              <button type="button" id="nextBtn" disabled>Next</button>
              <span id="pageInfo"></span> <span id="status"></span>
            </p>
            <h3>Snapshot / fetch view</h3>
            <div id="humanOut"></div>
            <h3>Raw JSON</h3>
            <pre id="out"></pre>
            <script>
            (function() {
              const runBtn = document.getElementById('runBtn');
              const prevBtn = document.getElementById('prevBtn');
              const nextBtn = document.getElementById('nextBtn');
              const pageInfo = document.getElementById('pageInfo');
              const status = document.getElementById('status');
              const out = document.getElementById('out');
              const humanOut = document.getElementById('humanOut');
              let lastNext = null;
              let lastPrev = null;
              function escHtml(s) {
                return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
              }
              function rowFields(row) {
                var f = row.fields || {};
                var keys = Object.keys(f);
                return { keys: keys, f: f };
              }
              function renderRowTable(rows) {
                if (!rows || !rows.length) return '<p><em>(no rows in this UI window)</em></p>';
                var first = rowFields(rows[0]);
                var keys = first.keys;
                var h = '<table border="1" cellpadding="4" cellspacing="0"><thead><tr>';
                for (var i = 0; i < keys.length; i++) h += '<th>' + escHtml(keys[i]) + '</th>';
                h += '</tr></thead><tbody>';
                for (var r = 0; r < rows.length; r++) {
                  var rf = rowFields(rows[r]);
                  h += '<tr>';
                  for (var c = 0; c < keys.length; c++) {
                    var k = keys[c];
                    var v = rf.f[k];
                    h += '<td>' + escHtml(v == null ? '' : (typeof v === 'object' ? JSON.stringify(v) : String(v))) + '</td>';
                  }
                  h += '</tr>';
                }
                h += '</tbody></table>';
                return h;
              }
              function renderSideFetchSection(side) {
                var conn = side.connector;
                var dir = side.connectorSnapshotDir || '';
                var reuse = side.snapshotReuseNoProviderCall === true;
                var html = '<h4>' + escHtml(conn) + '</h4>';
                if (reuse) {
                  html += '<p><strong>Cached</strong> — snapshot reuse (no provider call for this side).</p>';
                  html += '<p>Snapshot dir: <code>' + escHtml(dir) + '</code></p>';
                } else {
                  html += '<p><strong>Live</strong> — resolver ran the connector pipeline.</p>';
                  html += '<p>Provider fetches (this request): ' + escHtml(String(side.providerFetchesThisRequest)) +
                    ', continuations: ' + escHtml(String(side.continuationFetchesThisRequest)) + '</p>';
                  html += '<p>Connector snapshot dir: <code>' + escHtml(dir) + '</code></p>';
                  var calls = (side.execution && side.execution.calls) ? side.execution.calls : [];
                  if (calls.length) {
                    html += '<p><em>Per connector fetch (engine page loop)</em></p><ol>';
                    for (var i = 0; i < calls.length; i++) {
                      var c = calls[i];
                      html += '<li>Fetch #' + (i + 1) + ': cursor=<code>' + escHtml(c.cursor) + '</code>, rows=' +
                        escHtml(String(c.rowsReturned)) + ', next=<code>' + escHtml(c.nextCursor) + '</code> <strong>(live)</strong></li>';
                    }
                    html += '</ol>';
                  } else {
                    html += '<p><em>No per-fetch call list (e.g. in-memory path or single synthetic result).</em></p>';
                  }
                }
                return html;
              }
              function renderHuman(j) {
                if (!humanOut) return;
                humanOut.innerHTML = '';
                if (!j || j.ok === false) {
                  humanOut.innerHTML = '<p><em>No snapshot view (error payload).</em></p>';
                  return;
                }
                var h = '';
                h += '<p>Query snapshot root: <code>' + escHtml(j.snapshotPath || '') + '</code>';
                if (j.freshnessDecision) h += ' — freshness: <strong>' + escHtml(j.freshnessDecision) + '</strong>';
                h += '</p>';
                if (j.kind === 'single' && j.sides) {
                  h += '<p>UI page <strong>' + escHtml(String(j.uiPageIndex)) + '</strong> of ~' + escHtml(String(j.uiApproxPageCount)) +
                    ', window size <strong>' + escHtml(String(j.uiPageSize)) + '</strong> (rows shown per connector).</p>';
                  for (var s = 0; s < j.sides.length; s++) {
                    var side = j.sides[s];
                    h += renderSideFetchSection(side);
                    var rows = side.execution && side.execution.rows ? side.execution.rows : [];
                    var total = side.execution && side.execution.uiRowTotal != null ? side.execution.uiRowTotal : rows.length;
                    h += '<p>Rows in this UI window: ' + rows.length + (total != null ? ' (connector total before UI slice: ' + total + ')' : '') + '</p>';
                    h += renderRowTable(rows);
                  }
                } else if (j.kind === 'join' && j.pages && j.pages.length) {
                  var pg = j.pages[0];
                  var rows = pg.rows || [];
                  var jrt = (pg.uiRowTotal != null) ? pg.uiRowTotal : rows.length;
                  h += '<p>Join materialization reuse: <strong>' + escHtml(String(j.fullMaterializationReuse)) + '</strong></p>';
                  h += '<p>UI page <strong>' + escHtml(String(j.uiPageIndex)) + '</strong>, window size <strong>' +
                    escHtml(String(j.uiPageSize)) + '</strong>, rows in this window: ' + rows.length +
                    ' (materialized total before UI slice: ' + jrt + ')</p>';
                  if (j.fullMaterializationReuse === true) {
                    var sp = String(j.snapshotPath || '').replace(/\\\\/g, '/');
                    h += '<p><strong>Cached</strong> join snapshot.</p>';
                    if (sp) {
                      h += '<p>Materialized cache path: <code>' + escHtml(sp + '/_materialized') + '</code></p>';
                    }
                  } else {
                    h += '<p><strong>Live</strong> join — engine materialised provider rows for this request.</p>';
                  }
                  h += renderRowTable(rows);
                } else {
                  h += '<p><em>Snapshot view is optimised for single-source or join JSON shapes.</em></p>';
                }
                humanOut.innerHTML = h;
              }
              async function runQuery(cursor) {
                status.textContent = '';
                const t0 = performance.now();
                const sql = document.getElementById('sql').value;
                const pageSize = parseInt(document.getElementById('pageSize').value, 10);
                const payload = { sql: sql, pageSize: pageSize };
                ${CONNECTOR_PAYLOAD_LINES}
                if (cursor != null && cursor !== undefined) {
                  payload.cursor = String(cursor);
                }
                const res = await fetch('/api/query', {
                  method: 'POST',
                  headers: { 'Content-Type': 'application/json' },
                  body: JSON.stringify(payload)
                });
                const text = await res.text();
                let j;
                try {
                  j = JSON.parse(text);
                } catch (e) {
                  out.textContent = text;
                  humanOut.innerHTML = '';
                  status.textContent = 'HTTP ' + res.status;
                  return;
                }
                out.textContent = JSON.stringify(j, null, 2);
                renderHuman(j);
                var ms = Math.round(performance.now() - t0);
                if (!res.ok) {
                  status.textContent = 'HTTP ' + res.status + ' — ' + ms + ' ms';
                  return;
                }
                if (j.uiPagingSupported) {
                  pageInfo.textContent =
                    'Page ' + j.uiPageIndex + ' of ~' + j.uiApproxPageCount
                    + ' (size ' + j.uiPageSize + ', freshness=' + j.freshnessDecision + ')';
                  lastNext = j.uiNextCursor;
                  lastPrev = j.uiPrevCursor;
                  nextBtn.disabled = lastNext == null;
                  prevBtn.disabled = lastPrev == null;
                } else {
                  pageInfo.textContent = 'UI paging applies to single-source queries only.';
                  nextBtn.disabled = true;
                  prevBtn.disabled = true;
                }
                status.textContent = 'Round-trip ' + ms + ' ms';
              }
              runBtn.onclick = function() { runQuery(null); };
              nextBtn.onclick = function() { if (lastNext != null) runQuery(lastNext); };
              prevBtn.onclick = function() { if (lastPrev != null) runQuery(lastPrev); };
              ${HYBRID_PAGE_SIZE_WIRE}
              ${DEMO_PRESET_BOOTSTRAP}
            })();
            </script>
            </body></html>
            """;

    private static final ConcurrentHashMap<String, OAuthPending> OAUTH_STATE = new ConcurrentHashMap<>();

    private DemoWebServer() {}

    public static void main(String[] args) throws Exception {
        run();
    }

    /**
     * Starts the demo web server (blocking). See {@link WebEnv#load()} for environment variables.
     */
    public static void run() throws Exception {
        RuntimeEnv.loadDotEnv();
        WebEnv env = WebEnv.load();
        runHybridDemoServer(env);
    }

    private static void runHybridDemoServer(WebEnv env) throws Exception {
        SQLParserService parser = new SQLParserService();
        LiveWebStack live = tryCreateLiveWebStack(env, parser);
        WebQueryRunner liveQueryRunner = live != null ? live.runner() : null;
        GoogleTokenStore store = live != null ? live.store() : null;

        MockConnectorDevSettings mockDev = MockConnectorDevSettings.compiled();
        Map<String, Connector> mockConnectorsByName = new LinkedHashMap<>();
        mockConnectorsByName.put("google", new GoogleDriveConnector(mockDev));
        mockConnectorsByName.put("notion", new NotionConnector(mockDev));

        Planner mockPlanner = new Planner(true);
        QueryExecutor mockExecutor = new QueryExecutor();
        JoinExecutor mockJoinExecutor = new JoinExecutor(mockPlanner, mockExecutor);
        SnapshotQueryService mockSnapshots =
                new SnapshotQueryService(
                        mockPlanner,
                        mockExecutor,
                        new FsSnapshotStore(Path.of("data")),
                        new SystemClock(),
                        WebDefaults.snapshotChunkFreshness());
        WebQueryRunner mockQueryRunner =
                new WebQueryRunner(
                        parser,
                        mockPlanner,
                        mockExecutor,
                        mockJoinExecutor,
                        List.copyOf(mockConnectorsByName.values()),
                        mockConnectorsByName,
                        UserContext.anonymous(),
                        mockSnapshots,
                        SnapshotEnvironment.TEST,
                        WebDefaults.UI_QUERY_PAGE_SIZE_MOCK);

        Planner demoPlanner = new Planner(true);
        QueryExecutor demoExecutor = new QueryExecutor();
        JoinExecutor demoJoinExecutor = new JoinExecutor(demoPlanner, demoExecutor);
        SnapshotQueryService demoSnapshots =
                new SnapshotQueryService(
                        demoPlanner,
                        demoExecutor,
                        new FsSnapshotStore(Path.of("data")),
                        new SystemClock(),
                        WebDefaults.snapshotChunkFreshness());
        Map<String, Connector> demoConnectorsByName = new LinkedHashMap<>();
        demoConnectorsByName.put("google", new DemoGoogleDriveConnector());
        demoConnectorsByName.put("notion", new DemoNotionConnector());
        WebQueryRunner demoQueryRunner =
                new WebQueryRunner(
                        parser,
                        demoPlanner,
                        demoExecutor,
                        demoJoinExecutor,
                        List.copyOf(demoConnectorsByName.values()),
                        demoConnectorsByName,
                        UserContext.anonymous(),
                        demoSnapshots,
                        SnapshotEnvironment.TEST,
                        WebDefaults.UI_QUERY_PAGE_SIZE_DEMO);

        boolean liveConfigured = live != null;
        HttpServer http =
                HttpServer.create(
                        new InetSocketAddress(
                                java.net.InetAddress.getByName(WebDefaults.HTTP_BIND_ADDRESS), env.webPort()),
                        0);
        http.createContext("/", exchange -> handleRoot(exchange, env, liveConfigured));
        http.createContext("/health", exchange -> sendBytes(exchange, 200, "text/plain", "ok"));
        if (live != null) {
            http.createContext("/oauth/google/start", exchange -> handleOAuthStart(exchange, live.oauth()));
            http.createContext(
                    "/oauth/google/callback",
                    exchange -> handleOAuthCallback(exchange, live.oauth(), live.store(), live.encryptor(), env));
        } else {
            http.createContext("/oauth/google/start", exchange -> liveOAuthDisabled(exchange));
            http.createContext("/oauth/google/callback", exchange -> liveOAuthDisabled(exchange));
        }
        http.createContext(
                "/api/query",
                exchange ->
                        handleQuery(
                                exchange, liveQueryRunner, mockQueryRunner, demoQueryRunner, env.demoUserId(), store));

        http.setExecutor(null);
        http.start();
        System.out.println("Demo web UI: http://localhost:" + env.webPort() + "/");
        if (liveConfigured) {
            System.out.println("OAuth callback must match: " + env.oauthRedirectUri());
        } else {
            System.out.println("Live Google stack is off — " + LIVE_STACK_CONFIG_HELP);
        }

        Thread.currentThread().join();
    }

    private static DataSource createDataSource(WebEnv env) {
        if (env.useDevH2()) {
            var ds = new JdbcDataSource();
            ds.setURL(env.jdbcUrl());
            ds.setUser(env.pgUser());
            ds.setPassword(env.pgPassword());
            return ds;
        }
        var ds = new PGSimpleDataSource();
        ds.setUrl(env.jdbcUrl());
        ds.setUser(env.pgUser());
        ds.setPassword(env.pgPassword());
        return ds;
    }

    private static void handleRoot(HttpExchange ex, WebEnv env, boolean liveConfigured) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            sendBytes(ex, 405, "text/plain", "Method not allowed");
            return;
        }
        String q = ex.getRequestURI().getQuery();
        boolean connected = q != null && q.contains("connected=1");
        String oauthLine =
                liveConfigured
                        ? "<p><a href=\"/oauth/google/start\">Connect Google Drive</a> (OAuth — browser opens Google)</p>\n"
                        : "<p><em>Live Google + OAuth are disabled until "
                                + "<code>CONNECTOR_TOKEN_KEY</code>, <code>GOOGLE_OAUTH_CLIENT_ID</code>, "
                                + "<code>GOOGLE_OAUTH_CLIENT_SECRET</code>, and a token database are configured; "
                                + "restart the server. Mock and Demo work without them.</em></p>\n";
        String connLine =
                !liveConfigured
                        ? ""
                        : (connected
                                ? "<p><strong>Google connected for user <code>"
                                        + esc(env.demoUserId())
                                        + "</code>.</strong></p>\n"
                                : "<p>Not connected — Drive queries will fail until you connect.</p>\n");
        String intro =
                oauthLine
                        + connLine
                        + "<p>Same dialect as CLI demos. Use <strong>Live</strong>, <strong>Mock</strong>, or "
                        + "<strong>Demo</strong> to switch stacks when Live is available; Live runs the real Google "
                        + "connector plus mock Notion (OAuth required). Demo uses compiled fixtures and a fixed "
                        + "search delay (not from .env).</p>\n";
        String defaultSql =
                """
                SELECT title, updatedAt
                FROM resources
                WHERE updatedAt > '2020-01-01'
                ORDER BY updatedAt DESC
                LIMIT 20
                """;
        int defaultUiPs = liveConfigured ? WebDefaults.UI_QUERY_PAGE_SIZE : WebDefaults.UI_QUERY_PAGE_SIZE_MOCK;
        String html =
                federationQueryPlaygroundPage(
                        "Federated query demo",
                        intro,
                        defaultUiPs,
                        defaultSql,
                        hybridConnectorModeRadios(liveConfigured) + mockUserSelectSection(),
                        PAYLOAD_HYBRID_MODE_JS + PAYLOAD_MOCK_USER_JS,
                        Integer.toString(WebDefaults.UI_QUERY_PAGE_SIZE_MOCK),
                        Integer.toString(WebDefaults.UI_QUERY_PAGE_SIZE_DEMO),
                        Integer.toString(WebDefaults.UI_QUERY_PAGE_SIZE),
                        hybridPageSizeWireJs(
                                WebDefaults.UI_QUERY_PAGE_SIZE_MOCK,
                                WebDefaults.UI_QUERY_PAGE_SIZE_DEMO,
                                WebDefaults.UI_QUERY_PAGE_SIZE));
        sendBytes(ex, 200, "text/html; charset=utf-8", html);
    }

    private static String hybridPageSizeWireJs(int mockUiPageSize, int demoUiPageSize, int liveUiPageSize) {
        return """
                  function applyModePageSizeDefault() {
                    var radios = document.querySelectorAll('input[name="connectorMode"]');
                    if (!radios.length) return;
                    var modeEl = document.querySelector('input[name="connectorMode"]:checked');
                    var v = modeEl ? modeEl.value : 'live';
                    var ps = document.getElementById('pageSize');
                    if (!ps) return;
                    if (v === 'mock') ps.value = __MOCK_PS__;
                    else if (v === 'demo') ps.value = __DEMO_PS__;
                    else ps.value = __LIVE_PS__;
                  }
                  document.querySelectorAll('input[name="connectorMode"]').forEach(function(r) {
                    r.addEventListener('change', applyModePageSizeDefault);
                  });
                  applyModePageSizeDefault();
                """
                .replace("__MOCK_PS__", Integer.toString(mockUiPageSize))
                .replace("__DEMO_PS__", Integer.toString(demoUiPageSize))
                .replace("__LIVE_PS__", Integer.toString(liveUiPageSize))
                .strip();
    }

    private static String federationQueryPlaygroundPage(
            String h1InnerHtml,
            String introHtml,
            int defaultUiPageSize,
            String defaultSql,
            String connectorControlsHtml,
            String connectorPayloadJs,
            String mockUiPsHint,
            String demoUiPsHint,
            String liveUiPsHint,
            String hybridWireScript) {
        return FEDERATION_QUERY_PLAYGROUND_TEMPLATE.replace("${H1}", h1InnerHtml)
                .replace("${INTRO}", introHtml)
                .replace("${DEFAULT_SQL}", esc(defaultSql.strip()))
                .replace("${DEFAULT_UI_PS}", Integer.toString(defaultUiPageSize))
                .replace("${MAX_UI_PS}", Integer.toString(WebDefaults.UI_QUERY_PAGE_SIZE_CLIENT_MAX))
                .replace("${CONNECTOR_CONTROLS}", connectorControlsHtml)
                .replace("${CONNECTOR_PAYLOAD_LINES}", connectorPayloadJs.strip())
                .replace("${MOCK_UI_PS}", mockUiPsHint)
                .replace("${DEMO_UI_PS}", demoUiPsHint)
                .replace("${LIVE_UI_PS}", liveUiPsHint)
                .replace("${HYBRID_PAGE_SIZE_WIRE}", hybridWireScript)
                .replace("${DEMO_QUERY_PRESETS}", demoQueryPresetsSection())
                .replace("${DEMO_PRESET_BOOTSTRAP}", DEMO_PRESET_BOOTSTRAP_JS.strip());
    }

    private static void handleOAuthStart(HttpExchange ex, GoogleOAuthService oauth) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            sendBytes(ex, 405, "text/plain", "Method not allowed");
            return;
        }
        purgeStaleOAuthStates();
        String state = UUID.randomUUID().toString();
        String verifier = Pkce.newVerifier();
        String challenge = Pkce.challengeS256(verifier);
        OAUTH_STATE.put(state, new OAuthPending(verifier, Instant.now()));
        URI redirect = oauth.buildAuthorizationUrl(state, challenge);
        ex.getResponseHeaders().add("Location", redirect.toString());
        ex.sendResponseHeaders(302, -1);
        ex.close();
    }

    private static void handleOAuthCallback(
            HttpExchange ex,
            GoogleOAuthService oauth,
            GoogleTokenStore store,
            TokenEncryptor encryptor,
            WebEnv env)
            throws IOException {

        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            sendBytes(ex, 405, "text/plain", "Method not allowed");
            return;
        }
        URI uri = ex.getRequestURI();
        String raw = uri.getQuery() == null ? "" : uri.getQuery();
        Map<String, String> params = parseQuery(raw);
        if (params.containsKey("error")) {
            sendBytes(
                    ex,
                    400,
                    "text/html; charset=utf-8",
                    "<p>OAuth error: " + esc(params.get("error")) + "</p><p><a href=\"/\">Home</a></p>");
            return;
        }
        String code = params.get("code");
        String state = params.get("state");
        if (code == null || state == null) {
            sendBytes(ex, 400, "text/plain", "missing code or state");
            return;
        }
        OAuthPending pending = OAUTH_STATE.remove(state);
        if (pending == null) {
            sendBytes(ex, 400, "text/plain", "invalid or expired OAuth state");
            return;
        }
        try {
            GoogleOAuthService.TokenBundle bundle = oauth.exchangeAuthorizationCode(code, pending.codeVerifier());
            if (bundle.refreshToken() == null) {
                sendBytes(
                        ex,
                        400,
                        "text/html; charset=utf-8",
                        "<p>Google did not return a refresh token. Try revoking app access in Google"
                                + " account settings and connect again with prompt=consent.</p>"
                                + "<p><a href=\"/\">Home</a></p>");
                return;
            }
            Instant now = Instant.now();
            ConnectorAccount acc =
                    new ConnectorAccount(
                            null,
                            env.demoUserId(),
                            RealGoogleDriveConnector.ACCOUNT_TYPE,
                            encryptor.encrypt(bundle.accessToken()),
                            encryptor.encrypt(bundle.refreshToken()),
                            bundle.expiresAt(),
                            OAuthDefaults.GOOGLE_DRIVE_READONLY_SCOPE,
                            now,
                            now);
            store.upsert(acc);
        } catch (Exception e) {
            sendBytes(
                    ex,
                    500,
                    "text/html; charset=utf-8",
                    "<p>Token exchange failed.</p><pre>" + esc(e.getMessage()) + "</pre>");
            return;
        }
        ex.getResponseHeaders().add("Location", "/?connected=1");
        ex.sendResponseHeaders(302, -1);
        ex.close();
    }

    /**
     * @param liveRunner OAuth-backed stack, or {@code null} when live env / DB init did not succeed
     * @param mockRunner in-memory mock Google + mock Notion stack
     * @param demoRunner demo Google + demo Notion stack
     */
    private static void handleQuery(
            HttpExchange ex,
            WebQueryRunner liveRunner,
            WebQueryRunner mockRunner,
            WebQueryRunner demoRunner,
            String demoUserId,
            GoogleTokenStore store)
            throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            sendBytes(ex, 405, "text/plain", "Use POST");
            return;
        }
        String ct = ex.getRequestHeaders().getFirst("Content-Type");
        boolean json = ct != null && ct.toLowerCase().startsWith("application/json");
        byte[] body = readBodyLimited(ex);

        String sql;
        Integer pageNumber = null;
        String cursor = null;
        Integer pageSizeReq = null;
        Duration maxStaleness = null;
        String connectorMode = "live";
        String mockUserId = "";
        if (json) {
            try {
                JsonObject o = JsonParser.parseString(new String(body, StandardCharsets.UTF_8)).getAsJsonObject();
                sql = o.get("sql").getAsString();
                if (o.has("pageNumber") && !o.get("pageNumber").isJsonNull()) {
                    pageNumber = o.get("pageNumber").getAsInt();
                }
                if (o.has("cursor") && !o.get("cursor").isJsonNull()) {
                    cursor = o.get("cursor").getAsString();
                }
                if (o.has("pageSize") && !o.get("pageSize").isJsonNull()) {
                    pageSizeReq = o.get("pageSize").getAsInt();
                }
                if (o.has("maxStaleness") && !o.get("maxStaleness").isJsonNull()) {
                    maxStaleness = Duration.parse(o.get("maxStaleness").getAsString().trim());
                }
                if (o.has("connectorMode") && !o.get("connectorMode").isJsonNull()) {
                    connectorMode = o.get("connectorMode").getAsString().trim();
                }
                if (o.has("mockUserId") && !o.get("mockUserId").isJsonNull()) {
                    mockUserId = o.get("mockUserId").getAsString().trim();
                }
            } catch (Exception e) {
                errorJson(ex, "invalid JSON: " + e.getMessage());
                return;
            }
        } else {
            String raw = new String(body, StandardCharsets.UTF_8);
            Map<String, String> form = parseForm(raw);
            sql = form.get("sql");
            cursor = form.get("cursor");
            String pn = form.get("pageNumber");
            if (pn != null && !pn.isBlank()) {
                try {
                    pageNumber = Integer.parseInt(pn.trim());
                } catch (NumberFormatException e) {
                    errorJson(ex, "invalid pageNumber");
                    return;
                }
            }
            String psForm = form.get("pageSize");
            if (psForm != null && !psForm.isBlank()) {
                try {
                    pageSizeReq = Integer.parseInt(psForm.trim());
                } catch (NumberFormatException e) {
                    errorJson(ex, "invalid pageSize");
                    return;
                }
            }
            String ms = form.get("maxStaleness");
            if (ms != null && !ms.isBlank()) {
                maxStaleness = Duration.parse(ms.trim());
            }
            String cm = form.get("connectorMode");
            if (cm != null && !cm.isBlank()) {
                connectorMode = cm.trim();
            }
            String mu = form.get("mockUserId");
            if (mu != null) {
                mockUserId = mu.trim();
            }
        }
        if (sql == null || sql.isBlank()) {
            errorJson(ex, "missing sql");
            return;
        }

        try {
            Objects.requireNonNull(mockRunner, "mockRunner");
            Objects.requireNonNull(demoRunner, "demoRunner");

            WebQueryRunner runner;
            UserContext requestUser;
            boolean needGoogle = false;
            if ("mock".equalsIgnoreCase(connectorMode)) {
                runner = mockRunner;
                requestUser = mockUserId.isBlank() ? UserContext.anonymous() : new UserContext(mockUserId);
            } else if ("demo".equalsIgnoreCase(connectorMode)) {
                runner = demoRunner;
                requestUser = mockUserId.isBlank() ? UserContext.anonymous() : new UserContext(mockUserId);
            } else {
                if (liveRunner == null) {
                    sendLiveStackUnavailable(ex, json);
                    return;
                }
                runner = liveRunner;
                requestUser = new UserContext(demoUserId);
                needGoogle = true;
            }

            if (needGoogle && store != null && missingGoogleAccount(store, demoUserId)) {
                JsonObject hint = new JsonObject();
                hint.addProperty("ok", false);
                hint.addProperty(
                        "error",
                        "Connect Google first (/oauth/google/start). Live mode runs the real Google "
                                + "connector alongside mock Notion; choose Mock mode to try without OAuth.");
                if (json) {
                    sendJson(ex, 401, hint);
                } else {
                    sendBytes(
                            ex,
                            401,
                            "text/html; charset=utf-8",
                            "<p>Connect Google first: <a href=\"/oauth/google/start\">OAuth</a></p>");
                }
                return;
            }
            JsonObject result =
                    runner.run(sql, pageNumber, cursor, pageSizeReq, maxStaleness, QueryCacheScope.from(requestUser));
            WebQueryTraceLogger.appendTrace(connectorMode, sql, result);
            if (json) {
                sendJson(ex, 200, result);
            } else {
                sendBytes(
                        ex,
                        200,
                        "text/html; charset=utf-8",
                        "<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>Result</title></head><body>"
                                + "<h2>Result</h2><pre>"
                                + esc(GSON.toJson(result))
                                + "</pre><p><a href=\"/\">Back</a></p></body></html>");
            }
        } catch (IllegalArgumentException e) {
            if (json) {
                errorJson(ex, e.getMessage());
            } else {
                sendBytes(ex, 400, "text/html; charset=utf-8", "<pre>" + esc(e.getMessage()) + "</pre>");
            }
        } catch (Exception e) {
            String msg = esc(Objects.toString(e.getMessage(), e.getClass().getSimpleName()));
            if (json) {
                errorJson(ex, msg);
            } else {
                sendBytes(ex, 500, "text/html; charset=utf-8", "<pre>" + msg + "</pre>");
            }
        }
    }

    private static boolean missingGoogleAccount(GoogleTokenStore store, String userId) {
        try {
            return store.find(userId, RealGoogleDriveConnector.ACCOUNT_TYPE).isEmpty();
        } catch (SQLException e) {
            return true;
        }
    }

    private static void purgeStaleOAuthStates() {
        Instant cutoff = Instant.now().minus(15, ChronoUnit.MINUTES);
        List<String> rm = new ArrayList<>();
        OAUTH_STATE.forEach(
                (k, v) -> {
                    if (v.created().isBefore(cutoff)) {
                        rm.add(k);
                    }
                });
        rm.forEach(OAUTH_STATE::remove);
    }

    private record OAuthPending(String codeVerifier, Instant created) {}

    private static Map<String, String> parseQuery(String raw) {
        Map<String, String> m = new LinkedHashMap<>();
        if (raw == null || raw.isEmpty()) {
            return m;
        }
        for (String part : raw.split("&")) {
            int i = part.indexOf('=');
            if (i > 0) {
                m.put(urlDecode(part.substring(0, i)), urlDecode(part.substring(i + 1)));
            } else {
                m.put(urlDecode(part), "");
            }
        }
        return m;
    }

    private static Map<String, String> parseForm(String raw) {
        return parseQuery(raw);
    }

    private static String urlDecode(String s) {
        try {
            return java.net.URLDecoder.decode(s, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return s;
        }
    }

    private static byte[] readBodyLimited(HttpExchange ex) throws IOException {
        String len = ex.getRequestHeaders().getFirst("Content-Length");
        if (len != null && Long.parseLong(len) > WebDefaults.MAX_HTTP_REQUEST_BODY_BYTES) {
            throw new IllegalArgumentException("request body too large");
        }
        try (InputStream in = ex.getRequestBody()) {
            byte[] raw = in.readNBytes(WebDefaults.MAX_HTTP_REQUEST_BODY_BYTES + 1);
            if (raw.length > WebDefaults.MAX_HTTP_REQUEST_BODY_BYTES) {
                throw new IllegalArgumentException("request body too large");
            }
            return raw;
        }
    }

    private static void sendBytes(HttpExchange ex, int code, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", contentType);
        ex.sendResponseHeaders(code, bytes.length);
        ex.getResponseBody().write(bytes);
        ex.close();
    }

    private static void sendJson(HttpExchange ex, int code, JsonObject body) throws IOException {
        sendBytes(ex, code, "application/json; charset=utf-8", GSON.toJson(body));
    }

    private static void errorJson(HttpExchange ex, String message) throws IOException {
        JsonObject o = new JsonObject();
        o.addProperty("ok", false);
        o.addProperty("error", message);
        sendJson(ex, 400, o);
    }

    private static String esc(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}
