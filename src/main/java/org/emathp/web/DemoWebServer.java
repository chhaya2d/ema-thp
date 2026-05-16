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
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.emathp.auth.ConnectorAccount;
import org.emathp.authz.PrincipalRegistry;
import org.emathp.authz.demo.DemoPrincipalRegistry;
import org.emathp.cache.QueryCacheScope;
import java.nio.file.Path;
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
import org.emathp.connector.mock.MockConnectorDevSettings;
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
import org.emathp.authz.Principal;
import org.emathp.parser.SQLParserService;
import org.emathp.planner.Planner;
import org.emathp.query.ErrorCode;
import org.emathp.query.FederatedQueryService;
import org.emathp.query.RequestContext;
import org.emathp.query.ResponseContext;
import org.emathp.ratelimit.HierarchicalRateLimiter;
import org.emathp.ratelimit.HierarchicalRateLimiterConfig;
import org.emathp.ratelimit.RateLimitPolicy;
import org.emathp.ratelimit.TokenBucketConfig;
import org.h2.jdbcx.JdbcDataSource;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * Embedded HTTP browser demo: **Demo** (fixture Google + Notion) or **Live** (real Google + mock
 * Notion when OAuth env is configured). Mock connectors are not exposed in the UI. Binds to
 * loopback only.
 */
public final class DemoWebServer {

    private static final Gson GSON = new Gson();

    private static final String LIVE_STACK_CONFIG_HELP =
            "Live mode requires CONNECTOR_TOKEN_KEY, GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, "
                    + "and a working token database (Postgres or EMA_DEV_H2=true). Set them in OS env or .env and "
                    + "restart the server.";

    private record LiveWebStack(
            FederatedQueryService service,
            GoogleTokenStore store,
            GoogleOAuthService oauth,
            TokenEncryptor encryptor) {}

    /**
     * Service-layer rate limit (user + tenant) — debits every request including cache hits.
     * Honors Ema's per-user fairness and per-tenant SLO. Tenant 30 rps (burst 60); user 5 rps
     * (burst 10) is the tightest, so per-user fairness trips first in burst tests. Anon callers
     * bypass entirely.
     */
    private static HierarchicalRateLimiterConfig demoServiceRateLimitConfig() {
        return HierarchicalRateLimiterConfig.forService(
                new TokenBucketConfig(30.0, 60.0),  // tenant
                new TokenBucketConfig(5.0, 10.0));  // user
    }

    /**
     * Connector-layer rate limit — debits only when a provider call is actually made (cache
     * miss). Protects upstream APIs from being hammered. Generous default (30 rps / burst 60);
     * production would set per-connector based on each upstream's published rate (e.g. Notion 3
     * rps, Google Drive ~100 rps).
     */
    private static HierarchicalRateLimiterConfig demoConnectorRateLimitConfig() {
        return HierarchicalRateLimiterConfig.forConnector(new TokenBucketConfig(30.0, 60.0));
    }

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
     * Builds the live Google {@link FederatedQueryService} when env and DB init succeed; otherwise
     * {@code null} (demo fixtures remain available).
     */
    private static LiveWebStack tryCreateLiveWebStack(
            WebEnv env,
            SQLParserService parser,
            PrincipalRegistry principals,
            RateLimitPolicy connectorLimiter,
            RateLimitPolicy serviceLimiter) {
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
            QueryExecutor executor = new QueryExecutor(connectorLimiter);
            JoinExecutor joinExecutor = new JoinExecutor(planner, executor);
            UserContext user = new UserContext(env.demoUserId());
            SnapshotQueryService snapshots =
                    new SnapshotQueryService(
                            planner,
                            executor,
                            new FsSnapshotStore(Path.of("data")),
                            new SystemClock(),
                            WebDefaults.snapshotChunkFreshness());
            FederatedQueryService liveService =
                    new DefaultFederatedQueryService(
                            parser,
                            planner,
                            executor,
                            joinExecutor,
                            List.copyOf(connectorsByName.values()),
                            connectorsByName,
                            user,
                            snapshots,
                            SnapshotEnvironment.PROD,
                            WebDefaults.UI_QUERY_PAGE_SIZE,
                            principals,
                            serviceLimiter);
            return new LiveWebStack(liveService, store, oauth, encryptor);
        } catch (Exception e) {
            System.err.println("Live Google stack init failed (demo fixtures still work): " + e.getMessage());
            e.printStackTrace(System.err);
            return null;
        }
    }

    private static void liveOAuthDisabled(HttpExchange exchange) throws IOException {
        sendBytes(
                exchange,
                503,
                "text/html; charset=utf-8",
                "<p>" + esc(LIVE_STACK_CONFIG_HELP) + "</p><p><a href=\"/\">Home</a></p>");
    }

    /** Demo principals only (fixture corpus). */
    private static String demoUserSelectSection() {
        return "<p><label>User <select id=\"mockUserSelect\">"
                + "<option value=\"alice\" selected>alice (tenant-1, hr)</option>"
                + "<option value=\"bob\">bob (tenant-1, engineering)</option>"
                + "<option value=\"carol\">carol (tenant-2, hr)</option>"
                + "<option value=\"dan\">dan (tenant-1, hr) — shares cache with alice</option>"
                + "</select></label></p>\n";
    }

    /** Demo (fixture connectors) vs Live (OAuth Google). Two radios only. */
    private static String connectorModeTwoOptions(boolean liveConfigured) {
        if (liveConfigured) {
            return "<p>Connector "
                    + "<label><input type=\"radio\" name=\"connectorMode\" value=\"demo\" checked> Demo</label> "
                    + "<label><input type=\"radio\" name=\"connectorMode\" value=\"live\"> Live</label></p>\n";
        }
        return "<p>Connector "
                + "<label><input type=\"radio\" name=\"connectorMode\" value=\"demo\" checked> Demo</label> "
                + "<label><input type=\"radio\" name=\"connectorMode\" value=\"live\" disabled "
                + "title=\"Live requires CONNECTOR_TOKEN_KEY, OAuth client env, and DB in .env\"> Live</label></p>\n";
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
                              if (k.indexOf('live_') === 0) {
                                var lr = document.querySelector('input[name="connectorMode"][value="live"]');
                                if (lr && !lr.disabled) lr.checked = true;
                              } else {
                                var dr = document.querySelector('input[name="connectorMode"][value="demo"]');
                                if (dr) dr.checked = true;
                              }
                              if (typeof applyModePageSizeDefault === 'function') applyModePageSizeDefault();
                            });
                          })();
                    """;

    private static String demoQueryPresetsSection() {
        String json = GSON.toJson(DemoQueryPresets.presetMap());
        return "<p><label>Query preset <select id=\"demoQueryPreset\"><option value=\"\">Custom SQL</option>"
                + "<option value=\"d_g_1\">Demo Drive · date + ORDER BY desc</option>"
                + "<option value=\"d_g_2\">Demo Drive · title LIKE JoinKey</option>"
                + "<option value=\"d_g_3\">Demo Drive · join on title</option>"
                + "<option value=\"d_n_1\">Demo Notion · date + ORDER BY desc</option>"
                + "<option value=\"d_n_2\">Demo Notion · title LIKE Extra</option>"
                + "<option value=\"d_n_3\">Demo Notion · ORDER BY title asc</option>"
                + "<option value=\"live_1\">Live · date filter (OAuth)</option>"
                + "</select></label></p>\n"
                + "<script type=\"application/json\" id=\"ema-demo-presets\">"
                + json
                + "</script>\n";
    }

    /**
     * Home page: SQL playground with prev/next UI paging (POST JSON to /api/query). Placeholders:
     * ${H1}, ${INTRO}, ${DEFAULT_SQL}, ${DEFAULT_UI_PS}, ${MAX_UI_PS}, ${CONNECTOR_CONTROLS},
     * ${CONNECTOR_PAYLOAD_LINES}, ${DEMO_QUERY_PRESETS}, ${DEMO_UI_PS}, ${LIVE_UI_PS},
     * ${HYBRID_PAGE_SIZE_WIRE}, ${DEMO_PRESET_BOOTSTRAP}.
     */
    private static final String FEDERATION_QUERY_PLAYGROUND_TEMPLATE =
            """
            <!DOCTYPE html>
            <html><head><meta charset="utf-8"><title>EmaTHP demo</title></head><body>
            <h1>${H1}</h1>
            ${INTRO}
            <h2>Run SQL</h2>
            <p style="color:#555">POST to <code>/api/query</code>. Only <strong>User</strong> is sent from the browser; each JSON response includes resolved <code>tenantId</code> and <code>roleSlug</code> (demo registry). Snapshots are keyed per tenant+role+user after role tag filtering. Use <code>FROM resources</code> for every connector, or <code>FROM notion</code> / <code>FROM google</code> for one side. Optional <code>maxStaleness</code> (ISO duration) bounds snapshot reuse.</p>
            <label>SQL<br><textarea id="sql" rows="10" cols="96" wrap="off">${DEFAULT_SQL}</textarea></label>
            ${CONNECTOR_CONTROLS}
            ${DEMO_QUERY_PRESETS}
            <p><label>UI page size <input type="number" id="pageSize" min="1" max="${MAX_UI_PS}" value="${DEFAULT_UI_PS}"></label>
            <span style="color:#666">(defaults: demo <code>${DEMO_UI_PS}</code>, live <code>${LIVE_UI_PS}</code>)</span></p>
            <p><label>Max staleness <input type="text" id="maxStalenessInp" placeholder="e.g. 5m or 30s" size="16"></label>
            <span style="color:#666">Accepts <code>5m</code>, <code>30s</code>, <code>1h</code>, <code>500ms</code>, or ISO <code>PT5M</code>. Empty = connector default TTL.</span></p>
            <p><button type="button" id="runBtn">Run</button></p>
            <p><span id="pageInfo"></span>
              <button type="button" id="prevBtn" disabled>Prev</button>
              <button type="button" id="nextBtn" disabled>Next</button>
              <span id="status"></span></p>
            <h3>This run</h3>
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
              function formatMs(ms) {
                if (ms == null) return '—';
                var n = Number(ms);
                if (n < 1000) return n + ' ms';
                if (n < 60000) return (n / 1000).toFixed(n < 10000 ? 1 : 0) + ' s';
                if (n < 3600000) return Math.round(n / 60000) + ' min ' + Math.round((n % 60000) / 1000) + ' s';
                return Math.round(n / 3600000) + ' h ' + Math.round((n % 3600000) / 60000) + ' min';
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
                var mode = side.serveMode || (reuse ? 'cached' : 'live');
                var html = '<h4>' + escHtml(conn) + ' — <strong>' + escHtml(mode) + '</strong></h4>';
                if (reuse || mode === 'cached') {
                  html += '<p><strong>Cached snapshot</strong> — no provider HTTP call for this connector side.</p>';
                  html += '<p>Cache directory: <code>' + escHtml(dir) + '</code></p>';
                  var meta = side.authoritativeChunkMeta;
                  if (meta && meta.freshnessUntil) {
                    html += '<p>Chunk validity until: <code>' + escHtml(String(meta.freshnessUntil)) + '</code></p>';
                  }
                } else {
                  html += '<p><strong>Live fetch</strong> — connector pipeline ran this request.</p>';
                  html += '<p>Provider fetches: ' + escHtml(String(side.providerFetchesThisRequest)) +
                    ', continuations: ' + escHtml(String(side.continuationFetchesThisRequest)) + '</p>';
                  html += '<p><strong>Pushed to connector:</strong> <code>' + escHtml(String(side.pushedSummary || '')) + '</code></p>';
                  html += '<p><strong>Still pending in engine:</strong> <code>' + escHtml(String(side.pending || '')) + '</code></p>';
                  html += '<p><strong>Residual applied:</strong> <code>' + escHtml(String(side.residual || '')) + '</code></p>';
                  html += '<p>Snapshot staging dir: <code>' + escHtml(dir) + '</code></p>';
                  var calls = (side.execution && side.execution.calls) ? side.execution.calls : [];
                  if (calls.length) {
                    html += '<p><em>Engine → connector page loop</em></p><ol>';
                    for (var i = 0; i < calls.length; i++) {
                      var c = calls[i];
                      html += '<li>Fetch #' + (i + 1) + ': cursor=<code>' + escHtml(c.cursor) + '</code>, rows=' +
                        escHtml(String(c.rowsReturned)) + ', next=<code>' + escHtml(c.nextCursor) + '</code></li>';
                    }
                    html += '</ol>';
                  } else {
                    html += '<p><em>No per-fetch call list.</em></p>';
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
                if (j.serverElapsedMs != null) {
                  h += '<p><strong>Server time:</strong> ' + escHtml(String(j.serverElapsedMs)) + ' ms';
                  if (j.traceId) {
                    h += ' · <strong>traceId:</strong> <code>' + escHtml(j.traceId) + '</code>';
                  }
                  h += '</p>';
                }
                if (j.freshness_ms != null || j.rate_limit_status) {
                  h += '<p>';
                  if (j.freshness_ms != null) {
                    h += '<strong>Data age:</strong> ' + formatMs(j.freshness_ms);
                  }
                  if (j.rate_limit_status) {
                    if (j.freshness_ms != null) h += ' · ';
                    var rls = j.rate_limit_status;
                    var color = rls === 'EXHAUSTED' ? '#b00' : '#0a0';
                    h += '<strong>Rate limit:</strong> <span style="color:' + color + '">' + escHtml(rls) + '</span>';
                  }
                  h += '</p>';
                }
                if (j.tenantId || j.roleSlug) {
                  h += '<p><strong>Tenant / role (resolved):</strong> <code>' + escHtml(String(j.tenantId||'')) + '</code> · <code>' + escHtml(String(j.roleSlug||'')) + '</code></p>';
                }
                if (j.providerFetchSummary) {
                  h += '<p><strong>Fetch summary:</strong> <code>' + escHtml(j.providerFetchSummary) + '</code>';
                  if (j.targetConnectors && j.targetConnectors.length) {
                    h += ' — targets: <code>' + escHtml(j.targetConnectors.join(', ')) + '</code>';
                  }
                  h += '</p>';
                }
                h += '<p>Query snapshot root: <code>' + escHtml(j.snapshotPath || '') + '</code>';
                if (j.freshnessDecision) {
                  h += ' — tree decision: <strong>' + escHtml(j.freshnessDecision) + '</strong>';
                }
                h += '</p>';
                if (j.snapshotTreeNote) {
                  h += '<p style="color:#666;font-size:90%">' + escHtml(j.snapshotTreeNote) + '</p>';
                }
                if (j.kind === 'single' && j.sides) {
                  var oneSide = j.sides.length === 1;
                  h += '<p>UI page <strong>' + escHtml(String(j.uiPageIndex)) + '</strong> of ~' + escHtml(String(j.uiApproxPageCount)) +
                    ', window <strong>' + escHtml(String(j.uiPageSize)) + '</strong>' +
                    (oneSide ? ' (single connector).' : ' (one window per connector side).') + '</p>';
                  for (var s = 0; s < j.sides.length; s++) {
                    var side = j.sides[s];
                    h += renderSideFetchSection(side);
                    var rows = side.execution && side.execution.rows ? side.execution.rows : [];
                    var total = side.execution && side.execution.uiRowTotal != null ? side.execution.uiRowTotal : rows.length;
                    h += '<p>Rows in this UI window: ' + rows.length +
                      (total != null ? ' (materialized length before UI slice: ' + total + ')' : '') + '</p>';
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
                out.textContent = '';
                humanOut.innerHTML = '';
                status.textContent = '';
                const t0 = performance.now();
                const sql = document.getElementById('sql').value;
                const pageSize = parseInt(document.getElementById('pageSize').value, 10);
                const payload = { sql: sql, pageSize: pageSize };
                ${CONNECTOR_PAYLOAD_LINES}
                var msInp = document.getElementById('maxStalenessInp');
                if (msInp && msInp.value.trim()) payload.maxStaleness = msInp.value.trim();
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
                  var summ = j.providerFetchSummary ? '[' + j.providerFetchSummary + '] ' : '';
                  pageInfo.textContent =
                    summ + 'Page ' + j.uiPageIndex + ' of ~' + j.uiApproxPageCount
                    + ' (size ' + j.uiPageSize + ', tree=' + j.freshnessDecision + ')';
                  lastNext = j.uiNextCursor;
                  lastPrev = j.uiPrevCursor;
                  nextBtn.disabled = lastNext == null;
                  prevBtn.disabled = lastPrev == null;
                } else {
                  pageInfo.textContent = 'UI paging applies to single-source queries only.';
                  nextBtn.disabled = true;
                  prevBtn.disabled = true;
                }
                status.textContent = 'Round-trip ' + ms + ' ms' +
                  (j.serverElapsedMs != null ? ' · server ' + j.serverElapsedMs + ' ms' : '');
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
        PrincipalRegistry principals = new DemoPrincipalRegistry();
        // Two-layer rate limit: service (user+tenant) fires at every request including cache
        // hits; connector fires only on outbound provider calls. Each layer has its own bucket
        // state so they can't double-debit a single request.
        RateLimitPolicy serviceLimiter = new HierarchicalRateLimiter(demoServiceRateLimitConfig());
        RateLimitPolicy connectorLimiter =
                new HierarchicalRateLimiter(demoConnectorRateLimitConfig());
        LiveWebStack live =
                tryCreateLiveWebStack(env, parser, principals, connectorLimiter, serviceLimiter);
        FederatedQueryService liveService = live != null ? live.service() : null;
        GoogleTokenStore store = live != null ? live.store() : null;

        Planner demoPlanner = new Planner(true);
        QueryExecutor demoExecutor = new QueryExecutor(connectorLimiter);
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
        FederatedQueryService demoService =
                new DefaultFederatedQueryService(
                        parser,
                        demoPlanner,
                        demoExecutor,
                        demoJoinExecutor,
                        List.copyOf(demoConnectorsByName.values()),
                        demoConnectorsByName,
                        UserContext.anonymous(),
                        demoSnapshots,
                        SnapshotEnvironment.TEST,
                        WebDefaults.UI_QUERY_PAGE_SIZE_DEMO,
                        principals,
                        serviceLimiter);

        WebQueryService webQueryService = new WebQueryService(demoService, liveService);
        boolean liveConfigured = live != null;
        // Backlog 200 — generous accept-queue so short bursts above accept-rate don't
        // socket-reject. Default (0) maps to system default (~50 on Windows) which we observed
        // hitting under sustained 100 RPS k6 load.
        HttpServer http =
                HttpServer.create(
                        new InetSocketAddress(
                                java.net.InetAddress.getByName(WebDefaults.HTTP_BIND_ADDRESS), env.webPort()),
                        200);
        http.createContext("/", exchange -> handleRoot(exchange, env, liveConfigured));
        http.createContext("/health", exchange -> sendBytes(exchange, 200, "text/plain", "ok"));
        http.createContext("/metrics", DemoWebServer::handleMetrics);
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
                                exchange,
                                webQueryService,
                                env.demoUserId(),
                                store,
                                principals));

        // Fixed thread pool — default (null) is synchronous (single thread), which serializes
        // every request behind the JDK's accept loop. 16-way concurrency processes cache hits in
        // parallel without saturating; per-request work is mostly I/O (snapshot read) + cheap
        // CPU (planner / tag filter).
        http.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(16));
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
                                + "restart the server. Demo fixtures work without them.</em></p>\n";
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
                        + "<p>Same dialect as CLI demos. Choose <strong>Demo</strong> (fixture Google + Notion) or "
                        + "<strong>Live</strong> when configured — real Google Drive plus Notion mock connector "
                        + "(OAuth required). User selects fixture corpus for Demo only.</p>\n";
        String defaultSql =
                """
                SELECT title, updatedAt
                FROM resources
                WHERE updatedAt > '2020-01-01'
                ORDER BY updatedAt DESC
                LIMIT 20
                """;
        int defaultUiPs = WebDefaults.UI_QUERY_PAGE_SIZE_DEMO;
        String html =
                federationQueryPlaygroundPage(
                        "Federated query demo",
                        intro,
                        defaultUiPs,
                        defaultSql,
                        connectorModeTwoOptions(liveConfigured) + demoUserSelectSection(),
                        PAYLOAD_HYBRID_MODE_JS + PAYLOAD_MOCK_USER_JS,
                        Integer.toString(WebDefaults.UI_QUERY_PAGE_SIZE_DEMO),
                        Integer.toString(WebDefaults.UI_QUERY_PAGE_SIZE),
                        hybridPageSizeWireJs(
                                WebDefaults.UI_QUERY_PAGE_SIZE_DEMO,
                                WebDefaults.UI_QUERY_PAGE_SIZE));
        sendBytes(ex, 200, "text/html; charset=utf-8", html);
    }

    private static String hybridPageSizeWireJs(int demoUiPageSize, int liveUiPageSize) {
        return """
                  function applyModePageSizeDefault() {
                    var modeEl = document.querySelector('input[name="connectorMode"]:checked');
                    var v = modeEl ? modeEl.value : 'demo';
                    var ps = document.getElementById('pageSize');
                    if (!ps) return;
                    ps.value = v === 'live' ? __LIVE_PS__ : __DEMO_PS__;
                  }
                  document.querySelectorAll('input[name="connectorMode"]').forEach(function(r) {
                    r.addEventListener('change', applyModePageSizeDefault);
                  });
                  applyModePageSizeDefault();
                """
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
     * @param webQueryService routes demo vs live {@link FederatedQueryService}
     */
    private static void handleQuery(
            HttpExchange ex,
            WebQueryService webQueryService,
            String demoUserId,
            GoogleTokenStore store,
            PrincipalRegistry principals)
            throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            sendBytes(ex, 405, "text/plain", "Use POST");
            return;
        }

        // All HTTP header parsing lives in HttpEnvelope. Body parsing stays inline
        // because it depends on demo-specific fields (connectorMode, mockUserId).
        HttpEnvelope.RequestHeaders reqHeaders = HttpEnvelope.parse(ex);
        byte[] body = readBodyLimited(ex);

        HttpQueryPayload payload;
        String connectorMode = "demo";
        String mockUserId = "";
        try {
            if (reqHeaders.wantsJson()) {
                JsonObject o = JsonParser.parseString(new String(body, StandardCharsets.UTF_8)).getAsJsonObject();
                payload = HttpQueryPayload.parseJson(o);
                if (o.has("connectorMode") && !o.get("connectorMode").isJsonNull()) {
                    connectorMode = o.get("connectorMode").getAsString().trim();
                }
                if (o.has("mockUserId") && !o.get("mockUserId").isJsonNull()) {
                    mockUserId = o.get("mockUserId").getAsString().trim();
                }
            } else {
                String raw = new String(body, StandardCharsets.UTF_8);
                Map<String, String> form = parseForm(raw);
                payload = HttpQueryPayload.parseForm(form);
                String cm = form.get("connectorMode");
                if (cm != null && !cm.isBlank()) {
                    connectorMode = cm.trim();
                }
                String mu = form.get("mockUserId");
                if (mu != null) {
                    mockUserId = mu.trim();
                }
            }
        } catch (RuntimeException e) {
            HttpEnvelope.writeFailure(
                    ex,
                    reqHeaders,
                    null,   // no RequestContext yet — parse failed before identity resolution
                    new ResponseContext.Outcome.Failure(
                            ErrorCode.BAD_QUERY, "invalid payload: " + e.getMessage(), null, null));
            return;
        }

        if (payload.sql() == null || payload.sql().isBlank()) {
            HttpEnvelope.writeFailure(
                    ex,
                    reqHeaders,
                    null,
                    new ResponseContext.Outcome.Failure(ErrorCode.BAD_QUERY, "missing sql", null, null));
            return;
        }
        if ("mock".equalsIgnoreCase(connectorMode)) {
            HttpEnvelope.writeFailure(
                    ex,
                    reqHeaders,
                    null,
                    new ResponseContext.Outcome.Failure(
                            ErrorCode.BAD_QUERY,
                            "connectorMode \"mock\" is not available in the browser demo (use demo or live)",
                            null,
                            null));
            return;
        }

        // Identity resolution: header wins. X-User-Id header takes precedence over body
        // mockUserId / demoUserId so callers can drive identity from headers consistently.
        UserContext requestUser;
        boolean needGoogle = "live".equalsIgnoreCase(connectorMode);
        String effectiveUserIdForScope = mockUserId;
        if (reqHeaders.userIdHeader() != null) {
            requestUser = new UserContext(reqHeaders.userIdHeader());
            effectiveUserIdForScope = reqHeaders.userIdHeader();
        } else if ("demo".equalsIgnoreCase(connectorMode)) {
            requestUser = mockUserId.isBlank() ? UserContext.anonymous() : new UserContext(mockUserId);
        } else if ("live".equalsIgnoreCase(connectorMode)) {
            requestUser = new UserContext(demoUserId);
        } else {
            requestUser = UserContext.anonymous();
            // Unknown modes fall through to WebQueryService.execute → BAD_QUERY.
        }

        if (needGoogle && store != null && missingGoogleAccount(store, demoUserId)) {
            HttpEnvelope.writeFailure(
                    ex,
                    reqHeaders,
                    null,
                    new ResponseContext.Outcome.Failure(
                            ErrorCode.ENTITLEMENT_DENIED,
                            "Connect Google first (/oauth/google/start). Live mode needs OAuth for Drive.",
                            null,
                            null));
            return;
        }

        QueryCacheScope cacheScope =
                "demo".equalsIgnoreCase(connectorMode) || reqHeaders.userIdHeader() != null
                        ? principals.cacheScopeFor(effectiveUserIdForScope)
                        : QueryCacheScope.from(requestUser);
        Principal principal = principals.lookup(requestUser);
        String tenantId = principal.equals(Principal.anonymous()) ? null : cacheScope.tenantId();
        RequestContext ctx =
                new RequestContext(
                        reqHeaders.traceId(), requestUser, cacheScope, tenantId, Instant.now());

        // Cache-Control: max-age=N header overrides body maxStaleness for freshness control.
        HttpQueryPayload effectivePayload =
                reqHeaders.maxStaleness() != null
                        ? new HttpQueryPayload(
                                payload.sql(),
                                payload.pageNumber(),
                                payload.logicalCursorOffset(),
                                payload.requestPageSize(),
                                reqHeaders.maxStaleness())
                        : payload;

        ResponseContext rc = webQueryService.execute(connectorMode, ctx, effectivePayload.toRequest());

        if (rc.outcome() instanceof ResponseContext.Outcome.Success success) {
            JsonObject result = success.body();
            WebQueryTraceLogger.appendTrace(reqHeaders.traceId(), connectorMode, payload.sql(), result);
            if (reqHeaders.wantsJson()) {
                HttpEnvelope.writeSuccessJson(ex, reqHeaders, ctx, result);
            } else {
                String html =
                        "<!DOCTYPE html><html><head><meta charset=\"utf-8\"><title>Result</title></head><body>"
                                + "<h2>Result</h2><pre>"
                                + esc(GSON.toJson(result))
                                + "</pre><p><a href=\"/\">Back</a></p></body></html>";
                HttpEnvelope.writeSuccessHtml(ex, reqHeaders, ctx, result, html);
            }
        } else {
            HttpEnvelope.writeFailure(
                    ex, reqHeaders, ctx, (ResponseContext.Outcome.Failure) rc.outcome());
        }
    }

    /**
     * Prometheus text-exposition endpoint. Returns the full counter/histogram registry; no
     * authentication. Scrape with: {@code curl -s http://localhost:8080/metrics}.
     */
    private static void handleMetrics(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            sendBytes(ex, 405, "text/plain", "GET only");
            return;
        }
        byte[] body = org.emathp.metrics.Metrics.exposition().getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        ex.sendResponseHeaders(200, body.length);
        ex.getResponseBody().write(body);
        ex.close();
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
