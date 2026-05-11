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
import org.emathp.connector.google.mock.GoogleDriveConnector;
import org.emathp.connector.google.real.GoogleApiClient;
import org.emathp.connector.google.real.GoogleTokenStore;
import org.emathp.connector.google.real.RealGoogleDriveConnector;
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
 * Minimal embedded HTTP server: Google OAuth (PKCE) + JSON SQL execution against real Google Drive
 * and mock Notion — same planner/engine as {@link org.emathp.Main}. Binds to loopback only.
 */
public final class DemoWebServer {

    private static final Gson GSON = new Gson();

    /**
     * Home page: SQL playground with prev/next UI paging (POST JSON to /api/query). Placeholders:
     * ${H1}, ${INTRO}, ${DEFAULT_SQL}, ${DEFAULT_UI_PS}, ${MAX_UI_PS}.
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
            <p><label>UI page size <input type="number" id="pageSize" min="1" max="${MAX_UI_PS}" value="${DEFAULT_UI_PS}"></label></p>
            <p>
              <button type="button" id="runBtn">Run</button>
              <button type="button" id="prevBtn" disabled>Prev</button>
              <button type="button" id="nextBtn" disabled>Next</button>
              <span id="pageInfo"></span> <span id="status"></span>
            </p>
            <h3>Response</h3>
            <pre id="out"></pre>
            <script>
            (function() {
              const runBtn = document.getElementById('runBtn');
              const prevBtn = document.getElementById('prevBtn');
              const nextBtn = document.getElementById('nextBtn');
              const pageInfo = document.getElementById('pageInfo');
              const status = document.getElementById('status');
              const out = document.getElementById('out');
              let lastNext = null;
              let lastPrev = null;
              async function runQuery(cursor) {
                status.textContent = '';
                const sql = document.getElementById('sql').value;
                const pageSize = parseInt(document.getElementById('pageSize').value, 10);
                const payload = { sql: sql, pageSize: pageSize };
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
                  status.textContent = 'HTTP ' + res.status;
                  return;
                }
                out.textContent = JSON.stringify(j, null, 2);
                if (!res.ok) {
                  status.textContent = 'HTTP ' + res.status;
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
              }
              runBtn.onclick = function() { runQuery(null); };
              nextBtn.onclick = function() { if (lastNext != null) runQuery(lastNext); };
              prevBtn.onclick = function() { if (lastPrev != null) runQuery(lastPrev); };
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
        if (env.useMockConnectors()) {
            runMockConnectors(env);
        } else {
            runRealGoogle(env);
        }
    }

    /**
     * Mock Google + mock Notion — no DB, no OAuth. For quick local UI tests (same stack as {@link
     * org.emathp.Main}).
     */
    private static void runMockConnectors(WebEnv env) throws Exception {
        Map<String, Connector> connectorsByName = new LinkedHashMap<>();
        connectorsByName.put("google", new GoogleDriveConnector());
        connectorsByName.put("notion", new NotionConnector());

        SQLParserService parser = new SQLParserService();
        Planner planner = new Planner(true);
        QueryExecutor executor = new QueryExecutor();
        JoinExecutor joinExecutor = new JoinExecutor(planner, executor);

        SnapshotQueryService snapshots =
                new SnapshotQueryService(
                        planner,
                        executor,
                        new FsSnapshotStore(Path.of("data")),
                        new SystemClock(),
                        WebDefaults.snapshotChunkFreshness());
        WebQueryRunner queryRunner =
                new WebQueryRunner(
                        parser,
                        planner,
                        executor,
                        joinExecutor,
                        List.copyOf(connectorsByName.values()),
                        connectorsByName,
                        UserContext.anonymous(),
                        snapshots,
                        SnapshotEnvironment.TEST,
                        WebDefaults.UI_QUERY_PAGE_SIZE_MOCK);

        HttpServer http =
                HttpServer.create(
                        new InetSocketAddress(
                                java.net.InetAddress.getByName(WebDefaults.HTTP_BIND_ADDRESS), env.webPort()),
                        0);
        http.createContext("/", exchange -> handleRootMock(exchange));
        http.createContext("/health", exchange -> sendBytes(exchange, 200, "text/plain", "ok"));
        http.createContext("/oauth/google/start", exchange -> oauthDisabled(exchange));
        http.createContext("/oauth/google/callback", exchange -> oauthDisabled(exchange));
        http.createContext(
                "/api/query",
                exchange -> handleQuery(exchange, queryRunner, env.demoUserId(), null));

        http.setExecutor(null);
        http.start();
        System.out.println("Demo web UI (MOCK connectors): http://localhost:" + env.webPort() + "/");
        System.out.println("Set EMA_WEB_USE_MOCK_CONNECTORS=false for real Google + Postgres.");

        Thread.currentThread().join();
    }

    private static void oauthDisabled(HttpExchange exchange) throws IOException {
        sendBytes(
                exchange,
                503,
                "text/plain; charset=utf-8",
                "OAuth is disabled when EMA_WEB_USE_MOCK_CONNECTORS=true.");
    }

    private static void handleRootMock(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            sendBytes(ex, 405, "text/plain", "Method not allowed");
            return;
        }
        String defaultSql =
                """
                SELECT title, updatedAt
                FROM resources
                WHERE updatedAt > '2026-01-01'
                ORDER BY updatedAt DESC
                LIMIT 20
                """;
        String html =
                federationQueryPlaygroundPage(
                        "Federated query demo <small>(mock Google + mock Notion)</small>",
                        "<p>No OAuth or Postgres — in-memory connectors only.</p>",
                        WebDefaults.UI_QUERY_PAGE_SIZE_MOCK,
                        defaultSql);
        sendBytes(ex, 200, "text/html; charset=utf-8", html);
    }

    private static void runRealGoogle(WebEnv env) throws Exception {
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
        connectorsByName.put("notion", new NotionConnector());

        SQLParserService parser = new SQLParserService();
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
        WebQueryRunner queryRunner =
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

        HttpServer http =
                HttpServer.create(
                        new InetSocketAddress(
                                java.net.InetAddress.getByName(WebDefaults.HTTP_BIND_ADDRESS), env.webPort()),
                        0);
        http.createContext("/", exchange -> handleRoot(exchange, env));
        http.createContext("/health", exchange -> sendBytes(exchange, 200, "text/plain", "ok"));
        http.createContext("/oauth/google/start", exchange -> handleOAuthStart(exchange, oauth));
        http.createContext("/oauth/google/callback", exchange -> handleOAuthCallback(exchange, oauth, store, encryptor, env));
        http.createContext(
                "/api/query", exchange -> handleQuery(exchange, queryRunner, env.demoUserId(), store));

        http.setExecutor(null);
        http.start();
        System.out.println("Demo web UI: http://localhost:" + env.webPort() + "/");
        System.out.println("OAuth callback must match: " + env.oauthRedirectUri());

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

    private static void handleRoot(HttpExchange ex, WebEnv env) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            sendBytes(ex, 405, "text/plain", "Method not allowed");
            return;
        }
        String q = ex.getRequestURI().getQuery();
        boolean connected = q != null && q.contains("connected=1");
        String oauthLine =
                "<p><a href=\"/oauth/google/start\">Connect Google Drive</a> (OAuth — browser opens Google)</p>\n";
        String connLine =
                connected
                        ? "<p><strong>Google connected for user <code>"
                                + esc(env.demoUserId())
                                + "</code>.</strong></p>\n"
                        : "<p>Not connected — Drive queries will fail until you connect.</p>\n";
        String intro =
                oauthLine
                        + connLine
                        + "<p>Same dialect as CLI demos. Single-source runs against <em>each</em> connector (real "
                        + "Google + mock Notion).</p>\n";
        String defaultSql =
                """
                SELECT title, updatedAt
                FROM resources
                WHERE updatedAt > '2020-01-01'
                ORDER BY updatedAt DESC
                LIMIT 20
                """;
        String html =
                federationQueryPlaygroundPage(
                        "Federated query demo", intro, WebDefaults.UI_QUERY_PAGE_SIZE, defaultSql);
        sendBytes(ex, 200, "text/html; charset=utf-8", html);
    }

    private static String federationQueryPlaygroundPage(
            String h1InnerHtml, String introHtml, int defaultUiPageSize, String defaultSql) {
        return FEDERATION_QUERY_PLAYGROUND_TEMPLATE.replace("${H1}", h1InnerHtml)
                .replace("${INTRO}", introHtml)
                .replace("${DEFAULT_SQL}", esc(defaultSql.strip()))
                .replace("${DEFAULT_UI_PS}", Integer.toString(defaultUiPageSize))
                .replace("${MAX_UI_PS}", Integer.toString(WebDefaults.UI_QUERY_PAGE_SIZE_CLIENT_MAX));
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

    private static void handleQuery(HttpExchange ex, WebQueryRunner runner, String demoUserId, GoogleTokenStore store)
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
        }
        if (sql == null || sql.isBlank()) {
            errorJson(ex, "missing sql");
            return;
        }

        try {
            // Real Google path: require OAuth tokens. Mock connector path passes store=null.
            if (store != null && missingGoogleAccount(store, demoUserId)) {
                JsonObject hint = new JsonObject();
                hint.addProperty("ok", false);
                hint.addProperty(
                        "error",
                        "Connect Google first (/oauth/google/start). This demo always executes the "
                                + "real Google connector alongside mock Notion.");
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
            JsonObject result = runner.run(sql, pageNumber, cursor, pageSizeReq, maxStaleness, runner.cacheScope());
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
