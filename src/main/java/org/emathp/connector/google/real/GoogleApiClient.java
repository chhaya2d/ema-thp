package org.emathp.connector.google.real;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.emathp.connector.google.api.GoogleDriveFile;
import org.emathp.connector.google.api.GoogleSearchRequest;
import org.emathp.connector.google.api.GoogleSearchResponse;

/**
 * Production {@link GoogleDriveRpc}: REST {@code GET https://www.googleapis.com/drive/v3/files}.
 */
public final class GoogleApiClient implements GoogleDriveRpc {

    private static final String LIST_URL = "https://www.googleapis.com/drive/v3/files";

    private final HttpClient http;
    private final Gson gson = new Gson();

    public GoogleApiClient() {
        this(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build());
    }

    GoogleApiClient(HttpClient http) {
        this.http = http;
    }

    @Override
    public GoogleSearchResponse listFiles(String accessToken, GoogleSearchRequest request) throws Exception {
        URI uri = buildUri(request);
        HttpRequest hr = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(60))
                .header("Authorization", "Bearer " + accessToken)
                .GET()
                .build();
        HttpResponse<String> res = http.send(hr, HttpResponse.BodyHandlers.ofString());
        if (res.statusCode() >= 400) {
            throw new IllegalStateException(
                    "Drive files.list HTTP " + res.statusCode() + ": " + truncate(res.body(), 500));
        }
        return parseListBody(res.body());
    }

    private static String truncate(String s, int max) {
        if (s == null || s.length() <= max) {
            return s;
        }
        return s.substring(0, max) + "...";
    }

    private URI buildUri(GoogleSearchRequest request) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("spaces", "drive");
        params.put("supportsAllDrives", "false");
        params.put("includeItemsFromAllDrives", "false");
        if (request.fields() != null && !request.fields().isBlank()) {
            params.put("fields", request.fields());
        }
        if (request.q() != null && !request.q().isBlank()) {
            params.put("q", request.q());
        }
        if (request.orderBy() != null && !request.orderBy().isBlank()) {
            params.put("orderBy", request.orderBy());
        }
        if (request.pageSize() != null) {
            params.put("pageSize", String.valueOf(request.pageSize()));
        }
        if (request.pageToken() != null && !request.pageToken().isBlank()) {
            params.put("pageToken", request.pageToken());
        }

        StringJoiner joiner = new StringJoiner("&");
        for (Map.Entry<String, String> e : params.entrySet()) {
            joiner.add(enc(e.getKey()) + "=" + enc(e.getValue()));
        }
        return URI.create(LIST_URL + "?" + joiner);
    }

    private static String enc(String v) {
        return URLEncoder.encode(v, StandardCharsets.UTF_8);
    }

    private GoogleSearchResponse parseListBody(String body) {
        JsonObject root = gson.fromJson(body, JsonObject.class);
        if (root == null) {
            return new GoogleSearchResponse(List.of(), null);
        }
        String next =
                root.has("nextPageToken") && !root.get("nextPageToken").isJsonNull()
                        ? root.get("nextPageToken").getAsString()
                        : null;
        JsonElement filesEl = root.get("files");
        if (filesEl == null || !filesEl.isJsonArray()) {
            return new GoogleSearchResponse(List.of(), next);
        }
        JsonArray arr = filesEl.getAsJsonArray();
        List<GoogleDriveFile> files = new ArrayList<>();
        for (JsonElement el : arr) {
            if (el.isJsonObject()) {
                files.add(toFile(el.getAsJsonObject()));
            }
        }
        return new GoogleSearchResponse(files, next);
    }

    private static GoogleDriveFile toFile(JsonObject f) {
        String id = f.has("id") ? f.get("id").getAsString() : "";
        String name = f.has("name") && !f.get("name").isJsonNull() ? f.get("name").getAsString() : "";
        String web =
                f.has("webViewLink") && !f.get("webViewLink").isJsonNull()
                        ? f.get("webViewLink").getAsString()
                        : "";
        Instant mod = parseInstant(f, "modifiedTime");
        Instant created = parseInstant(f, "createdTime");
        if (created == null) {
            created = mod != null ? mod : Instant.EPOCH;
        }
        if (mod == null) {
            mod = created;
        }

        String owner = resolveOwner(f);
        List<String> modifiers = resolveModifiers(f, owner);

        return new GoogleDriveFile(id, name, owner, modifiers, created, mod, web);
    }

    private static Instant parseInstant(JsonObject f, String field) {
        if (!f.has(field) || f.get(field).isJsonNull()) {
            return null;
        }
        return Instant.parse(f.get(field).getAsString());
    }

    private static String resolveOwner(JsonObject f) {
        if (f.has("owners") && f.get("owners").isJsonArray()) {
            JsonArray ow = f.getAsJsonArray("owners");
            if (!ow.isEmpty() && ow.get(0).isJsonObject()) {
                JsonObject o = ow.get(0).getAsJsonObject();
                if (o.has("emailAddress") && !o.get("emailAddress").isJsonNull()) {
                    return o.get("emailAddress").getAsString();
                }
                if (o.has("displayName") && !o.get("displayName").isJsonNull()) {
                    return o.get("displayName").getAsString();
                }
            }
        }
        return "unknown";
    }

    private static List<String> resolveModifiers(JsonObject f, String owner) {
        List<String> mods = new ArrayList<>();
        if (f.has("lastModifyingUser") && f.get("lastModifyingUser").isJsonObject()) {
            JsonObject lm = f.getAsJsonObject("lastModifyingUser");
            String m = null;
            if (lm.has("emailAddress") && !lm.get("emailAddress").isJsonNull()) {
                m = lm.get("emailAddress").getAsString();
            } else if (lm.has("displayName") && !lm.get("displayName").isJsonNull()) {
                m = lm.get("displayName").getAsString();
            }
            if (m != null && !m.equals(owner)) {
                mods.add(m);
            } else if (m != null) {
                mods.add(m);
            }
        }
        if (mods.size() > 1) {
            // Dedupe while preserving order for GoogleDriveFile uniqueness contract
            List<String> distinct = new ArrayList<>();
            for (String s : mods) {
                if (!distinct.contains(s)) {
                    distinct.add(s);
                }
            }
            mods = distinct;
        }
        return List.copyOf(mods);
    }
}
