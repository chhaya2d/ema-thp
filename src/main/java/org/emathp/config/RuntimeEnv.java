package org.emathp.config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Loads {@link EnvDefaults#DOT_ENV_FILENAME} from the process working directory once at startup.
 * {@link System#getenv(String)} cannot be mutated after JVM launch, so callers use {@link
 * #get(String, String)}: real OS environment variables take precedence when set and non-blank;
 * otherwise values from {@code .env} apply.
 */
public final class RuntimeEnv {

    private static volatile Map<String, String> dotenv = Map.of();

    /** True once {@link #loadDotEnv()} has scanned for {@code .env} (file may be absent or empty). */
    private static volatile boolean dotenvScanDone;

    private RuntimeEnv() {}

    /**
     * Parses the dot-env file in {@link Path#of(String, String...)}{@code (EnvDefaults.DOT_ENV_FILENAME)}
     * relative to {@code user.dir}
     * if the file exists. Idempotent — runs at most once per JVM.
     */
    public static void loadDotEnv() {
        synchronized (RuntimeEnv.class) {
            if (dotenvScanDone) {
                return;
            }
            dotenvScanDone = true;
            Path file = Path.of(EnvDefaults.DOT_ENV_FILENAME);
            if (!Files.isRegularFile(file)) {
                dotenv = Map.of();
                return;
            }
            try {
                String raw = Files.readString(file, StandardCharsets.UTF_8);
                dotenv = Map.copyOf(parseDotEnvBody(raw));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read .env from " + file.toAbsolutePath(), e);
            }
        }
    }

    /** Parses the body of a {@code .env} file ({@code KEY=value}, {@code #} comments). */
    static Map<String, String> parseDotEnvBody(String raw) {
        if (raw == null || raw.isEmpty()) {
            return Map.of();
        }
        if (raw.startsWith("\uFEFF")) {
            raw = raw.substring(1);
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (String line : raw.split("\\R")) {
            String t = line.trim();
            if (t.isEmpty() || t.startsWith("#")) {
                continue;
            }
            int eq = t.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            String key = t.substring(0, eq).trim();
            if (key.isEmpty()) {
                continue;
            }
            String val = t.substring(eq + 1).trim();
            val = stripQuotes(val);
            out.put(key, val);
        }
        return out;
    }

    private static String stripQuotes(String val) {
        if (val.length() >= 2) {
            if ((val.startsWith("\"") && val.endsWith("\"")) || (val.startsWith("'") && val.endsWith("'"))) {
                return val.substring(1, val.length() - 1);
            }
        }
        return val;
    }

    /** OS env if non-blank; else {@code .env}; else {@code defaultVal}. */
    public static String get(String key, String defaultVal) {
        Objects.requireNonNull(key, "key");
        String sys = System.getenv(key);
        if (sys != null && !sys.isBlank()) {
            return sys;
        }
        String fromFile = dotenv.get(key);
        if (fromFile != null && !fromFile.isBlank()) {
            return fromFile;
        }
        return defaultVal;
    }

    /** OS env if non-blank; else {@code .env}; else {@code null}. */
    public static String getOrNull(String key) {
        String sys = System.getenv(key);
        if (sys != null && !sys.isBlank()) {
            return sys;
        }
        String fromFile = dotenv.get(key);
        if (fromFile != null && !fromFile.isBlank()) {
            return fromFile;
        }
        return null;
    }

    /**
     * Required variable: OS env or {@code .env} must supply a non-blank value.
     *
     * @throws IllegalStateException if missing
     */
    public static String require(String key) {
        String v = getOrNull(key);
        if (v == null) {
            throw new IllegalStateException("Missing required environment variable: " + key);
        }
        return v;
    }
}
