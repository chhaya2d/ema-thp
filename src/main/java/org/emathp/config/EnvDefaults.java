package org.emathp.config;

/**
 * File-level defaults for {@link RuntimeEnv}; separate from {@link WebDefaults} so dot-env loading
 * stays usable without pulling in HTTP/DB defaults.
 */
public final class EnvDefaults {

    private EnvDefaults() {}

    /** File read by {@link RuntimeEnv#loadDotEnv()} relative to the process working directory. */
    public static final String DOT_ENV_FILENAME = ".env";
}
