package org.emathp.model;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Canonical engine / connector row: a flat bag of named values. Single-source rows use API field
 * names ({@code title}, {@code id}, …). Join output merges sides with qualified keys ({@code
 * g.title}, {@code n.title}).
 */
public record EngineRow(Map<String, Object> fields) {

    public EngineRow {
        fields = fields == null ? Map.of() : Map.copyOf(fields);
    }

    /** Value for residual WHERE / ORDER BY / join keys; missing keys yield {@code null}. */
    public Object get(String name) {
        return fields.get(name);
    }

    /** Prefix every key as {@code alias + "." + key} (iteration order preserved). */
    public static EngineRow qualify(String alias, EngineRow bare) {
        Objects.requireNonNull(alias, "alias");
        Objects.requireNonNull(bare, "bare");
        if (bare.fields().isEmpty()) {
            return bare;
        }
        Map<String, Object> out = new LinkedHashMap<>(bare.fields().size());
        for (var e : bare.fields().entrySet()) {
            out.put(alias + "." + e.getKey(), e.getValue());
        }
        return new EngineRow(out);
    }

    /** {@code putAll(b)} after {@code a}; on duplicate keys {@code b} wins. */
    public static EngineRow merge(EngineRow a, EngineRow b) {
        Objects.requireNonNull(a, "a");
        Objects.requireNonNull(b, "b");
        Map<String, Object> out = new LinkedHashMap<>(a.fields());
        out.putAll(b.fields());
        return new EngineRow(out);
    }
}
