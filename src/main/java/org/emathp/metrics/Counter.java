package org.emathp.metrics;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Monotonic counter with optional labels. Emits Prometheus text exposition format
 * ({@code # HELP}, {@code # TYPE}, label-keyed series).
 *
 * <p>Hand-rolled to avoid the {@code io.prometheus:simpleclient} dependency for a prototype.
 */
public final class Counter {

    private final String name;
    private final String help;
    private final String[] labelNames;
    private final ConcurrentMap<String, AtomicLong> values = new ConcurrentHashMap<>();

    public Counter(String name, String help, String... labelNames) {
        this.name = Objects.requireNonNull(name, "name");
        this.help = Objects.requireNonNull(help, "help");
        this.labelNames = labelNames == null ? new String[0] : labelNames;
    }

    public void inc(String... labelValues) {
        inc(1L, labelValues);
    }

    public void inc(long delta, String... labelValues) {
        if (labelValues.length != labelNames.length) {
            throw new IllegalArgumentException(
                    "expected " + labelNames.length + " labels, got " + labelValues.length);
        }
        String key = MetricsKeys.join(labelValues);
        values.computeIfAbsent(key, k -> new AtomicLong()).addAndGet(delta);
    }

    void writeTo(StringBuilder sb) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" counter\n");
        if (values.isEmpty()) {
            sb.append(name);
            if (labelNames.length > 0) sb.append("{}");
            sb.append(" 0\n");
            return;
        }
        for (Map.Entry<String, AtomicLong> e : values.entrySet()) {
            sb.append(name);
            String labels = MetricsKeys.labelsToProm(labelNames, e.getKey());
            if (!labels.isEmpty()) sb.append('{').append(labels).append('}');
            sb.append(' ').append(e.getValue().get()).append('\n');
        }
    }
}
