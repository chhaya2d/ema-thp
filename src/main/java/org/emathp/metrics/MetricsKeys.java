package org.emathp.metrics;

/**
 * Label encoding for the {@link Counter} / {@link Histogram} ConcurrentMap key, plus the
 * Prometheus exposition-format renderer. Label values are joined with {@code } (control
 * char) so SQL/connector names with commas / equals don't collide.
 */
final class MetricsKeys {

    private static final String SEP = "";

    private MetricsKeys() {}

    static String join(String[] labelValues) {
        if (labelValues.length == 0) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < labelValues.length; i++) {
            if (i > 0) sb.append(SEP);
            sb.append(labelValues[i] == null ? "" : labelValues[i]);
        }
        return sb.toString();
    }

    static String labelsToProm(String[] labelNames, String key) {
        if (labelNames.length == 0) return "";
        String[] parts = key.split(SEP, -1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < labelNames.length; i++) {
            if (i > 0) sb.append(',');
            sb.append(labelNames[i])
                    .append("=\"")
                    .append(escape(i < parts.length ? parts[i] : ""))
                    .append('"');
        }
        return sb.toString();
    }

    private static String escape(String v) {
        return v.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
}
