package org.emathp.metrics;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Fixed-bucket histogram with optional labels. Emits Prometheus text exposition format:
 * {@code _bucket{le="…"}} cumulative counts, {@code _sum}, {@code _count}.
 *
 * <p>Synchronizes per-label-series on observe; fine for prototype contention levels.
 */
public final class Histogram {

    /** Default bucket boundaries in the metric's natural unit (seconds or ms — caller decides). */
    public static final double[] DEFAULT_SECONDS_BUCKETS = {
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
    };

    public static final double[] DEFAULT_MS_BUCKETS = {
        50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 300000
    };

    private final String name;
    private final String help;
    private final String[] labelNames;
    private final double[] buckets;
    private final ConcurrentMap<String, Series> series = new ConcurrentHashMap<>();

    public Histogram(String name, String help, double[] buckets, String... labelNames) {
        this.name = Objects.requireNonNull(name, "name");
        this.help = Objects.requireNonNull(help, "help");
        this.buckets = buckets;
        this.labelNames = labelNames == null ? new String[0] : labelNames;
    }

    public void observe(double value, String... labelValues) {
        if (labelValues.length != labelNames.length) {
            throw new IllegalArgumentException(
                    "expected " + labelNames.length + " labels, got " + labelValues.length);
        }
        String key = MetricsKeys.join(labelValues);
        series.computeIfAbsent(key, k -> new Series(buckets.length)).observe(value, buckets);
    }

    void writeTo(StringBuilder sb) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" histogram\n");
        if (series.isEmpty()) {
            // Emit a stub so /metrics still shows the metric existed.
            for (double b : buckets) {
                sb.append(name).append("_bucket{le=\"").append(b).append("\"} 0\n");
            }
            sb.append(name).append("_bucket{le=\"+Inf\"} 0\n");
            sb.append(name).append("_sum 0\n");
            sb.append(name).append("_count 0\n");
            return;
        }
        for (Map.Entry<String, Series> e : series.entrySet()) {
            Series s = e.getValue();
            String labels = MetricsKeys.labelsToProm(labelNames, e.getKey());
            String prefix = labels.isEmpty() ? "" : labels + ",";
            long[] bc;
            double sum;
            long count;
            synchronized (s) {
                bc = s.bucketCounts.clone();
                sum = s.sum;
                count = s.count;
            }
            long cumulative = 0;
            for (int i = 0; i < buckets.length; i++) {
                cumulative += bc[i];
                sb.append(name)
                        .append("_bucket{")
                        .append(prefix)
                        .append("le=\"")
                        .append(buckets[i])
                        .append("\"} ")
                        .append(cumulative)
                        .append('\n');
            }
            sb.append(name)
                    .append("_bucket{")
                    .append(prefix)
                    .append("le=\"+Inf\"} ")
                    .append(count)
                    .append('\n');
            String labelClause = labels.isEmpty() ? "" : "{" + labels + "}";
            sb.append(name).append("_sum").append(labelClause).append(' ').append(sum).append('\n');
            sb.append(name).append("_count").append(labelClause).append(' ').append(count).append('\n');
        }
    }

    private static final class Series {
        final long[] bucketCounts;
        double sum;
        long count;

        Series(int n) {
            bucketCounts = new long[n];
        }

        synchronized void observe(double v, double[] buckets) {
            for (int i = 0; i < buckets.length; i++) {
                if (v <= buckets[i]) {
                    bucketCounts[i]++;
                }
            }
            sum += v;
            count++;
        }
    }
}
