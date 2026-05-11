package org.emathp.engine;

import java.util.List;
import java.util.Objects;

/**
 * Engine-level logical {@code LIMIT} over an in-memory row list: single-source execution (after
 * residuals) or joined rows. Does not fetch data.
 */
public final class LimitExecutor {

    private LimitExecutor() {}

    /**
     * Rows after applying {@code limit}, plus whether the cap cut the stream. When {@code limit}
     * is {@code null}, returns a {@linkplain List#copyOf copy} of all rows and {@code
     * stoppedAtLimit == false}.
     */
    public record Limited<T>(List<T> rows, int totalBeforeLimit, boolean stoppedAtLimit) {

        public Limited {
            rows = Objects.requireNonNull(rows, "rows");
        }
    }

    public static <T> Limited<T> apply(List<T> rows, Integer limit) {
        Objects.requireNonNull(rows, "rows");
        int total = rows.size();
        if (limit == null || total <= limit) {
            return new Limited<>(List.copyOf(rows), total, false);
        }
        return new Limited<>(List.copyOf(rows.subList(0, limit)), total, true);
    }
}
