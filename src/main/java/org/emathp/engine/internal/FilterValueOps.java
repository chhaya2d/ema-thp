package org.emathp.engine.internal;

import java.util.Locale;

/**
 * Value comparison and loose LIKE matching for in-memory filter/sort evaluation. Not a public
 * surface of the engine module; kept in {@code internal} alongside {@link RowFields}.
 */
public final class FilterValueOps {

    private FilterValueOps() {}

    public static int compareNullSafe(Object a, Object b) {
        if (a == null && b == null) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        @SuppressWarnings({"unchecked", "rawtypes"})
        int c = ((Comparable) a).compareTo(b);
        return c;
    }

    public static boolean likeMatch(Object actual, Object expected) {
        if (!(actual instanceof String s) || !(expected instanceof String pattern)) {
            return false;
        }
        String stripped = pattern;
        if (stripped.startsWith("%")) {
            stripped = stripped.substring(1);
        }
        if (stripped.endsWith("%")) {
            stripped = stripped.substring(0, stripped.length() - 1);
        }
        return s.toLowerCase(Locale.ROOT).contains(stripped.toLowerCase(Locale.ROOT));
    }
}
