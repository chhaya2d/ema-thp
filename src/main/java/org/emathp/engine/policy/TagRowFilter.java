package org.emathp.engine.policy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.emathp.model.EngineRow;

/** Applies {@link TagAccessPolicy} to rows before logical LIMIT. */
public final class TagRowFilter {

    public static final String TAG_FIELD = "tags";

    private TagRowFilter() {}

    /**
     * Rows missing {@value #TAG_FIELD} or carrying an empty tag list always pass (permissive).
     * Otherwise at least one normalized tag must appear in {@link TagAccessPolicy#allowedTags()}.
     */
    public static List<EngineRow> apply(List<EngineRow> rows, TagAccessPolicy policy) {
        if (policy == null || policy.allowedTags().isEmpty()) {
            return rows;
        }
        List<EngineRow> out = new ArrayList<>(rows.size());
        for (EngineRow row : rows) {
            if (passes(row, policy)) {
                out.add(row);
            }
        }
        return out;
    }

    static boolean passes(EngineRow row, TagAccessPolicy policy) {
        Object raw = row.fields().get(TAG_FIELD);
        Set<String> rowTags = normalizeTags(raw);
        if (rowTags.isEmpty()) {
            return true;
        }
        for (String t : rowTags) {
            if (policy.containsAllowedLiteral(t)) {
                return true;
            }
        }
        return false;
    }

    private static Set<String> normalizeTags(Object raw) {
        if (raw == null) {
            return Set.of();
        }
        if (raw instanceof Collection<?> c) {
            Set<String> s = new HashSet<>();
            for (Object o : c) {
                if (o != null && !String.valueOf(o).isBlank()) {
                    s.add(String.valueOf(o).trim().toLowerCase(Locale.ROOT));
                }
            }
            return s;
        }
        String one = String.valueOf(raw).trim();
        if (one.isEmpty()) {
            return Set.of();
        }
        return Set.of(one.toLowerCase(Locale.ROOT));
    }
}
