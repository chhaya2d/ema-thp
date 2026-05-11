package org.emathp.cache;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.emathp.model.ComparisonExpr;
import org.emathp.model.Direction;
import org.emathp.model.JoinPredicate;
import org.emathp.model.JoinQuery;
import org.emathp.model.JoinSide;
import org.emathp.model.JoinWhere;
import org.emathp.model.Operator;
import org.emathp.model.OrderBy;
import org.emathp.model.ParsedQuery;
import org.emathp.model.Query;

/**
 * Deterministic text form of a parsed {@link ParsedQuery} for cache keys. Normalizes identifier
 * casing only (SQL string literal values are preserved).
 *
 * <p><b>Tradeoffs</b>
 *
 * <ul>
 *   <li>Whitespace in the original SQL is irrelevant — we canonicalize the <em>model</em>, not the
 *       raw string.</li>
 *   <li>Select list order is preserved (may affect client column order).</li>
 *   <li>Semantically equivalent SQL that parses to different models still misses cache (acceptable
 *       for v1).</li>
 * </ul>
 */
public final class ParsedQueryNormalizer {

    private ParsedQueryNormalizer() {}

    public static String canonical(ParsedQuery pq) {
        return switch (pq) {
            case Query q -> canonicalQuery(q);
            case JoinQuery j -> canonicalJoin(j);
        };
    }

    /**
     * Cache identity for single-source federation: same logical SELECT/WHERE/order/limit/UI page size,
     * <em>ignoring any future user-level cursor embedded in {@link Query}</em> (UI paging uses a
     * separate HTTP cursor and slices a cached full run — see {@code WebQueryRunner}).
     *
     * <p>Today {@link Query#cursor()} after shaping is always null anyway; this method freezes that
     * intent if the engine later threads other cursor kinds through {@link Query}.
     */
    public static String canonicalLogicalSingleSource(Query shapedForRunner) {
        Query keyQuery = shapedForRunner.withPagination(null, shapedForRunner.pageSize());
        return canonicalQuery(keyQuery);
    }

    /**
     * Identity for filesystem snapshots: logical SQL only — no UI pagination or resume cursor.
     * Independent of {@link Query#pageSize()} used only to enable planner pagination pushdown.
     */
    public static String canonicalSnapshotIdentity(Query q) {
        return canonicalQuery(q.withPagination(null, null));
    }

    /** Snapshot tree identity for a join: logical join only (no federated result cursor / page size). */
    public static String canonicalSnapshotIdentity(JoinQuery j) {
        return canonicalJoin(j.withPagination(null, null));
    }

    private static String canonicalQuery(Query q) {
        StringBuilder sb = new StringBuilder();
        sb.append("Q|v1|sel:");
        sb.append(String.join(",", mapSelect(q.select())));
        sb.append("|where:");
        sb.append(whereCanon(q.where()));
        sb.append("|ob:");
        sb.append(orderByCanon(q.orderBy()));
        sb.append("|lim:");
        sb.append(q.limit() == null ? "_" : q.limit());
        sb.append("|cur:");
        sb.append(q.cursor() == null ? "_" : escape(q.cursor()));
        sb.append("|ps:");
        sb.append(q.pageSize() == null ? "_" : q.pageSize());
        return sb.toString();
    }

    private static String canonicalJoin(JoinQuery j) {
        StringBuilder sb = new StringBuilder();
        sb.append("J|v1|L:");
        sb.append(sideCanon(j.left()));
        sb.append("|R:");
        sb.append(sideCanon(j.right()));
        sb.append("|on:");
        sb.append(predicateCanon(j.on()));
        sb.append("|jw:");
        sb.append(joinWhereCanon(j.where()));
        sb.append("|sel:");
        sb.append(String.join(",", mapSelect(j.select())));
        sb.append("|lim:");
        sb.append(j.limit() == null ? "_" : j.limit());
        sb.append("|cur:");
        sb.append(j.cursor() == null ? "_" : escape(j.cursor()));
        sb.append("|ps:");
        sb.append(j.pageSize() == null ? "_" : j.pageSize());
        return sb.toString();
    }

    private static List<String> mapSelect(List<String> select) {
        List<String> out = new ArrayList<>(select.size());
        for (String s : select) {
            out.add(qualCanon(s));
        }
        return out;
    }

    /** Lowercase each segment of a dotted name ({@code G.title} and {@code g.Title} align). */
    private static String qualCanon(String qual) {
        if (qual == null || qual.isEmpty()) {
            return "";
        }
        String[] parts = qual.split("\\.", -1);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                sb.append('.');
            }
            sb.append(parts[i].toLowerCase(Locale.ROOT));
        }
        return sb.toString();
    }

    private static String sideCanon(JoinSide s) {
        return s.connectorName().toLowerCase(Locale.ROOT)
                + ':'
                + s.alias().toLowerCase(Locale.ROOT);
    }

    private static String predicateCanon(JoinPredicate p) {
        return p.leftAlias().toLowerCase(Locale.ROOT)
                + '.'
                + p.leftField().toLowerCase(Locale.ROOT)
                + '='
                + p.rightAlias().toLowerCase(Locale.ROOT)
                + '.'
                + p.rightField().toLowerCase(Locale.ROOT);
    }

    private static String joinWhereCanon(JoinWhere w) {
        if (w == null) {
            return "_";
        }
        return w.alias().toLowerCase(Locale.ROOT) + ':' + comparisonCanon(w.predicate());
    }

    private static String whereCanon(org.emathp.model.ComparisonExpr w) {
        if (w == null) {
            return "_";
        }
        return comparisonCanon(w);
    }

    private static String comparisonCanon(ComparisonExpr w) {
        return fieldCanon(w.field())
                + ':'
                + w.operator().name()
                + ':'
                + valueCanon(w.value());
    }

    private static String fieldCanon(String f) {
        if (f == null) {
            return "";
        }
        if (f.contains(".")) {
            return qualCanon(f);
        }
        return f.toLowerCase(Locale.ROOT);
    }

    private static String valueCanon(Object v) {
        if (v == null) {
            return "null";
        }
        if (v instanceof Instant i) {
            return i.toString();
        }
        if (v instanceof Number || v instanceof Boolean) {
            return v.toString();
        }
        // String literals: preserve exact equality semantics
        return "s:" + escape(String.valueOf(v));
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("|", "\\|");
    }

    private static String orderByCanon(List<OrderBy> orderBy) {
        if (orderBy.isEmpty()) {
            return "_";
        }
        List<String> parts = new ArrayList<>();
        for (OrderBy o : orderBy) {
            Direction d = o.direction() == null ? Direction.ASC : o.direction();
            parts.add(fieldCanon(o.field()) + ':' + d.name());
        }
        return String.join(",", parts);
    }

    /**
     * Web single-source profile includes UI page size layered in {@link
     * org.emathp.web.WebQueryRunner} (distinct from connector {@code defaultFetchPageSize}).
     */
    public static String webRunnerSingleProfile(int uiPageSize) {
        return "web:single:uiPageSize=" + uiPageSize;
    }

    public static final String PROFILE_WEB_JOIN = "web:join:firstPageSize=1:v1";

    public static String cacheKey(
            QueryCacheScope scope, String canonicalParsedQuery, String executionProfile) {
        return scope.keySegment()
                + "\u0001"
                + executionProfile
                + "\u0001"
                + canonicalParsedQuery;
    }
}
