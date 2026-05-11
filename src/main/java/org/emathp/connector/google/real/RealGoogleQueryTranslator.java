package org.emathp.connector.google.real;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.emathp.connector.google.api.GoogleSearchRequest;
import org.emathp.model.ComparisonExpr;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.Direction;
import org.emathp.model.Operator;
import org.emathp.model.OrderBy;

/**
 * Maps unified {@link ConnectorQuery} to <strong>real</strong> Google Drive v3 Files API strings
 * ({@code q}, {@code orderBy}, {@code fields}) using native field names {@code name} and
 * {@code modifiedTime}.
 */
public final class RealGoogleQueryTranslator {

    private static final DateTimeFormatter INSTANT_FORMAT = DateTimeFormatter.ISO_INSTANT;

    public GoogleSearchRequest translate(ConnectorQuery query) {
        String q = buildQ(query.where());
        String orderBy = buildOrderBy(query.orderBy());
        String fields = buildFieldsMask(query.projection());
        return new GoogleSearchRequest(q, orderBy, query.pageSize(), fields, query.cursor());
    }

    private String buildQ(ComparisonExpr where) {
        if (where == null) {
            return "";
        }
        return switch (where.operator()) {
            case EQ -> buildEquality(where);
            case GT, LT -> buildInstantCompare(where);
            case LIKE -> buildLike(where);
        };
    }

    private String buildEquality(ComparisonExpr where) {
        String providerField = mapField(where.field());
        if ("modifiedTime".equals(providerField)) {
            Instant instant = requireInstant(where.value());
            String iso = INSTANT_FORMAT.format(instant);
            return "modifiedTime = '" + iso + "'";
        }
        if ("name".equals(providerField)) {
            String literal = escapeQuotes(String.valueOf(where.value()));
            return "name = '" + literal + "'";
        }
        throw new IllegalArgumentException("Unsupported EQ field: " + where.field());
    }

    private String buildInstantCompare(ComparisonExpr where) {
        String providerField = mapField(where.field());
        if (!"modifiedTime".equals(providerField)) {
            throw new IllegalArgumentException("GT/LT only supported on updatedAt for real Drive translator");
        }
        Instant instant = requireInstant(where.value());
        String iso = INSTANT_FORMAT.format(instant);
        String op = where.operator() == Operator.GT ? ">" : "<";
        return "modifiedTime " + op + " '" + iso + "'";
    }

    private String buildLike(ComparisonExpr where) {
        String providerField = mapField(where.field());
        if (!"name".equals(providerField)) {
            throw new IllegalArgumentException("LIKE only supported on title for real Drive translator");
        }
        String pattern = String.valueOf(where.value());
        String literal = stripSqlWildcards(pattern);
        return "name contains '" + escapeQuotes(literal) + "'";
    }

    private static String stripSqlWildcards(String pattern) {
        String p = pattern;
        if (p.startsWith("%")) {
            p = p.substring(1);
        }
        if (p.endsWith("%")) {
            p = p.substring(0, p.length() - 1);
        }
        return p;
    }

    private String buildOrderBy(List<OrderBy> orderBy) {
        if (orderBy.isEmpty()) {
            return "";
        }
        return orderBy.stream().map(this::formatOrderClause).collect(Collectors.joining(", "));
    }

    private String formatOrderClause(OrderBy ob) {
        String apiField =
                switch (mapField(ob.field())) {
                    case "modifiedTime" -> "modifiedTime";
                    case "name" -> "name";
                    default -> "name";
                };
        String dir = ob.direction() == Direction.DESC ? "desc" : "asc";
        return apiField + " " + dir;
    }

    /**
     * Partial response mask for {@code files.list} — uses API field names (e.g. {@code name},
     * {@code modifiedTime}).
     */
    private String buildFieldsMask(List<String> projection) {
        Set<String> driveFields = new LinkedHashSet<>();
        driveFields.add("id");
        for (String logical : projection) {
            driveFields.add(mapField(logical));
        }
        driveFields.add("webViewLink");
        driveFields.add("createdTime");
        driveFields.add("owners");
        driveFields.add("lastModifyingUser");

        List<String> ordered = new ArrayList<>(driveFields);
        String filesInner = String.join(", ", ordered);
        return "nextPageToken, files(" + filesInner + ")";
    }

    private static String mapField(String logical) {
        return switch (logical) {
            case "title" -> "name";
            case "updatedAt" -> "modifiedTime";
            default -> logical;
        };
    }

    private static Instant requireInstant(Object value) {
        if (value instanceof Instant i) {
            return i;
        }
        throw new IllegalArgumentException("Expected Instant for date comparison, got " + value);
    }

    private static String escapeQuotes(String s) {
        return s.replace("'", "\\'");
    }
}
