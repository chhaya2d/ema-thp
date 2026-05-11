package org.emathp.connector.google.mock;

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
 * Maps unified {@link org.emathp.model.ConnectorQuery} to mock-DSL Google Drive–style request
 * parameters (same vocabulary the {@link MockGoogleDriveApi} regex layer understands).
 */
public final class GoogleQueryTranslator {

    private static final DateTimeFormatter INSTANT_FORMAT = DateTimeFormatter.ISO_INSTANT;

    public GoogleSearchRequest translate(org.emathp.model.ConnectorQuery query) {
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
        if ("updatedAt".equals(providerField)) {
            Instant instant = requireInstant(where.value());
            String iso = INSTANT_FORMAT.format(instant);
            return "updatedAt = '" + iso + "'";
        }
        if ("name".equals(providerField)) {
            String literal = escapeQuotes(String.valueOf(where.value()));
            return "name = '" + literal + "'";
        }
        throw new IllegalArgumentException("Unsupported EQ field: " + where.field());
    }

    private String buildInstantCompare(ComparisonExpr where) {
        String providerField = mapField(where.field());
        if (!"updatedAt".equals(providerField)) {
            throw new IllegalArgumentException("GT/LT only supported on updatedAt for Google translator");
        }
        Instant instant = requireInstant(where.value());
        String iso = INSTANT_FORMAT.format(instant);
        String op = where.operator() == Operator.GT ? ">" : "<";
        return "updatedAt " + op + " '" + iso + "'";
    }

    private String buildLike(ComparisonExpr where) {
        String providerField = mapField(where.field());
        if (!"name".equals(providerField)) {
            throw new IllegalArgumentException("LIKE only supported on title for Google translator");
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
                    case "updatedAt" -> "updatedAt";
                    case "name" -> "name";
                    default -> "name";
                };
        String dir = ob.direction() == Direction.DESC ? "desc" : "asc";
        return apiField + " " + dir;
    }

    /**
     * Builds a Google partial-response style {@code fields} mask for {@code files.list}.
     */
    private String buildFieldsMask(List<String> projection) {
        Set<String> driveFields = new LinkedHashSet<>();
        driveFields.add("id");
        for (String logical : projection) {
            driveFields.add(mapField(logical));
        }
        driveFields.add("webViewLink");

        List<String> ordered = new ArrayList<>(driveFields);
        String filesInner = String.join(", ", ordered);
        return "nextPageToken, files(" + filesInner + ")";
    }

    private static String mapField(String logical) {
        return switch (logical) {
            case "title" -> "name";
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
