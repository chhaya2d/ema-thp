package org.emathp.connector.notion.mock;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.emathp.connector.notion.api.NotionSearchRequest;
import org.emathp.model.ComparisonExpr;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.Operator;

/**
 * Maps a unified {@link ConnectorQuery} to Notion-native search parameters.
 * Notion only supports filtering, so {@code orderBy} and {@code projection} are ignored.
 */
public final class NotionQueryTranslator {

    private static final DateTimeFormatter INSTANT_FORMAT = DateTimeFormatter.ISO_INSTANT;

    public NotionSearchRequest translate(ConnectorQuery query) {
        return new NotionSearchRequest(buildFilter(query.where()), query.cursor(), query.pageSize());
    }

    private String buildFilter(ComparisonExpr where) {
        if (where == null) {
            return "";
        }
        String providerField = mapField(where.field());
        return switch (where.operator()) {
            case EQ -> buildEq(providerField, where.value());
            case GT, LT -> buildInstantCompare(providerField, where);
            case LIKE -> buildLike(providerField, where.value());
        };
    }

    private String buildEq(String field, Object value) {
        if ("lastEditedTime".equals(field)) {
            String iso = INSTANT_FORMAT.format(requireInstant(value));
            return "lastEditedTime = '" + iso + "'";
        }
        if ("title".equals(field)) {
            return "title = '" + escape(String.valueOf(value)) + "'";
        }
        throw new IllegalArgumentException("Unsupported EQ field: " + field);
    }

    private String buildInstantCompare(String field, ComparisonExpr where) {
        if (!"lastEditedTime".equals(field)) {
            throw new IllegalArgumentException("GT/LT only supported on updatedAt for Notion translator");
        }
        String iso = INSTANT_FORMAT.format(requireInstant(where.value()));
        String op = where.operator() == Operator.GT ? ">" : "<";
        return "lastEditedTime " + op + " '" + iso + "'";
    }

    private String buildLike(String field, Object value) {
        if (!"title".equals(field)) {
            throw new IllegalArgumentException("LIKE only supported on title for Notion translator");
        }
        String pattern = String.valueOf(value);
        if (pattern.startsWith("%")) {
            pattern = pattern.substring(1);
        }
        if (pattern.endsWith("%")) {
            pattern = pattern.substring(0, pattern.length() - 1);
        }
        return "title contains '" + escape(pattern) + "'";
    }

    private static String mapField(String logical) {
        return switch (logical) {
            case "title" -> "title";
            case "updatedAt" -> "lastEditedTime";
            default -> logical;
        };
    }

    private static String escape(String s) {
        return s.replace("'", "\\'");
    }

    private static Instant requireInstant(Object value) {
        if (value instanceof Instant i) {
            return i;
        }
        throw new IllegalArgumentException("Expected Instant for date comparison, got " + value);
    }
}
