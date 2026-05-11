package org.emathp.parser;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
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
 * Parses a small SQL subset into the engine's logical query model.
 *
 * <p>Supported single-source queries:
 * <ul>
 *   <li>{@code SELECT col1, col2, ...} (or {@code *})</li>
 *   <li>{@code FROM <anything>} (table name is currently ignored)</li>
 *   <li>{@code WHERE} a single binary comparison: {@code =}, {@code >}, {@code <}, {@code LIKE}</li>
 *   <li>{@code ORDER BY <single column> [ASC|DESC]}</li>
 *   <li>{@code LIMIT <integer>}</li>
 * </ul>
 *
 * <p>Supported JOIN queries (v1, see {@link JoinQuery}):
 * <ul>
 *   <li>{@code SELECT [* | qual.col, ...] FROM a x JOIN b y ON x.f = y.g [WHERE ...] [LIMIT n]}</li>
 *   <li>INNER only (plain {@code JOIN} or explicit {@code INNER JOIN}).</li>
 *   <li>Single equi-predicate ON two qualified columns, one per side.</li>
 *   <li>WHERE: a single comparison referencing exactly one side (e.g. {@code g.updatedAt > '...'}).
 *       Two-sided predicates and compound (AND/OR) are not yet supported - they need post-join
 *       qualified-field semantics. The single-side WHERE is routed to that side's per-side
 *       {@link Query} so it goes through normal predicate-pushdown.</li>
 *   <li>ORDER BY on a JOIN is not yet supported.</li>
 * </ul>
 *
 * <p>Anything else (GROUP BY, HAVING, OR/AND, multiple ORDER BY, subqueries, functions,
 * non-INNER joins, {@code g.*}, multi-JOIN) raises {@link IllegalArgumentException} with a
 * specific message. JSQLParser AST types never leak past this class.
 */
public final class SQLParserService {

    public ParsedQuery parse(String sql) {
        Statement statement;
        try {
            statement = CCJSqlParserUtil.parse(sql);
        } catch (JSQLParserException e) {
            throw new IllegalArgumentException("Failed to parse SQL: " + sql, e);
        }
        if (!(statement instanceof PlainSelect plain)) {
            throw new IllegalArgumentException(
                    "Only simple SELECT statements are supported (got: " + statement.getClass().getSimpleName() + ")");
        }

        rejectCommonUnsupported(plain);

        if (plain.getJoins() != null && !plain.getJoins().isEmpty()) {
            return parseJoinQuery(plain);
        }
        return parseSingleSource(plain);
    }

    // ---- Common rejections (apply to both single-source and JOIN paths) ----

    private void rejectCommonUnsupported(PlainSelect plain) {
        if (plain.getGroupBy() != null) {
            throw new IllegalArgumentException("GROUP BY is not supported");
        }
        if (plain.getHaving() != null) {
            throw new IllegalArgumentException("HAVING is not supported");
        }
        if (plain.getDistinct() != null) {
            throw new IllegalArgumentException("DISTINCT is not supported");
        }
        if (plain.getOffset() != null || plain.getFetch() != null) {
            throw new IllegalArgumentException("OFFSET / FETCH are not supported");
        }
    }

    // ---- Single-source path (unchanged behavior; callers see Query) ----

    private Query parseSingleSource(PlainSelect plain) {
        List<String> projection = parseProjection(plain.getSelectItems());
        ComparisonExpr where = plain.getWhere() == null ? null : parseWhere(plain.getWhere());
        List<OrderBy> orderBy = parseOrderBy(plain.getOrderByElements());
        Integer limit = parseLimit(plain.getLimit());
        return new Query(projection, where, orderBy, limit, null, null);
    }

    private List<String> parseProjection(List<SelectItem<?>> items) {
        if (items == null || items.isEmpty()) {
            return List.of();
        }
        List<String> cols = new ArrayList<>(items.size());
        for (SelectItem<?> item : items) {
            Expression expr = item.getExpression();
            if (expr instanceof AllColumns) {
                return List.of();
            }
            if (expr instanceof Column col) {
                cols.add(col.getColumnName());
            } else {
                throw new IllegalArgumentException(
                        "Only column references are supported in SELECT (got: " + expr + ")");
            }
        }
        return cols;
    }

    private ComparisonExpr parseWhere(Expression where) {
        if (where instanceof EqualsTo eq) {
            return binary(eq.getLeftExpression(), Operator.EQ, eq.getRightExpression());
        }
        if (where instanceof GreaterThan gt) {
            return binary(gt.getLeftExpression(), Operator.GT, gt.getRightExpression());
        }
        if (where instanceof MinorThan lt) {
            return binary(lt.getLeftExpression(), Operator.LT, lt.getRightExpression());
        }
        if (where instanceof LikeExpression like) {
            return binary(like.getLeftExpression(), Operator.LIKE, like.getRightExpression());
        }
        throw new IllegalArgumentException(
                "Only a single =/>/</LIKE comparison is supported in WHERE (got: " + where + ")");
    }

    private ComparisonExpr binary(Expression left, Operator op, Expression right) {
        if (!(left instanceof Column col)) {
            throw new IllegalArgumentException("Left side of comparison must be a column reference");
        }
        return new ComparisonExpr(col.getColumnName(), op, parseLiteral(right));
    }

    private Object parseLiteral(Expression expr) {
        if (expr instanceof StringValue sv) {
            String raw = sv.getValue();
            // NOTE: opportunistic date coercion — any quoted literal that parses as ISO-8601
            // (Instant or LocalDate) becomes an Instant; everything else stays a String. This
            // keeps `WHERE updatedAt > '2026-05-07'` working without a schema, but a real
            // string column whose value happens to look like an ISO date would be misread.
            // Schema-aware typing (resolved against the connector's supportedFields) is the
            // proper fix; deferred until we have one.
            Instant maybeDate = tryParseDate(raw);
            return maybeDate != null ? maybeDate : raw;
        }
        if (expr instanceof LongValue lv) {
            return lv.getValue();
        }
        if (expr instanceof DoubleValue dv) {
            return dv.getValue();
        }
        throw new IllegalArgumentException("Unsupported literal expression: " + expr);
    }

    private static Instant tryParseDate(String raw) {
        try {
            return Instant.parse(raw);
        } catch (Exception ignored) {
        }
        try {
            return LocalDate.parse(raw).atStartOfDay(ZoneOffset.UTC).toInstant();
        } catch (Exception ignored) {
        }
        return null;
    }

    private List<OrderBy> parseOrderBy(List<OrderByElement> elements) {
        if (elements == null || elements.isEmpty()) {
            return List.of();
        }
        if (elements.size() > 1) {
            throw new IllegalArgumentException("Only a single ORDER BY field is supported");
        }
        OrderByElement el = elements.get(0);
        if (!(el.getExpression() instanceof Column col)) {
            throw new IllegalArgumentException("ORDER BY must reference a column");
        }
        Direction direction = el.isAsc() ? Direction.ASC : Direction.DESC;
        return List.of(new OrderBy(col.getColumnName(), direction));
    }

    private Integer parseLimit(Limit limit) {
        if (limit == null) {
            return null;
        }
        Expression rowCount = limit.getRowCount();
        if (rowCount instanceof LongValue lv) {
            return Math.toIntExact(lv.getValue());
        }
        throw new IllegalArgumentException("LIMIT must be a numeric literal (got: " + rowCount + ")");
    }

    // ---- JOIN path ----

    private JoinQuery parseJoinQuery(PlainSelect plain) {
        if (plain.getOrderByElements() != null && !plain.getOrderByElements().isEmpty()) {
            throw new IllegalArgumentException(
                    "ORDER BY on a JOIN query is not supported in v1");
        }

        List<Join> joins = plain.getJoins();
        if (joins.size() > 1) {
            throw new IllegalArgumentException(
                    "Only a single JOIN is supported in v1 (got: " + joins.size() + ")");
        }
        Join join = joins.get(0);
        validateInnerJoin(join);

        JoinSide left = parseJoinSide(plain.getFromItem(), "FROM");
        JoinSide right = parseJoinSide(join.getRightItem(), "JOIN");
        if (left.alias().equals(right.alias())) {
            throw new IllegalArgumentException(
                    "Both JOIN sides resolve to alias '" + left.alias()
                            + "'; use distinct table aliases (AS x / AS y)");
        }
        Map<String, JoinSide> aliasToSide = new HashMap<>();
        aliasToSide.put(left.alias(), left);
        aliasToSide.put(right.alias(), right);

        JoinPredicate on = parseOnPredicate(join, left, right);
        JoinWhere where = plain.getWhere() == null ? null : parseJoinWhere(plain.getWhere(), aliasToSide);
        List<String> select = parseQualifiedSelect(plain.getSelectItems(), aliasToSide);
        Integer limit = parseLimit(plain.getLimit());

        return new JoinQuery(left, right, on, where, select, limit, null, null);
    }

    /**
     * Parses a JOIN-level WHERE into a {@link JoinWhere}. v1 supports only a single comparison
     * referencing exactly one side (compound and cross-side predicates are rejected). The
     * column qualifier identifies the target side; the stored {@link ComparisonExpr#field()}
     * is the unqualified column name so the per-side {@link Query} sees a normal predicate.
     */
    private JoinWhere parseJoinWhere(Expression where, Map<String, JoinSide> aliasToSide) {
        ComparisonExpr predicate = parseWhere(where);
        Column col = extractWhereColumn(where);
        String qual = qualifierOrThrow(col, "WHERE column");
        if (!aliasToSide.containsKey(qual)) {
            throw new IllegalArgumentException(
                    "WHERE qualifier '" + qual + "' does not match any joined side ('"
                            + String.join("', '", aliasToSide.keySet()) + "')");
        }
        return new JoinWhere(qual, predicate);
    }

    /**
     * Re-extracts the column from a WHERE expression so we can read its qualifier. The shapes
     * accepted here mirror {@link #parseWhere(Expression)}; if {@code parseWhere} accepted the
     * expression as a binary comparison, its left side is a {@link Column}.
     */
    private static Column extractWhereColumn(Expression where) {
        Expression left;
        if (where instanceof EqualsTo eq) {
            left = eq.getLeftExpression();
        } else if (where instanceof GreaterThan gt) {
            left = gt.getLeftExpression();
        } else if (where instanceof MinorThan lt) {
            left = lt.getLeftExpression();
        } else if (where instanceof LikeExpression like) {
            left = like.getLeftExpression();
        } else {
            throw new IllegalArgumentException(
                    "WHERE on a JOIN must be a single =/>/</LIKE comparison (got: " + where + ")");
        }
        if (!(left instanceof Column col)) {
            throw new IllegalArgumentException("WHERE left side must be a column reference");
        }
        return col;
    }

    /**
     * INNER JOIN only in v1. JSQLParser exposes flags for each variant; the simple/inner case
     * is the absence of left/right/full/cross/outer/natural modifiers.
     */
    private void validateInnerJoin(Join join) {
        if (join.isLeft() || join.isRight() || join.isFull() || join.isOuter()) {
            throw new IllegalArgumentException(
                    "Only INNER JOIN is supported in v1 (got: "
                            + describeJoin(join) + ")");
        }
        if (join.isCross()) {
            throw new IllegalArgumentException("CROSS JOIN is not supported in v1");
        }
        if (join.isNatural()) {
            throw new IllegalArgumentException(
                    "NATURAL JOIN is not supported (ambiguous without an explicit ON predicate)");
        }
        if (join.isApply() || join.isSemi()) {
            throw new IllegalArgumentException(
                    "Only INNER JOIN is supported in v1 (got: " + describeJoin(join) + ")");
        }
    }

    private static String describeJoin(Join join) {
        StringBuilder sb = new StringBuilder();
        if (join.isLeft()) {
            sb.append("LEFT ");
        }
        if (join.isRight()) {
            sb.append("RIGHT ");
        }
        if (join.isFull()) {
            sb.append("FULL ");
        }
        if (join.isOuter()) {
            sb.append("OUTER ");
        }
        if (join.isCross()) {
            sb.append("CROSS ");
        }
        if (join.isNatural()) {
            sb.append("NATURAL ");
        }
        sb.append("JOIN");
        return sb.toString().trim();
    }

    /**
     * Builds a {@link JoinSide} from a FROM/JOIN item. Requires a plain table (subqueries and
     * lateral derived tables are not supported). The {@code alias} field of the returned
     * {@code JoinSide} is the SQL alias if one was written (e.g. {@code FROM google g}), or the
     * table name otherwise (e.g. {@code FROM google}). This is the qualifier the user must use
     * when referencing columns from this side.
     */
    private JoinSide parseJoinSide(FromItem item, String position) {
        if (!(item instanceof Table table)) {
            throw new IllegalArgumentException(
                    position + " must reference a single table (subqueries/derived tables not supported)");
        }
        String connectorName = table.getName();
        if (connectorName == null || connectorName.isBlank()) {
            throw new IllegalArgumentException(position + " is missing a table name");
        }
        String alias = (table.getAlias() != null && table.getAlias().getName() != null)
                ? table.getAlias().getName()
                : connectorName;
        return new JoinSide(connectorName, alias);
    }

    /**
     * Parses the ON predicate and canonicalizes orientation so the returned
     * {@link JoinPredicate}'s {@code leftAlias} always matches {@code left.alias()}.
     *
     * <p>Resolution rule (the column-to-side mapping):
     * <ol>
     *   <li>The ON expression must be a single equality between two {@link Column}s.</li>
     *   <li>Each {@code Column}'s qualifier ({@code col.getTable().getName()}) must be present
     *       and must match either the left side's or the right side's alias.</li>
     *   <li>The two columns must reference different sides — otherwise the join collapses.</li>
     *   <li>If the user wrote the equality with the right side first
     *       ({@code ON n.title = g.title}), the parser swaps the column order so the stored
     *       predicate is canonical. The hash-join executor can then build the right side's hash
     *       table without re-checking orientation.</li>
     * </ol>
     */
    private JoinPredicate parseOnPredicate(Join join, JoinSide left, JoinSide right) {
        Collection<Expression> onExprs = join.getOnExpressions();
        if (onExprs == null || onExprs.isEmpty()) {
            throw new IllegalArgumentException("JOIN must have an ON predicate");
        }
        if (onExprs.size() > 1) {
            throw new IllegalArgumentException(
                    "JOIN must have a single equi-predicate (got: " + onExprs.size() + ")");
        }
        Expression onExpr = onExprs.iterator().next();
        if (!(onExpr instanceof EqualsTo eq)) {
            throw new IllegalArgumentException(
                    "ON must be an equality between two columns (got: " + onExpr + ")");
        }

        Column leftCol = requireColumn(eq.getLeftExpression(), "ON left side");
        Column rightCol = requireColumn(eq.getRightExpression(), "ON right side");
        String leftQual = qualifierOrThrow(leftCol, "ON left column");
        String rightQual = qualifierOrThrow(rightCol, "ON right column");

        boolean leftIsLeft = leftQual.equals(left.alias()) && rightQual.equals(right.alias());
        boolean leftIsRight = leftQual.equals(right.alias()) && rightQual.equals(left.alias());

        if (leftIsLeft) {
            return new JoinPredicate(
                    left.alias(), leftCol.getColumnName(),
                    right.alias(), rightCol.getColumnName());
        }
        if (leftIsRight) {
            // user wrote ON <right>.x = <left>.y; canonicalize.
            return new JoinPredicate(
                    left.alias(), rightCol.getColumnName(),
                    right.alias(), leftCol.getColumnName());
        }
        throw new IllegalArgumentException(
                "ON must reference both joined sides exactly once (saw qualifiers '"
                        + leftQual + "' and '" + rightQual
                        + "', expected one of '" + left.alias() + "' / '" + right.alias() + "')");
    }

    private List<String> parseQualifiedSelect(
            List<SelectItem<?>> items, Map<String, JoinSide> aliasToSide) {
        if (items == null || items.isEmpty()) {
            return List.of();
        }
        List<String> cols = new ArrayList<>(items.size());
        for (SelectItem<?> item : items) {
            Expression expr = item.getExpression();
            if (expr instanceof AllColumns) {
                if (items.size() != 1) {
                    throw new IllegalArgumentException(
                            "SELECT * cannot be combined with explicit columns in a JOIN query");
                }
                return List.of();
            }
            if (!(expr instanceof Column col)) {
                throw new IllegalArgumentException(
                        "JOIN query SELECT items must be qualified column references "
                                + "(got: " + expr + ")");
            }
            String qual = qualifierOrThrow(col, "SELECT item");
            if (!aliasToSide.containsKey(qual)) {
                throw new IllegalArgumentException(
                        "SELECT item '" + qual + "." + col.getColumnName()
                                + "' references unknown alias '" + qual + "'");
            }
            cols.add(qual + "." + col.getColumnName());
        }
        return cols;
    }

    private static Column requireColumn(Expression expr, String context) {
        if (!(expr instanceof Column col)) {
            throw new IllegalArgumentException(
                    context + " must be a column reference (got: " + expr + ")");
        }
        return col;
    }

    private static String qualifierOrThrow(Column col, String context) {
        Table table = col.getTable();
        String qual = table == null ? null : table.getName();
        if (qual == null || qual.isBlank()) {
            throw new IllegalArgumentException(
                    context + " '" + col.getColumnName() + "' must be qualified with a table alias "
                            + "(e.g. 'g." + col.getColumnName() + "')");
        }
        return qual;
    }
}
