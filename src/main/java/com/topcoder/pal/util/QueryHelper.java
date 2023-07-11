package com.topcoder.pal.util;

import com.topcoder.dal.rdb.*;
import com.topcoder.dal.rdb.JoinCondition.RightCase;
import com.topcoder.dal.rdb.Value.ValueCase;

import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

@Component
public class QueryHelper {

    public ParameterizedExpression getSelectQuery(SelectQuery query) {
        final String tableName = buildName(query);

        List<TypedColumn> columnsList = query.getColumnList();

        final String[] columns = columnsList.stream().map(this::buildName).toArray(String[]::new);

        final List<ParameterizedExpression> whereClause = query.getWhereList().stream()
                .map(this::toWhereCriteria).toList();

        final String[] groupByClause = query.getGroupByList().stream().map(this::buildName).toArray(String[]::new);
        final String[] orderByClause = query.getOrderByList().stream().map(this::buildName).toArray(String[]::new);

        final int limit = query.getLimit();
        final int offset = query.getOffset();

        ParameterizedExpression expression = new ParameterizedExpression();
        expression.setExpression("SELECT"
                + (" " + String.join(",", columns) + " FROM " + tableName)
                + (query.getJoinCount() > 0
                        ? " " + String.join(" ",
                                query.getJoinList().stream().map(toJoin).toArray(String[]::new))
                        : "")
                + (!whereClause.isEmpty()
                        ? " WHERE " + String.join(" AND ",
                                whereClause.stream().map(
                                        ParameterizedExpression::getExpression)
                                        .toArray(String[]::new))
                        : "")
                + (groupByClause.length > 0 ? " GROUP BY " + String.join(",", groupByClause) : "")
                + (orderByClause.length > 0 ? " ORDER BY " + String.join(",", orderByClause) : "")
                + (limit > 0 ? " LIMIT " + limit : "")
                + (offset > 0 ? " OFFSET " + offset : ""));
        if (!whereClause.isEmpty()) {
            expression.setParameter(
                    whereClause.stream().filter(x -> x.getParameter().length > 0)
                            .flatMap(x -> Arrays.stream(x.getParameter()))
                            .toArray());
        }
        return expression;
    }

    public ParameterizedExpression getInsertQuery(InsertQuery query) {
        final String tableName = buildName(query);
        final List<ColumnValue> valuesToInsert = query.getColumnValueList();

        final String[] columns = valuesToInsert.stream().map(ColumnValue::getColumn).toArray(String[]::new);

        final Object[] params = valuesToInsert.stream().map(ColumnValue::getValue)
                .filter(x -> findSQLExpressionOrFunction(x).isEmpty()).map(QueryHelper::toValue).toArray();

        final String[] values = valuesToInsert.stream().map(ColumnValue::getValue)
                .map(x -> findSQLExpressionOrFunction(x).orElse("?")).toArray(String[]::new);

        ParameterizedExpression expression = new ParameterizedExpression();
        expression.setExpression("INSERT INTO " + tableName + " (" + String.join(",", columns) + ") VALUES ("
                + String.join(",", values) + ")");
        expression.setParameter(params);
        return expression;
    }

    public ParameterizedExpression getUpdateQuery(UpdateQuery query) {
        final String tableName = buildName(query);

        final List<ColumnValue> valuesToUpdate = query.getColumnValueList();
        final String[] columns = valuesToUpdate.stream().map(ColumnValue::getColumn).toArray(String[]::new);

        final String[] values = valuesToUpdate.stream().map(ColumnValue::getValue)
                .map(x -> findSQLExpressionOrFunction(x).orElse("?"))
                .toArray(String[]::new);

        final Stream<Object> paramsStream = valuesToUpdate.stream()
                .map(ColumnValue::getValue)
                .filter(x -> findSQLExpressionOrFunction(x).isEmpty())
                .map(QueryHelper::toValue);

        final List<ParameterizedExpression> whereClause = query.getWhereList().stream()
                .map(this::toWhereCriteria).toList();

        if (whereClause.isEmpty()) {
            throw new RuntimeException("Update query must have a where clause");
        }
        final Object[] params = Stream
                .concat(paramsStream,
                        whereClause.stream().filter(x -> x.getParameter().length > 0)
                                .flatMap(x -> Arrays.stream(x.getParameter())))
                .toArray();

        ParameterizedExpression expression = new ParameterizedExpression();
        expression.setExpression("UPDATE "
                + tableName
                + " SET " + String.join(",", zip(columns, values, (c, v) -> c + "=" + v))
                + " WHERE "
                + String.join(" AND ",
                        whereClause.stream().map(ParameterizedExpression::getExpression)
                                .toArray(String[]::new)));
        expression.setParameter(params);
        return expression;
    }

    public ParameterizedExpression getDeleteQuery(DeleteQuery query) {
        final String tableName = buildName(query);

        final List<ParameterizedExpression> whereClause = query.getWhereList().stream()
                .map(this::toWhereCriteria).toList();

        if (whereClause.isEmpty()) {
            throw new IllegalArgumentException("Delete query must have a where clause");
        }
        ParameterizedExpression expression = new ParameterizedExpression();
        expression.setExpression("DELETE FROM "
                + tableName
                + " WHERE "
                + String.join(" AND ",
                        whereClause.stream().map(ParameterizedExpression::getExpression)
                                .toArray(String[]::new)));
        expression.setParameter(
                whereClause.stream().filter(x -> x.getParameter().length > 0)
                        .flatMap(x -> Arrays.stream(x.getParameter())).toArray());
        return expression;
    }

    public String getRawQuery(RawQuery query) {
        return sanitizeSQLStatement(query.getQuery());
    }

    public static String sanitizeSQLStatement(String sql) {
        if (sql == null || sql.trim().length() == 0) {
            throw new IllegalArgumentException("SQL statement is null or empty");
        }

        // Limit the length of the SQL statement to prevent very long strings
        if (sql.length() > 1000) {
            throw new IllegalArgumentException("SQL statement length exceeds the allowed limit");
        }

        // Whitelist characters
        StringBuilder safeSQL = new StringBuilder();
        for (char c : sql.toCharArray()) {
            if (Character.isLetterOrDigit(c) || c == ' ' || c == ',' || c == '(' || c == ')' || c == '='
                    || c == '<'
                    || c == '>' || c == '_' || c == ':' || c == '.' || c == '-' || c == '+'
                    || c == '*' || c == '\'') {
                safeSQL.append(c);
            }
        }
        sql = safeSQL.toString();

        // replace single quotes with two single quotes to prevent SQL injection through
        // strings
        sql = sql.replace("'", "''");

        return sql;
    }

    private ParameterizedExpression toWhereCriteria(WhereCriteria criteria) {
        ParameterizedExpression pe;
        switch (criteria.getWhereTypeCase()) {
            case CONDITION -> pe = toWhereCriteria(criteria.getCondition());
            case AND -> {
                ParameterizedExpression expression = new ParameterizedExpression();
                List<ParameterizedExpression> list = criteria.getAnd().getWhereList().stream()
                        .map(this::toWhereCriteria).toList();
                expression.setExpression("(" + String.join(" AND ",
                        list.stream().map(ParameterizedExpression::getExpression)
                                .toArray(String[]::new))
                        + ")");
                expression.setParameter(
                        list.stream().filter(x -> x.parameter.length > 0)
                                .flatMap(x -> Arrays.stream(x.getParameter()))
                                .toArray());
                pe = expression;
            }
            case OR -> {
                ParameterizedExpression expression = new ParameterizedExpression();
                List<ParameterizedExpression> list = criteria.getOr().getWhereList().stream()
                        .map(this::toWhereCriteria).toList();
                expression.setExpression("(" + String.join(" OR ",
                        list.stream().map(ParameterizedExpression::getExpression)
                                .toArray(String[]::new))
                        + ")");
                expression.setParameter(
                        list.stream().filter(x -> x.parameter.length > 0)
                                .flatMap(x -> Arrays.stream(x.getParameter()))
                                .toArray());
                pe = expression;
            }
            case WHERETYPE_NOT_SET ->
                throw new UnsupportedOperationException(
                        "Unimplemented case: " + criteria.getWhereTypeCase());
            default ->
                throw new IllegalArgumentException("Unexpected value: " + criteria.getWhereTypeCase());
        }
        return pe;
    }

    private final Function<Join, String> toJoin = (join) -> {
        if (join.getConditionsCount() == 0) {
            throw new IllegalArgumentException("At least 1 join condition is required");
        }
        return new StringBuilder(getJoinType(join.getType())).append(" ").append(buildName(join.getTable()))
                .append(" ON ")
                .append(String.join(" AND ",
                        join.getConditionsList().stream().map(this::buildJoinCondition).toArray(String[]::new)))
                .toString();
    };

    private String buildJoinCondition(JoinCondition condition) {
        return new StringBuilder(buildName(condition.getLeft())).append(getOperator(condition.getOperator()))
                .append(condition.getRightCase().equals(RightCase.COLUMN) ? buildName(condition.getColumn())
                        : "'" + toValue(condition.getValue()).toString() + "'")
                .toString();
    }

    private static String getJoinType(JoinType joinType) {
        return switch (joinType) {
            case JOIN_TYPE_INNER -> "INNER JOIN";
            case JOIN_TYPE_LEFT -> "LEFT JOIN";
            case JOIN_TYPE_RIGHT -> "RIGHT JOIN";
            case JOIN_TYPE_FULL -> "FULL JOIN";
            default -> "JOIN";
        };
    }

    private String getOperator(Operator operator) {
        return switch (operator) {
            case OPERATOR_EQUAL -> "=";
            case OPERATOR_NOT_EQUAL -> "<>";
            case OPERATOR_GREATER_THAN -> ">";
            case OPERATOR_GREATER_THAN_OR_EQUAL -> ">=";
            case OPERATOR_LESS_THAN -> "<";
            case OPERATOR_LESS_THAN_OR_EQUAL -> "<=";
            case OPERATOR_LIKE -> "LIKE";
            case OPERATOR_NOT_LIKE -> "NOT LIKE";
            case OPERATOR_IN -> "IN";
            case OPERATOR_NOT_IN -> "IN";
            case OPERATOR_IS_NULL -> "IS NULL";
            case OPERATOR_IS_NOT_NULL -> "IS NOT NULL";
            default -> null;
        };
    }

    private ParameterizedExpression toWhereCriteria(Condition criteria) {
        String key = buildName(criteria.getKey());
        Object value = toValue(criteria.getValue());
        ParameterizedExpression parameterizedExpression = new ParameterizedExpression();

        String clause = switch (criteria.getOperator()) {
            case OPERATOR_EQUAL -> key + " = ?";
            case OPERATOR_NOT_EQUAL -> key + " <> ?";
            case OPERATOR_GREATER_THAN -> key + " > ?";
            case OPERATOR_GREATER_THAN_OR_EQUAL -> key + " >= ?";
            case OPERATOR_LESS_THAN -> key + " < ?";
            case OPERATOR_LESS_THAN_OR_EQUAL -> key + " <= ?";
            case OPERATOR_LIKE -> key + " LIKE ?";
            case OPERATOR_NOT_LIKE -> key + " NOT LIKE ?";
            case OPERATOR_IN -> key + " IN (?)";
            case OPERATOR_NOT_IN -> key + " NOT IN (?)";
            case OPERATOR_IS_NULL -> key + " IS NULL";
            case OPERATOR_IS_NOT_NULL -> key + " IS NOT NULL";
            default -> null;
        };
        Optional<String> foundExpressionOrFunction = findSQLExpressionOrFunction(criteria.getValue());

        if (!criteria.getOperator().equals(Operator.OPERATOR_IS_NULL)
                && !criteria.getOperator().equals(Operator.OPERATOR_IS_NOT_NULL)
                && foundExpressionOrFunction.isPresent()) {
            clause = Objects.requireNonNull(clause).replace("?", foundExpressionOrFunction.get());
        } else if (value != null) {
            parameterizedExpression.setParameter(new Object[] { value });
        }

        parameterizedExpression.setExpression(clause);

        return parameterizedExpression;
    };

    private static Object toValue(Value value) {
        return switch (value.getValueCase()) {
            case STRING_VALUE -> value.getStringValue();
            case INT_VALUE -> value.getIntValue();
            case LONG_VALUE -> value.getLongValue();
            case DOUBLE_VALUE -> value.getDoubleValue();
            case FLOAT_VALUE -> value.getFloatValue();
            case BOOLEAN_VALUE -> value.getBooleanValue();
            case DATE_VALUE -> value.getDateValue();
            case DATETIME_VALUE -> value.getDatetimeValue();
            case BLOB_VALUE, VALUE_NOT_SET -> null;
        };
    }

    private static Optional<String> findSQLExpressionOrFunction(Value value) {
        List<String> sqlExpressionsAndFunctions = Arrays.asList(
                "CURRENT", "EXTEND", "DATE", "TODAY", "MDY", "YEAR", "MONTH",
                "DAY", "HOUR", "MINUTE", "SECOND");

        if (value.getValueCase().equals(ValueCase.DATE_VALUE)
                || value.getValueCase().equals(ValueCase.DATETIME_VALUE)) {
            String valueStr = value.getValueCase().equals(ValueCase.DATE_VALUE)
                    ? value.getDateValue()
                    : value.getDatetimeValue();

            if (sqlExpressionsAndFunctions.stream().anyMatch(valueStr::contains)) {
                return Optional.of(valueStr);
            }
        }

        return Optional.empty();
    }

    private static String[] zip(String[] columns, String[] values, BiFunction<String, String, String> f) {
        final int length = columns.length;
        final String[] result = new String[length];

        for (int i = 0; i < length; i++) {
            result[i] = f.apply(columns[i], values[i]);
        }

        return result;
    }

    private String buildName(SelectQuery query) {
        return buildName(query.getSchema(), query.getTable());
    }

    private String buildName(InsertQuery query) {
        return buildName(query.getSchema(), query.getTable());
    }

    private String buildName(UpdateQuery query) {
        return buildName(query.getSchema(), query.getTable());
    }

    private String buildName(DeleteQuery query) {
        return buildName(query.getSchema(), query.getTable());
    }

    private String buildName(Table table) {
        return buildName(table.getSchema(), table.getTableName());
    }

    private String buildName(Column column) {
        return buildName(column.getSchema(), column.getTableName(), column.getName());
    }

    private String buildName(TypedColumn column) {
        return buildName(column.getSchema(), column.getTableName(), column.getName());
    }

    private String buildName(String schema, String table) {
        return new StringBuilder().append("\"").append(schema).append("\".\"").append(table).append("\"").toString();
    }

    private String buildName(String schema, String table, String column) {
        return new StringBuilder().append("\"").append(schema).append("\".\"").append(table).append("\".\"")
                .append(column).append("\"").toString();
    }
}
