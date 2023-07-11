package com.topcoder.pal.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.lang.Nullable;

import com.topcoder.dal.rdb.Row;
import com.topcoder.dal.rdb.TypedColumn;
import com.topcoder.dal.rdb.Value;

public class GrpcRowMapper implements RowMapper<Row> {
    private final List<TypedColumn> columnList;
    private final boolean columnListProvided;

    public GrpcRowMapper() {
        this(null);
    }

    public GrpcRowMapper(@Nullable List<TypedColumn> columnList) {
        this.columnList = columnList;
        this.columnListProvided = columnList != null;
    }

    public Row mapRow(ResultSet rs) throws SQLException {
        return mapRow(rs, 0);
    }

    public Row mapRow(ResultSet rs, int rowNum) throws SQLException {
        if (columnListProvided) {
            return buildRowFromColumnMap(rs);
        } else {
            return buildRowFromMetadata(rs);
        }
    }

    private Row buildRowFromColumnMap(ResultSet rs) throws SQLException {
        Row.Builder rowBuilder = Row.newBuilder();
        Value.Builder valueBuilder = Value.newBuilder();
        ValueBuilder builder = new ValueBuilder(valueBuilder, rs);
        for (int i = 0; i < columnList.size(); i++) {
            switch (columnList.get(i).getType()) {
                case COLUMN_TYPE_INT -> builder.setIntValue();
                case COLUMN_TYPE_LONG -> builder.setLongValue();
                case COLUMN_TYPE_FLOAT -> builder.setFloatValue();
                case COLUMN_TYPE_DOUBLE -> builder.setDoubleValue();
                case COLUMN_TYPE_STRING -> builder.setStringValue();
                case COLUMN_TYPE_BOOLEAN -> builder.setBooleanValue();
                case COLUMN_TYPE_DATE, COLUMN_TYPE_DATETIME -> builder.setDateValue();
                default -> throw new IllegalArgumentException(
                        "Unsupported column type: " + i + ": " + columnList.get(i).getType());
            }
            rowBuilder.putValues(columnList.get(i).getName(), valueBuilder.build());
        }
        return rowBuilder.build();
    }

    private Row buildRowFromMetadata(ResultSet rs) throws SQLException {
        Row.Builder rowBuilder = Row.newBuilder();
        Value.Builder valueBuilder = Value.newBuilder();
        ValueBuilder builder = new ValueBuilder(valueBuilder, rs);
        for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
            switch (rs.getMetaData().getColumnType(i + 1)) {
                case java.sql.Types.BIT -> builder.setBooleanValue();
                case java.sql.Types.TINYINT -> builder.setIntValue();
                case java.sql.Types.SMALLINT -> builder.setIntValue();
                case java.sql.Types.DECIMAL -> builder.setDecimalValue();
                case java.sql.Types.INTEGER -> builder.setIntValue();
                case java.sql.Types.BIGINT -> builder.setLongValue();
                case java.sql.Types.FLOAT -> builder.setFloatValue();
                case java.sql.Types.DOUBLE -> builder.setDoubleValue();
                case java.sql.Types.NUMERIC -> builder.setDecimalValue();
                case java.sql.Types.CHAR -> builder.setStringValue();
                case java.sql.Types.VARCHAR -> builder.setStringValue();
                case java.sql.Types.LONGNVARCHAR -> builder.setStringValue();
                case java.sql.Types.BOOLEAN -> builder.setBooleanValue();
                case java.sql.Types.DATE, java.sql.Types.TIMESTAMP, java.sql.Types.TIME -> builder.setDateValue();
                default -> throw new IllegalArgumentException(
                        "Unsupported column type: " + rs.getMetaData().getColumnType(i + 1));
            }
            rowBuilder.putValues(JdbcUtils.lookupColumnName(rs.getMetaData(), i + 1), valueBuilder.build());
        }
        return rowBuilder.build();
    }
}
