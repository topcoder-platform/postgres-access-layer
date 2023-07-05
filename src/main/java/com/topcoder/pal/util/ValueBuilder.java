package com.topcoder.pal.util;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import com.topcoder.dal.rdb.Value;

public class ValueBuilder {
    private final Value.Builder builder;
    private final ResultSet rs;
    private int columnNumber = 0;

    public ValueBuilder(Value.Builder builder, ResultSet rs) {
        this.builder = builder;
        this.rs = rs;
        columnNumber++;
    }

    public void setIntValue() throws SQLException {
        int value = rs.getInt(columnNumber++);
        if (!rs.wasNull()) {
            builder.setIntValue(value);
        }
    }

    public void setLongValue() throws SQLException {
        long value = rs.getLong(columnNumber++);
        if (!rs.wasNull()) {
            builder.setLongValue(value);
        }
    }

    public void setFloatValue() throws SQLException {
        float value = rs.getFloat(columnNumber++);
        if (!rs.wasNull()) {
            builder.setFloatValue(value);
        }
    }

    public void setDoubleValue() throws SQLException {
        double value = rs.getDouble(columnNumber++);
        if (!rs.wasNull()) {
            builder.setDoubleValue(value);
        }
    }

    public void setStringValue() throws SQLException {
        String value = rs.getString(columnNumber++);
        if (!rs.wasNull()) {
            builder.setStringValue(value);
        }
    }

    public void setBooleanValue() throws SQLException {
        boolean value = rs.getBoolean(columnNumber++);
        if (!rs.wasNull()) {
            builder.setBooleanValue(value);
        }
    }

    public void setDateValue() throws SQLException {
        Timestamp value = rs.getTimestamp(columnNumber++);
        if (!rs.wasNull()) {
            builder.setDateValue(value.toString());
        }
    }

    public void setDecimalValue() throws SQLException {
        BigDecimal value = rs.getBigDecimal(columnNumber++);
        if (!rs.wasNull()) {
            builder.setStringValue(value.toString());
        }
    }
}
