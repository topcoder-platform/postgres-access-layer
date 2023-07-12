package com.topcoder.pal.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.lang.Nullable;

import com.topcoder.dal.rdb.Row;
import com.topcoder.dal.rdb.TypedColumn;

public class TypedResultSetExtractor implements ResultSetExtractor<List<Row>> {
    private final GrpcRowMapper rowMapper;
    private final int rowsExpected;

    public TypedResultSetExtractor() {
        this(null, 0);
    }

    public TypedResultSetExtractor(int rowsExpected) {
        this(null, rowsExpected);
    }

    public TypedResultSetExtractor(@Nullable List<TypedColumn> columnList) {
        this(columnList, 0);
    }

    public TypedResultSetExtractor(@Nullable List<TypedColumn> columnList, int rowsExpected) {
        this.rowsExpected = rowsExpected;
        this.rowMapper = new GrpcRowMapper(columnList, null);
    }

    public List<Row> extractData(ResultSet rs) throws SQLException {
        List<Row> results = this.rowsExpected > 0 ? new ArrayList<Row>(this.rowsExpected) : new ArrayList<Row>();
        int rowNum = 0;

        while (rs.next()) {
            results.add(this.rowMapper.mapRow(rs, rowNum++));
        }

        return results;
    }
}