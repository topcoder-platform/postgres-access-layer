package com.topcoder.pal.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.*;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Map;

public class StreamJdbcTemplate extends JdbcTemplate {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public StreamJdbcTemplate(DataSource dataSource) {
        super(dataSource);
    }

    public <T> List<T> query(String sql, RowMapper<T> rowMapper, Connection con) throws DataAccessException {
        return result(query(sql, new RowMapperResultSetExtractor<>(rowMapper), con));
    }

    public <T> List<T> query(String sql, RowMapper<T> rowMapper, Connection con, @Nullable Object... args)
            throws DataAccessException {
        return result(query(sql, args, new RowMapperResultSetExtractor<>(rowMapper), con));
    }

    @Nullable
    public <T> T query(String sql, @Nullable Object[] args, ResultSetExtractor<T> rse, Connection con)
            throws DataAccessException {
        return query(sql, newArgPreparedStatementSetter(args), rse, con);
    }

    @Nullable
    public <T> T query(String sql, @Nullable PreparedStatementSetter pss, ResultSetExtractor<T> rse, Connection con)
            throws DataAccessException {
        return query(new SimplePreparedStatementCreator(sql), pss, rse, con);
    }

    @Nullable
    public <T> T query(
            PreparedStatementCreator psc, @Nullable final PreparedStatementSetter pss, final ResultSetExtractor<T> rse,
            Connection con)
            throws DataAccessException {

        Assert.notNull(rse, "ResultSetExtractor must not be null");

        return execute(psc, new PreparedStatementCallback<T>() {
            @Override
            @Nullable
            public T doInPreparedStatement(PreparedStatement ps) throws SQLException {
                ResultSet rs = null;
                try {
                    if (pss != null) {
                        pss.setValues(ps);
                    }
                    rs = ps.executeQuery();
                    return rse.extractData(rs);
                } finally {
                    closeResultSet(rs);
                    if (pss instanceof ParameterDisposer) {
                        ((ParameterDisposer) pss).cleanupParameters();
                    }
                }
            }
        }, con);
    }

    @Nullable
    public <T> T query(final String sql, final ResultSetExtractor<T> rse, Connection con) throws DataAccessException {
        Assert.notNull(sql, "SQL must not be null");
        Assert.notNull(rse, "ResultSetExtractor must not be null");

        class QueryStatementCallback implements StatementCallback<T>, SqlProvider {
            @Override
            @Nullable
            public T doInStatement(Statement stmt) throws SQLException {
                ResultSet rs = null;
                try {
                    rs = stmt.executeQuery(sql);
                    return rse.extractData(rs);
                } finally {
                    closeResultSet(rs);
                }
            }

            @Override
            public String getSql() {
                return sql;
            }
        }

        return execute(new QueryStatementCallback(), con);
    }

    public int update(final String sql, Connection con) throws DataAccessException {
        Assert.notNull(sql, "SQL must not be null");

        class UpdateStatementCallback implements StatementCallback<Integer>, SqlProvider {
            @Override
            public Integer doInStatement(Statement stmt) throws SQLException {
                int rows = stmt.executeUpdate(sql);
                if (logger.isTraceEnabled()) {
                    logger.trace("SQL update affected " + rows + " rows");
                }
                return rows;
            }

            @Override
            public String getSql() {
                return sql;
            }
        }

        return updateCount(execute(new UpdateStatementCallback(), con));
    }

    public int update(String sql, Connection con, @Nullable Object... args) throws DataAccessException {
        return update(sql, newArgPreparedStatementSetter(args), con);
    }

    public int update(String sql, @Nullable PreparedStatementSetter pss, Connection con) throws DataAccessException {
        return update(new SimplePreparedStatementCreator(sql), pss, con);
    }

    private int update(final PreparedStatementCreator psc, @Nullable final PreparedStatementSetter pss, Connection con)
            throws DataAccessException {

        return updateCount(execute(psc, ps -> {
            try {
                if (pss != null) {
                    pss.setValues(ps);
                }
                return ps.executeUpdate();
            } finally {
                if (pss instanceof ParameterDisposer) {
                    ((ParameterDisposer) pss).cleanupParameters();
                }
            }
        }, con));
    }

    public Map<String, Object> update(String sql, Connection con, String[] returningFields, @Nullable Object... args)
            throws DataAccessException {
        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        update(sql, newArgPreparedStatementSetter(args), keyHolder, returningFields, con);
        return keyHolder.getKeys();
    }

    public int update(String sql, @Nullable PreparedStatementSetter pss, final KeyHolder generatedKeyHolder,
            String[] returningFields, Connection con) throws DataAccessException {
        return update(new SimplePreparedStatementCreator(sql), pss, generatedKeyHolder, con);
    }

    public int update(final PreparedStatementCreator psc, @Nullable final PreparedStatementSetter pss,
            final KeyHolder generatedKeyHolder, Connection con) throws DataAccessException {
        Assert.notNull(generatedKeyHolder, "KeyHolder must not be null");
        return updateCount((Integer) execute(psc, (ps) -> {
            int rows = 0;
            try {
                if (pss != null) {
                    pss.setValues(ps);
                }
                rows = ps.executeUpdate();
            } finally {
                if (pss instanceof ParameterDisposer) {
                    ((ParameterDisposer) pss).cleanupParameters();
                }
            }
            List<Map<String, Object>> generatedKeys = generatedKeyHolder.getKeyList();
            generatedKeys.clear();
            ResultSet keys = ps.getGeneratedKeys();
            if (keys != null) {
                try {
                    RowMapperResultSetExtractor<Map<String, Object>> rse = new RowMapperResultSetExtractor<Map<String, Object>>(
                            getColumnMapRowMapper(), 1);
                    generatedKeys.addAll(result(rse.extractData(keys)));
                } finally {
                    closeResultSet(keys);
                }
            }

            return rows;
        }, con));
    }

    public Map<String, Object> update(String sql, String[] returningFields, @Nullable Object... args)
            throws DataAccessException {
        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        update(sql, newArgPreparedStatementSetter(args), keyHolder, returningFields);
        return keyHolder.getKeys();
    }

    public int update(String sql, @Nullable PreparedStatementSetter pss, final KeyHolder generatedKeyHolder,
            String[] returningFields) throws DataAccessException {
        return update(new SimplePreparedStatementCreator(sql), pss, generatedKeyHolder);
    }

    public int update(final PreparedStatementCreator psc, @Nullable final PreparedStatementSetter pss,
            final KeyHolder generatedKeyHolder) throws DataAccessException {
        Assert.notNull(generatedKeyHolder, "KeyHolder must not be null");
        return updateCount((Integer) execute(psc, (ps) -> {
            int rows = 0;
            try {
                if (pss != null) {
                    pss.setValues(ps);
                }
                rows = ps.executeUpdate();
            } finally {
                if (pss instanceof ParameterDisposer) {
                    ((ParameterDisposer) pss).cleanupParameters();
                }
            }
            List<Map<String, Object>> generatedKeys = generatedKeyHolder.getKeyList();
            generatedKeys.clear();
            ResultSet keys = ps.getGeneratedKeys();
            if (keys != null) {
                try {
                    RowMapperResultSetExtractor<Map<String, Object>> rse = new RowMapperResultSetExtractor<Map<String, Object>>(
                            getColumnMapRowMapper(), 1);
                    generatedKeys.addAll(result(rse.extractData(keys)));
                } finally {
                    closeResultSet(keys);
                }
            }

            return rows;
        }));
    }

    @Nullable
    private <T> T execute(StatementCallback<T> action, Connection con) throws DataAccessException {
        Assert.notNull(action, "Callback object must not be null");
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            applyStatementSettings(stmt);
            T result = action.doInStatement(stmt);
            handleWarnings(stmt);
            return result;
        } catch (SQLException ex) {
            String sql = getSql(action);
            closeStatement(stmt);
            throw translateException("StatementCallback", sql, ex);
        } finally {
            closeStatement(stmt);
        }
    }

    @Nullable
    private <T> T execute(PreparedStatementCreator psc, PreparedStatementCallback<T> action, Connection con)
            throws DataAccessException {

        Assert.notNull(psc, "PreparedStatementCreator must not be null");
        Assert.notNull(action, "Callback object must not be null");

        PreparedStatement ps = null;
        try {
            ps = psc.createPreparedStatement(con);
            applyStatementSettings(ps);
            T result = action.doInPreparedStatement(ps);
            handleWarnings(ps);
            return result;
        } catch (SQLException ex) {
            if (psc instanceof ParameterDisposer) {
                ((ParameterDisposer) psc).cleanupParameters();
            }
            String sql = getSql(psc);
            psc = null;
            closeStatement(ps);
            ps = null;
            throw translateException("PreparedStatementCallback", sql, ex);
        } finally {
            if (psc instanceof ParameterDisposer) {
                ((ParameterDisposer) psc).cleanupParameters();
            }
            closeStatement(ps);
        }
    }

    @Nullable
    private static String getSql(Object sqlProvider) {
        if (sqlProvider instanceof SqlProvider) {
            return ((SqlProvider) sqlProvider).getSql();
        } else {
            return null;
        }
    }

    private static int updateCount(@Nullable Integer result) {
        Assert.state(result != null, "No update count");
        return result;
    }

    private static <T> T result(@Nullable T result) {
        Assert.state(result != null, "No result");
        return result;
    }

    private static class SimplePreparedStatementCreator implements PreparedStatementCreator, SqlProvider {

        private final String sql;
        private final String[] returningFields;

        public SimplePreparedStatementCreator(String sql, @Nullable String... returningFields) {
            Assert.notNull(sql, "SQL must not be null");
            this.sql = sql;
            this.returningFields = returningFields;
        }

        @Override
        public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
            if (returningFields != null && returningFields.length > 0) {
                return con.prepareStatement(this.sql, returningFields);
            } else {
                return con.prepareStatement(this.sql);
            }
        }

        @Override
        public String getSql() {
            return this.sql;
        }
    }

    public Connection getConnection() throws CannotGetJdbcConnectionException {
        try {
            Connection con = getDataSource().getConnection();
            if (con == null) {
                throw new IllegalStateException("DataSource returned null from getConnection(): " + getDataSource());
            }
            con.setAutoCommit(false);
            return con;
        } catch (SQLException ex) {
            throw new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection", ex);
        } catch (IllegalStateException ex) {
            throw new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection", ex);
        }
    }

    public void closeConnection(@Nullable Connection con) {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException ex) {
                logger.error("Could not close JDBC Connection", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on closing JDBC Connection", ex);
            }
        }
    }

    public void closeStatement(@Nullable Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
                stmt = null;
            } catch (SQLException ex) {
                logger.error("Could not close JDBC Statement", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on closing JDBC Statement", ex);
            }
        }
    }

    public void closeResultSet(@Nullable ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException ex) {
                logger.error("Could not close JDBC ResultSet", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on closing JDBC ResultSet", ex);
            }
        }
    }

    public void setTransactionIsolation(Connection con, int transaction) {
        if (con != null) {
            try {
                con.setTransactionIsolation(transaction);
            } catch (SQLException ex) {
                logger.error("Could not set transaction level", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on setting transaction level", ex);
            }
        }
    }

    public void commit(Connection con) {
        if (con != null) {
            try {
                con.commit();
                closeConnection(con);
                con = null;
            } catch (SQLException ex) {
                logger.error("Could not commit", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on commit", ex);
            }
        }
    }

    public void rollback(Connection con) {
        if (con != null) {
            try {
                con.rollback();
                closeConnection(con);
                con = null;
            } catch (SQLException ex) {
                logger.error("Could not rollback", ex);
            } catch (Throwable ex) {
                logger.error("Unexpected exception on rollback", ex);
            }
        }
    }
}
