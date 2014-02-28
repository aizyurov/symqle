package org.symqle.sql;

import org.symqle.common.*;
import org.symqle.jdbc.Configuration;
import org.symqle.jdbc.Option;
import org.symqle.jdbc.QueryEngine;
import org.symqle.jdbc.UpdatableConfiguration;
import org.symqle.querybuilder.TableNameGenerator;
import org.symqle.querybuilder.UniqueColumnNameGenerator;
import org.symqle.querybuilder.ColumnNameGenerator;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lvovich
 */
public abstract class SmartSelector<D> extends AbstractSelectList<D> {

    protected abstract D create() throws SQLException;

    protected final <T> T get(SelectList<T> selectList) throws SQLException {
        return currentRowMap.get(selectList);
    }

    protected final QueryEngine getQueryEngine() {
        return currentRowMap.getQueryEngine();
    }

    @Override
    public Query<D> z$sqlOfSelectList(final SqlContext context) {
        currentRowMap = new ProbeRowMap(context);
        try {
            create();
        } catch (SQLException e) {
            // never expected from ProbeRowMap
            Bug.reportException(e);
        }
        final Sql sql = result.z$sqlOfSelectList(context);
        return new Query<D>() {
            @Override
            public D extract(final Row row) throws SQLException {
                currentRowMap = new ResultSetRowMap(row);
                return create();
            }

            @Override
            public void appendTo(final StringBuilder builder) {
                sql.appendTo(builder);
            }

            @Override
            public void setParameters(final SqlParameters p) throws SQLException {
                sql.setParameters(p);
            }
        };
    }

    private RowMap currentRowMap;

    public interface RowMap {
        <T> T get(SelectList<T> selectList) throws SQLException;
        /**
         * An engine to execute queries on the same connection.
         * May be used to make query for each row, which depends on row values.
         * Although it is not a good practice, it may be useful.
         * @return proper QueryEngine
         */
        QueryEngine getQueryEngine();
    }

    private class ProbeRowMap implements RowMap {
        private final SqlContext context;

        private ProbeRowMap(final SqlContext context) {
            this.context = context;
        }

        @Override
        public <T> T get(final SelectList<T> selectList) throws SQLException {
            if (result == null) {
                result = AbstractSelectList.adapt(new ProbeSelectList<T>(selectList));
            } else {
                result = result.pair(new ProbeSelectList<T>(selectList));
            }
            SqlContext context = probeContext();
            final Query<T> query = selectList.z$sqlOfSelectList(context);
            return query.extract(probeRow);
        }

        @Override
        public QueryEngine getQueryEngine() {
            return probeRow.getQueryEngine();
        }
    }

    private class ProbeSelectList<T> extends AbstractSelectList<T> {
        private final SelectList<T> sl;

        private ProbeSelectList(final SelectList<T> sl) {
            this.sl = sl;
        }

        @Override
        public Query<T> z$sqlOfSelectList(final SqlContext context) {
            final Query<T> query = sl.z$sqlOfSelectList(context);
            mappers.put(key(sl), query);
            return query;
        }
    }

    private class ResultSetRowMap implements RowMap {
        private final Row row;

        private ResultSetRowMap(final Row row) {
            this.row = row;
        }

        @Override
        public <T> T get(final SelectList<T> selectList) throws SQLException {
            return (T) mappers.get(key(selectList)).extract(row);
        }

        @Override
        public QueryEngine getQueryEngine() {
            return row.getQueryEngine();
        }
    }

    private AbstractSelectList<?> result = null;

    private final Map<String, RowMapper<?>> mappers = new HashMap<String, RowMapper<?>>();

    private String key(final SelectList<?> selectList) {
        return selectList.z$sqlOfSelectList(probeContext()).toString();
    }

    private SqlContext probeContext() {
        final UpdatableConfiguration configuration = new UpdatableConfiguration();
        final Option noTables = Option.allowNoTables(true);
        noTables.apply(configuration);
        final Option crossJoins = Option.allowImplicitCrossJoins(true);
        crossJoins.apply(configuration);
        return new SqlContext.Builder()
                .put(Dialect.class, new DebugDialect())
                .put(TableRegistry.class, new RootSelectTableRegistry())
                .put(ColumnNameGenerator.class, new UniqueColumnNameGenerator())
                .put(Configuration.class, configuration)
                .put(TableNameGenerator.class, new TableNameGenerator())
                .toSqlContext();
    }

    private final static Row probeRow = new ProbeRow();

    private static class ProbeRow implements Row {
        @Override
        public InBox getValue(final String label) {
            return new ProbeElement();
        }

        @Override
        public QueryEngine getQueryEngine() {
            return new ProbeQueryEngine();
        }
    }

    private static class ProbeElement implements InBox {
        @Override
        public Boolean getBoolean() throws SQLException {
            return Boolean.TRUE;
        }

        @Override
        public Byte getByte() throws SQLException {
            return 0;
        }

        @Override
        public Short getShort() throws SQLException {
            return 0;
        }

        @Override
        public Integer getInt() throws SQLException {
            return 0;
        }

        @Override
        public Long getLong() throws SQLException {
            return 0L;
        }

        @Override
        public Float getFloat() throws SQLException {
            return 0.0f;
        }

        @Override
        public Double getDouble() throws SQLException {
            return 0.0;
        }

        @Override
        public BigDecimal getBigDecimal() throws SQLException {
            return new BigDecimal("0");
        }

        @Override
        public String getString() throws SQLException {
            return "a";
        }

        @Override
        public Date getDate() throws SQLException {
            return new Date(System.currentTimeMillis());
        }

        @Override
        public Time getTime() throws SQLException {
            return new Time(0);
        }

        @Override
        public Timestamp getTimestamp() throws SQLException {
            return new Timestamp(System.currentTimeMillis());
        }

        @Override
        public byte[] getBytes() throws SQLException {
            return new byte[] {(byte) 0};
        }
    }

    private static class ProbeQueryEngine implements QueryEngine {

        private ProbeQueryEngine() {
            System.out.println("ProbeQueryEngine constructor");
        }

        @Override
        public Dialect getDialect() {
            return new DebugDialect();
        }

        @Override
        public List<Option> getOptions() {
            return Arrays.<Option>asList(Option.allowImplicitCrossJoins(true), Option.allowNoTables(true));
        }

        @Override
        public int scroll(final Sql query, final Callback<Row> callback, final List<Option> options) throws SQLException {
            return 0;
        }

        @Override
        public String getDatabaseName() {
            System.out.println("getDatabaseName called: " + getDialect().getName());
            return getDialect().getName();
        }
    }
}
