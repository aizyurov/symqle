/*
   Copyright 2010-2013 Alexander Izyurov

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.package org.symqle.common;
*/

package org.symqle.sql;

import org.symqle.common.*;
import org.symqle.jdbc.Option;
import org.symqle.jdbc.QueryEngine;
import org.symqle.querybuilder.ColumnNameGenerator;
import org.symqle.querybuilder.Configuration;
import org.symqle.querybuilder.TableNameGenerator;
import org.symqle.querybuilder.UniqueColumnNameGenerator;
import org.symqle.querybuilder.UpdatableConfiguration;

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
 * A base class for Selectors, which has more simple API than Selector (at the expence of some performance penalty).
 * You have only to implement {@link #create()} method, using {@link #get(SelectList)} inside.
 * Example:
 * <pre>
 * <code>
 *     public class PersonSelector extends SmartSelector&gt;PersonDTO&lt; {
 *         private final Person person;
 *         ...
 *         public PersonSelector(final Person person) {
 *             this.person = person;
 *         }
 *         protected PersonDTO create() {
 *             return new PersonDTO(
 *                  get(person.id()),
 *                  get(person.name()),
 *                  ...
 *             );
 *         }
 *     }
 * </code>
 * </pre>
 * @param <D> type of created objects
 * @author lvovich
 */
public abstract class SmartSelector<D> extends AbstractSelectList<D> {

     /**
      * Implement in derived classes.
      * Recursive calls to {@link #create()}
      * are not allowed. Use {@link #get(SelectList)} in the body
      * to get data from current row.
      * @return created object
      * @throws SQLException error extracting data from row (e.g. wrong data type).
     */
    protected abstract D create() throws SQLException;

    /**
     * Extracts a value from current row.
     * @param selectList what is extracted.
     * @param <T> result type
     * @return extracted value
     * @throws SQLException from JDBC driver
     */
    protected final <T> T get(final SelectList<T> selectList) throws SQLException {
        return currentRowMap.get(selectList);
    }

    /**
     * Engine, which re-uses the same connection.
     * This engine can be used inside {@link #create()} method to make additional queries
     * (probably depending of the values in current row).
     * @return engine, which operates on current connection
     */
    protected final QueryEngine getQueryEngine() {
        return currentRowMap.getQueryEngine();
    }

    @Override
    public final QueryBuilder<D> z$sqlOfSelectList(final SqlContext context) {
        currentRowMap = new ProbeRowMap();
        try {
            create();
        } catch (SQLException e) {
            // never expected from ProbeRowMap
            Bug.reportException(e);
        }
        final SqlBuilder sql = result.z$sqlOfSelectList(context);
        return new QueryBuilder<D>() {
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

            @Override
            public char firstChar() {
                return sql.firstChar();
            }
        };
    }

    private RowMap currentRowMap;

    private interface RowMap {
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

        public ProbeRowMap() {
        }

        @Override
        public <T> T get(final SelectList<T> selectList) throws SQLException {
            if (result == null) {
                result = AbstractSelectList.adapt(new ProbeSelectList<T>(selectList));
            } else {
                result = result.pair(new ProbeSelectList<T>(selectList));
            }
            SqlContext context = probeContext();
            final QueryBuilder<T> query = selectList.z$sqlOfSelectList(context);
            return query.extract(probeRow);
        }

        @Override
        public QueryEngine getQueryEngine() {
            return probeRow.getQueryEngine();
        }
    }

    private class ProbeSelectList<T> extends AbstractSelectList<T> {
        private final SelectList<T> sl;

        public ProbeSelectList(final SelectList<T> sl) {
            this.sl = sl;
        }

        @Override
        public QueryBuilder<T> z$sqlOfSelectList(final SqlContext context) {
            final QueryBuilder<T> query = sl.z$sqlOfSelectList(context);
            mappers.put(key(sl), query);
            return query;
        }
    }

    private class ResultSetRowMap implements RowMap {
        private final Row row;

        public ResultSetRowMap(final Row row) {
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
        final QueryBuilder<?> query = selectList.z$sqlOfSelectList(probeContext());
        final StringBuilder builder = new StringBuilder();
        query.appendTo(builder);
        return builder.toString();
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

    private static final Row probeRow = new ProbeRow();

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

        @Override
        public Dialect getDialect() {
            return new DebugDialect();
        }

        @Override
        public List<Option> getOptions() {
            return Arrays.<Option>asList(Option.allowImplicitCrossJoins(true), Option.allowNoTables(true));
        }

        @Override
        public int scroll(final Sql query, final Callback<Row> callback, final List<Option> options)
                throws SQLException {
            return 0;
        }

        @Override
        public String getDatabaseName() {
            return getDialect().getName();
        }
    }
}
