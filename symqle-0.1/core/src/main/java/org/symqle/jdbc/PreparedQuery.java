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

package org.symqle.jdbc;

import org.symqle.common.Callback;
import org.symqle.common.CompiledSql;
import org.symqle.common.QueryBuilder;
import org.symqle.common.Row;
import org.symqle.common.RowMapper;
import org.symqle.common.Sql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Compiled and prepared for execution query.
 * @param <T> JAva type of objects, to which query results are converted.
 * @author lvovich
 */
public class PreparedQuery<T> {

    private final QueryEngine engine;
    private final Sql sql;
    private final RowMapper<T> rowMapper;
    private final List<Option> options;

    /**
     * Constructs the query.
     * @param engine the engine to use for list/scroll.
     * @param query the query to compile
     * @param options options to use while executing.
     */
    public PreparedQuery(final QueryEngine engine, final QueryBuilder<T> query, final List<Option> options) {
        this.engine = engine;
        this.sql = new CompiledSql(query);
        this.rowMapper = query;
        this.options = options;
    }

    /**
     * Execute {@code this} and convert the results to type T.
     * @return result set converted to Java objects,
     * @throws SQLException from JDBC driver
     */
    public List<T> list() throws SQLException {
        final List<T> list = new ArrayList<T>();
        scroll(new Callback<T>() {
            @Override
            public boolean iterate(final T t) throws SQLException {
                list.add(t);
                return true;
            }
        });
        return list;
    }

    /**
     * Execute {@code this} and calls callback for each row of result set,
     * Exits when end of result set is reached or callback returns false.
     * @return number of processed rows,,
     * @throws SQLException from JDBC driver
     */
    public int scroll(final Callback<T> callback) throws SQLException {
        return engine.scroll(sql, new Callback<Row>() {
            @Override
            public boolean iterate(final Row row) throws SQLException {
                return callback.iterate(rowMapper.extract(row));
            }
        }, options);
    }
}
