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
import org.symqle.common.Sql;
import org.symqle.common.Parameterizer;
import org.symqle.common.Row;
import org.symqle.common.SqlParameters;
import org.symqle.sql.Dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Implements basic functionality of QueryEngine.
 * @author lvovich
 */
abstract class AbstractQueryEngine implements QueryEngine {

    private final Dialect dialect;
    private final Option[] options;
    private final String databaseName;

    /**
     * Constructs the engine
     * @param dialect forces the use of this dialect, no auto-detection
     * @param databaseName used for dialect detection
     * @param options options to apply for query building and execution
     */
    protected AbstractQueryEngine(final Dialect dialect, final String databaseName, final Option[] options) {
        this.dialect = dialect;
        this.options = Arrays.copyOf(options, options.length);
        this.databaseName = databaseName;
    }

    /**
     * Constructs with auto-detected Dialect.
     * @param databaseName used for dialect detection
     * @param options options to apply for query building and execution
     */
    protected AbstractQueryEngine(final String databaseName, final Option[] options) {
        this(DatabaseUtils.getDialect(databaseName), databaseName, options);
    }

    /**
     * Copy constructor.
     * @param other
     */
    protected AbstractQueryEngine(final AbstractQueryEngine other) {
        this(other.dialect, other.databaseName, Arrays.copyOf(other.options, other.options.length));
    }

    @Override
    public final String getDatabaseName() {
        return databaseName;
    }

    @Override
    public Dialect getDialect() {
        return dialect;
    }

    @Override
    public List<Option> getOptions() {
        return Collections.unmodifiableList(Arrays.asList(options));
    }

    /**
     * Scroll using already open connection.
     * @param connection open connection
     * @param query the query to execute
     * @param callback called for each row
     * @param options will be applied after default options
     * @return number of processed rows
     * @throws SQLException something wrong happened (from JDBC driver)
     */
    protected final  int scroll(final Connection connection, final Sql query, final Callback<Row> callback, final List<Option> options) throws SQLException {
        final PreparedStatement preparedStatement = connection.prepareStatement(query.text());
        try {
            setupStatement(preparedStatement, query, options);
            final ResultSet resultSet = preparedStatement.executeQuery();
            try {
                final InnerQueryEngine innerEngine = new InnerQueryEngine(this, connection);
                int count = 0;
                while (resultSet.next()) {
                    count += 1;
                    final Row row = new ResultSetRow(resultSet, innerEngine);
                    if (!callback.iterate(row)) {
                        break;
                    }
                }
                return count;
            } finally {
                resultSet.close();
            }
        } finally {
            preparedStatement.close();
        }
    }

    /**
     * Applies options and sets parameters to {@link PreparedStatement}.
     * Call this method after statement creation.
     * @param preparedStatement the statement
     * @param parameterizer sets parameters
     * @param options will be applied to statement.
     * @throws SQLException from JDBC driver
     */
    protected final void setupStatement(final PreparedStatement preparedStatement, final Parameterizer parameterizer, final List<Option> options) throws SQLException {
        for (Option option : options) {
            option.apply(preparedStatement);
        }
        SqlParameters parameters = new StatementParameters(preparedStatement);
        parameterizer.setParameters(parameters);
    }

}
