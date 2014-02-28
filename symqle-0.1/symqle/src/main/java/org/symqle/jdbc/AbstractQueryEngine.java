package org.symqle.jdbc;

import org.symqle.common.Callback;
import org.symqle.common.Row;
import org.symqle.common.Sql;
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
 * @author lvovich
 */
abstract class AbstractQueryEngine implements QueryEngine {

    private final Dialect dialect;
    private final Option[] options;
    private final String databaseName;

    /**
     * Constructs the engine
     * @param dialect if null, auto-detect
     * @param databaseName used for dialect detection
     * @param options options to apply for query building and execution
     */
    protected AbstractQueryEngine(final Dialect dialect, final String databaseName, final Option[] options) {
        this.dialect = dialect;
        this.options = Arrays.copyOf(options, options.length);
        this.databaseName = databaseName;
    }

    protected AbstractQueryEngine(final String databaseName, final Option[] options) {
        this(DatabaseUtils.getDialect(databaseName), databaseName, options);
    }

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

    protected final  int scroll(final Connection connection, final Sql query, final Callback<Row> callback, final List<Option> options) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();
        query.appendTo(stringBuilder);
        final PreparedStatement preparedStatement = connection.prepareStatement(stringBuilder.toString());
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

    protected final void setupStatement(final PreparedStatement preparedStatement, final Sql sql, final List<Option> options) throws SQLException {
        for (Option option : options) {
            option.apply(preparedStatement);
        }
        SqlParameters parameters = new StatementParameters(preparedStatement);
        sql.setParameters(parameters);
    }

}
