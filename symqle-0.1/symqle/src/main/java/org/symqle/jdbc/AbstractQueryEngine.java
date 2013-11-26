package org.symqle.jdbc;

import org.symqle.common.Callback;
import org.symqle.common.Row;
import org.symqle.common.Sql;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.sql.Dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

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
    public final SqlContext initialContext() {
        UpdatableConfiguration configuration = new UpdatableConfiguration();
        for (Option option : options) {
            option.apply(configuration);
        }
        return new SqlContext().
                put(Dialect.class, dialect).
                put(Configuration.class, configuration);
    }

    protected final  int scroll(final Connection connection, final Sql query, final Callback<Row> callback, final Option[] options) throws SQLException {
        final PreparedStatement preparedStatement = connection.prepareStatement(query.sql());
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

    protected final void setupStatement(final PreparedStatement preparedStatement, final Sql sql, final Option[] options) throws SQLException {
        for (Option option : this.options) {
            option.apply(preparedStatement);
        }
        for (Option option : options) {
            option.apply(preparedStatement);
        }
        SqlParameters parameters = new StatementParameters(preparedStatement);
        sql.setParameters(parameters);
    }

}
