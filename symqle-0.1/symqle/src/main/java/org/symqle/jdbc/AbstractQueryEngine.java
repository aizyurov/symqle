package org.symqle.jdbc;

import org.symqle.common.Callback;
import org.symqle.common.Query;
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

    protected AbstractQueryEngine(final Dialect dialect, final Option[] options) {
        this.dialect = dialect;
        this.options = Arrays.copyOf(options, options.length);
    }

    protected AbstractQueryEngine(final AbstractQueryEngine other) {
        this.dialect = other.dialect;
        this.options = Arrays.copyOf(other.options, other.options.length);
    }

    protected abstract Connection getConnection() throws SQLException;

    protected abstract void releaseConnection(Connection connection) throws SQLException;

    @Override
    public final SqlContext initialContext() {
        final SqlContext context = new SqlContext();
        context.set(Dialect.class, dialect);
        UpdatableConfiguration configuration = new UpdatableConfiguration();
        for (Option option : options) {
            option.apply(configuration);
        }
        context.set(Configuration.class, configuration);
        return context;
    }

    @Override
    public <T> int scroll(final Query<T> query, final Callback<T> callback, final Option... options) throws SQLException {
        final Connection connection = getConnection();
        try {
            return scrollInConnection(connection, query, callback, options);
        } finally {
            releaseConnection(connection);
        }
    }

    protected final <T> int scrollInConnection(final Connection connection, final Query<T> query, final Callback<T> callback, final Option[] options) throws SQLException {
        final PreparedStatement preparedStatement = connection.prepareStatement(query.sql());
        try {
            setupStatement(preparedStatement, query, options);
            final ResultSet resultSet = preparedStatement.executeQuery();
            final InnerQueryEngine innerEngine = new InnerQueryEngine(this, connection);
            int count = 0;
            while (resultSet.next()) {
                count += 1;
                final Row row = new ResultSetRow(resultSet, innerEngine);
                if (!callback.iterate(query.extract(row))) {
                    break;
                }
            }
            return count;
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
