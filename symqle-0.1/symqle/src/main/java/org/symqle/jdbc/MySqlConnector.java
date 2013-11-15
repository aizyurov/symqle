package org.symqle.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * @author lvovich
 */
class MySqlConnector implements Connector {

    // true if global PIPES_AS_CONCAT
    // false if have to set session variable
    // null if unknown
    private Boolean pipesAsConcat;
    private final static String PIPES_AS_CONCAT = "PIPES_AS_CONCAT";

    private final Connector connector;

    public MySqlConnector(final Connector connector) {
        this.connector = connector;
    }

    @Override
    public Connection getConnection() throws SQLException {
        final Connection connection = connector.getConnection();
        prepareConnection(connection);
        return connection;
    }

    private void prepareConnection(Connection connection) throws SQLException {
        String sqlMode = null;
        if (pipesAsConcat == null) {
            sqlMode = getSqlMode(connection);
            pipesAsConcat = pipesAsConcatSet(sqlMode);
        }
        if (pipesAsConcat) {
            // global setting
            return;
        }
        // got here if pipesAsConcat is false before or set to false just now
        // in the first case sqlMode may be unknown
        if (sqlMode == null) {
            sqlMode = getSqlMode(connection);
        }
        if (pipesAsConcatSet(sqlMode)) {
            // already set for this connection
            return;
        }
        final PreparedStatement preparedStatement = connection.prepareStatement("set session sql_mode=?");
        try {
            preparedStatement.setString(1,  "".equals(sqlMode) ? PIPES_AS_CONCAT : sqlMode +","+ PIPES_AS_CONCAT);
            preparedStatement.executeUpdate();
        } finally {
            preparedStatement.close();
        }

    }
    private boolean pipesAsConcatSet(final String sqlMode) {
        return Arrays.asList(sqlMode.split(",")).contains(PIPES_AS_CONCAT);
    }

    private String getSqlMode(final Connection connection) throws SQLException {
        final PreparedStatement preparedStatement = connection.prepareStatement("select @@sql_mode");
        final String sqlMode;
        try {
            final ResultSet resultSet = preparedStatement.executeQuery();
            try {
                resultSet.next();
                sqlMode = resultSet.getString(1);
            } finally {
                resultSet.close();
            }
        } finally {
            preparedStatement.close();
        }
        return sqlMode;
    }
}
