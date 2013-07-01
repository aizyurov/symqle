package org.simqle.front;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * @author lvovich
 */
public class MySqlConnectionCallback implements ConnectionCallback {

    // true if global PIPES_AS_CONCAT
    // false if have to set session variable
    // null if unknown
    private Boolean pipesAsConcat;
    private final static String PIPES_AS_CONCAT = "PIPES_AS_CONCAT";

    @Override
    public void call(final Connection connection) throws SQLException {
        String sqlMode = null;
        if (pipesAsConcat == null) {
            sqlMode = getSqlMode(connection);
            pipesAsConcat = Arrays.asList(sqlMode.split(",")).contains(PIPES_AS_CONCAT);
        }
        if (!pipesAsConcat) {
            if (sqlMode == null) {
                sqlMode = getSqlMode(connection);
            }
            final PreparedStatement preparedStatement = connection.prepareStatement("set session sql_mode=?");
            try {
                preparedStatement.setString(1,  "".equals(sqlMode) ? PIPES_AS_CONCAT : sqlMode +","+ PIPES_AS_CONCAT);
                preparedStatement.executeUpdate();
            } finally {
                preparedStatement.close();
            }
        }
    }

    private String getSqlMode(final Connection connection) {
        try {
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
        } catch (SQLException e) {
            throw new IllegalStateException("Error getting sql_mode", e);
        }
    }
}
