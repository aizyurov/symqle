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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * MySQL-specific implementation of Connector.
 * It checks variable sql_mode and adds PIPES_AS_CONCAT if necessary (to session scope sql_mode).
 * @author lvovich
 */
class MySqlConnector implements Connector {

    // true if global PIPES_AS_CONCAT
    // false if have to set session variable
    // null if unknown
    private Boolean pipesAsConcat;
    private static final String PIPES_AS_CONCAT = "PIPES_AS_CONCAT";

    private final Connector connector;

    /**
     * Constructs from another connector.
     * @param connector
     */
    public MySqlConnector(final Connector connector) {
        this.connector = connector;
    }

    @Override
    public Connection getConnection() throws SQLException {
        final Connection connection = connector.getConnection();
        prepareConnection(connection);
        return connection;
    }

    private void prepareConnection(final Connection connection) throws SQLException {
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
            preparedStatement.setString(1,  "".equals(sqlMode) ? PIPES_AS_CONCAT : sqlMode + "," + PIPES_AS_CONCAT);
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
