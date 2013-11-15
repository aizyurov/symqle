package org.symqle.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author lvovich
 */
interface Connector {

    /**
     * Makes a connection to data source
     *
     * @return  a connection to the data source
     * @exception java.sql.SQLException if a database access error occurs
     */
    Connection getConnection() throws SQLException;
}
