package org.symqle.gate;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author lvovich
 */
public interface ConnectionCallback {
    void call(Connection connection) throws SQLException ;
}
