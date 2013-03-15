package org.simqle.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * An option, which can be applied to a Statement, e.g. fetch size, query timeout etc.
 * @author lvovich
 */
public interface StatementOption {
    void apply(Statement statement) throws SQLException;
}
