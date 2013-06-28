package org.simqle.jdbc;

import org.simqle.jdbc.UpdatableConfiguration;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * An option, which can be applied to a Statement, e.g. fetch size, query timeout etc.
 * @author lvovich
 */
public abstract class StatementOption {
    public void apply(Statement statement) throws SQLException {
    }
    public void apply(UpdatableConfiguration configuration) {
    }
}
