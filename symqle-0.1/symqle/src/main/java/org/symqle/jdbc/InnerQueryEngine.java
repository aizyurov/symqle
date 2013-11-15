package org.symqle.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author lvovich
 */
class InnerQueryEngine extends AbstractQueryEngine {

    private final Connection connection;

    public InnerQueryEngine(final AbstractQueryEngine parent, final Connection connection) {
        super(parent);
        this.connection = connection;
    }

    @Override
    protected Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    protected void releaseConnection(final Connection connection) throws SQLException {
        // do nothing
    }
}
