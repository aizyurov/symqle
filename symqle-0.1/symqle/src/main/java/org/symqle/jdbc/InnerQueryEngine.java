package org.symqle.jdbc;

import org.symqle.common.Callback;
import org.symqle.common.Query;

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
    public <T> int scroll(final Query<T> query, final Callback<T> callback, final Option... options) throws SQLException {
        return scroll(connection, query, callback, options);
    }
}
