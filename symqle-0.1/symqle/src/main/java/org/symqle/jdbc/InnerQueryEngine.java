package org.symqle.jdbc;

import org.symqle.common.Callback;
import org.symqle.common.Row;
import org.symqle.common.Sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

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
    public int scroll(final Sql query, final Callback<Row> callback, final List<Option> options) throws SQLException {
        return scroll(connection, query, callback, options);
    }
}
