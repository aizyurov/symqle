package org.symqle.jdbc;

import org.symqle.common.Callback;
import org.symqle.common.Query;
import org.symqle.common.Row;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lvovich
 */
public class PreparedQuery<T> {

    private final QueryEngine engine;
    private final Query<T> query;
    private final List<Option> options;

    public PreparedQuery(final QueryEngine engine, final Query<T> query, final List<Option> options) {
        this.engine = engine;
        this.query = query;
        this.options = options;
    }

    public List<T> list() throws SQLException {
        final List<T> list = new ArrayList<T>();
        scroll(new Callback<T>() {
            @Override
            public boolean iterate(final T t) throws SQLException {
                list.add(t);
                return true;
            }
        });
        return list;
    }

    public int scroll(final Callback<T> callback) throws SQLException {
        return engine.scroll(query, new Callback<Row>() {
            @Override
            public boolean iterate(final Row row) throws SQLException {
                return callback.iterate(query.extract(row));
            }
        }, options);
    }
}
