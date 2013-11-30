package org.symqle.generic;

import org.symqle.common.Query;
import org.symqle.common.Row;
import org.symqle.common.RowMapper;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.sql.AbstractSelectList;
import org.symqle.sql.SelectList;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A basic class to build custom selectors.
 * @author lvovich
 * @param <D> data type
 */
public abstract class AbstractSelector<D> extends AbstractSelectList<D> {


    private final List<KeyImpl<?>> keys = new ArrayList<KeyImpl<?>>();
    private final AtomicBoolean keysLocked = new AtomicBoolean();

    protected abstract D create(final Row row) throws SQLException;

    @Override
    public final Query<D> z$sqlOfSelectList(final SqlContext context) {
        keysLocked.set(true);
        if (keys.isEmpty()) {
            throw new IllegalStateException("No mappings defined");
        }
        AbstractSelectList<?> result = keys.get(0).selectList;
        for (int i=1; i<keys.size(); i++) {
            result = result.pair(keys.get(i).selectList);
        }
        final Query<?> query = result.z$sqlOfSelectList(context);
        return new InnerQuery(query);
    }

    public final <E> RowMapper<E> map(final SelectList<E> selectList) {
        if (keysLocked.get()) {
            throw new IllegalStateException("map() cannot be called at this point");
        }
        final KeyImpl<E> key = new KeyImpl<E>(selectList);
        keys.add(key);
        return key;
    }


    public class KeyImpl<E> implements RowMapper<E> {
        private final AbstractSelectList<E> selectList;
        private RowMapper<E> rowMapper;

        private KeyImpl(final SelectList<E> selectList) {
            this.selectList = new AbstractSelectList<E>() {
                @Override
                public Query<E> z$sqlOfSelectList(final SqlContext context) {
                    final Query<E> query = selectList.z$sqlOfSelectList(context);
                    rowMapper = query;
                    return query;
                }
            };
        }


        @Override
        public E extract(final Row row) throws SQLException {
            return rowMapper.extract(row);
        }

    }

    private class InnerQuery extends Query<D> {
        private final Query<?> query;

        public InnerQuery(final Query<?> query) {
            this.query = query;
        }

        @Override
        public D extract(final Row row) throws SQLException {
            return create(row);
        }

        @Override
        public void append(final StringBuilder builder) {
            query.append(builder);
        }

        @Override
        public void setParameters(final SqlParameters sqlParameters) throws SQLException {
            query.setParameters(sqlParameters);
        }
    }
}
