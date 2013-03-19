package org.simqle.front;

import org.simqle.Query;
import org.simqle.Row;
import org.simqle.RowMapper;
import org.simqle.SqlContext;
import org.simqle.SqlParameters;
import org.simqle.sql.AbstractSelectList;
import org.simqle.sql.SelectList;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A basic class to build custom mappers.
 * @author lvovich
 * @param <D> data type
 */
public abstract class AbstractMapper<D> extends AbstractSelectList<D> {


    private final List<KeyImpl<?>> keys = new ArrayList<KeyImpl<?>>();
    private final AtomicBoolean keysLocked = new AtomicBoolean();

    protected abstract D create(final Row row) throws SQLException;

    @Override
    public final Query<D> z$create$SelectList(final SqlContext context) {
        keysLocked.set(true);
        if (keys.isEmpty()) {
            throw new IllegalStateException("No mappings defined");
        }
        AbstractSelectList<?> result = keys.get(0).selectList;
        for (int i=1; i<keys.size(); i++) {
            result = result.pair(keys.get(i).selectList);
        }
        final Query<?> query = result.z$create$SelectList(context);
        return new Query<D>() {
            @Override
            public D extract(final Row row) throws SQLException {
                return create(row);
            }

            @Override
            public String getSqlText() {
                return query.getSqlText();
            }

            @Override
            public void setParameters(final SqlParameters sqlParameters) throws SQLException {
                query.setParameters(sqlParameters);
            }
        };
    }

    public final <E> KeyImpl<E> map(final SelectList<E> selectList) {
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
                public Query<E> z$create$SelectList(final SqlContext context) {
                    final Query<E> query = selectList.z$create$SelectList(context);
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
}
