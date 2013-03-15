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

/**
 * A basic class to build custom mappers.
 * @author lvovich
 * @param <D> data type
 */
public abstract class AbstractMapper<D> extends AbstractSelectList<D> {


    private final List<KeyImpl<?>> keys = new ArrayList<KeyImpl<?>>();

    protected abstract D create(final Row row) throws SQLException;

    @Override
    public final Query<D> z$create$SelectList(final SqlContext context) {
                // todo block calls to key() after this point
        if (keys.isEmpty()) {
            throw new IllegalStateException("No keys defined");
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

    public final <E> KeyImpl<E> key(final SelectList<E> selectList) {
        final KeyImpl<E> key = new KeyImpl<E>(selectList);
        keys.add(key);
        return key;
    }


    public interface Key<E> {
        E value(final Row row) throws SQLException;
    }

    public class KeyImpl<E> implements Key<E> {
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
        public E value(final Row row) throws SQLException {
            return rowMapper.extract(row);
        }

    }
}
