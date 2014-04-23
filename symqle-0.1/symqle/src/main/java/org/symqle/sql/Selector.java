/*
   Copyright 2010-2013 Alexander Izyurov

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.package org.symqle.common;
*/

package org.symqle.sql;

import org.symqle.common.QueryBuilder;
import org.symqle.common.Row;
import org.symqle.common.RowMapper;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A basic class to build custom selectors.
 * Use {@link #map(SelectList)} to define what you are selecting and save
 * RowMappers in private members.
 * Implement {@link #create(org.symqle.common.Row)}, calling
 * {@link RowMapper#extract(org.symqle.common.Row)}.
 * Example:
 * <pre>
 * <code>
 *     public class PersonSelector extends Selector&gt;PersonDTO&lt; {
 *         private final RowMapper&lt;String&gt; nameMapper;
 *         ...
 *         public PersonSelector(final Person person) {
 *             nameMapper = map(person.name());
 *             ...
 *         }
 *         protected PersonDTO create(final Row row) {
 *             return new PersonDTO(
 *                  nameMapper.extract(row),
 *                  ...
 *             );
 *         }
 *     }
 * </code>
 * </pre>
 * @author lvovich
 * @param <D> data type
 */
public abstract class Selector<D> extends AbstractSelectList<D> {


    private final List<KeyImpl<?>> keys = new ArrayList<KeyImpl<?>>();
    private final AtomicBoolean keysLocked = new AtomicBoolean();

    /**
     * Implement in derived classes.
     * {@link #map(SelectList)} and recursive calls to {@link #create(org.symqle.common.Row)}
     * are not allowed, Use {@link RowMapper#extract(org.symqle.common.Row)} in the body
     * to get data from row.
     * @param row current row
     * @return created object
     * @throws SQLException error extracting data from row (e.g. wrong data type).
     */
    protected abstract D create(final Row row) throws SQLException;

    @Override
    public final QueryBuilder<D> z$sqlOfSelectList(final SqlContext context) {
        keysLocked.set(true);
        if (keys.isEmpty()) {
            throw new IllegalStateException("No mappings defined");
        }
        AbstractSelectList<?> result = keys.get(0).selectList;
        for (int i = 1; i < keys.size(); i++) {
            result = result.pair(keys.get(i).selectList);
        }
        final QueryBuilder<?> query = result.z$sqlOfSelectList(context);
        return new InnerQueryBuilder(query);
    }

    /**
     * Creates a RowMapper, which can be later used in create() method.
     * Use this method in constructor and save results in class members.
     * @param selectList what you are selecting
     * @param <E> Java type of selected value
     * @return ready to use RowMapper
     */
    public final <E> RowMapper<E> map(final SelectList<E> selectList) {
        if (keysLocked.get()) {
            throw new IllegalStateException("map() cannot be called at this point");
        }
        final KeyImpl<E> key = new KeyImpl<E>(selectList);
        keys.add(key);
        return key;
    }


    private class KeyImpl<E> implements RowMapper<E> {
        private final AbstractSelectList<E> selectList;
        private RowMapper<E> rowMapper;

        public KeyImpl(final SelectList<E> selectList) {
            this.selectList = new AbstractSelectList<E>() {
                @Override
                public QueryBuilder<E> z$sqlOfSelectList(final SqlContext context) {
                    final QueryBuilder<E> query = selectList.z$sqlOfSelectList(context);
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

    private class InnerQueryBuilder implements QueryBuilder<D> {
        private final QueryBuilder<?> query;

        public InnerQueryBuilder(final QueryBuilder<?> query) {
            this.query = query;
        }

        @Override
        public D extract(final Row row) throws SQLException {
            return create(row);
        }

        @Override
        public void appendTo(final StringBuilder builder) {
            query.appendTo(builder);
        }

        @Override
        public void setParameters(final SqlParameters sqlParameters) throws SQLException {
            query.setParameters(sqlParameters);
        }

        @Override
        public char firstChar() {
            return query.firstChar();
        }
    }
}
