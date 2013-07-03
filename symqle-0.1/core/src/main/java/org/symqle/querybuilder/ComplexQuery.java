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

package org.symqle.querybuilder;

import org.symqle.common.Query;
import org.symqle.common.Row;
import org.symqle.common.RowMapper;
import org.symqle.common.Sql;
import org.symqle.common.SqlParameters;

import java.sql.SQLException;

/**
 * A query, which is constructed from a RowMapper and Sql.
 * It delegates its methods to the constructor arguments.
 * @param <T> type of objects created by row mapper.
 */
public class ComplexQuery<T> implements Query<T> {
    private final RowMapper<T> extractor;
    private final Sql sql;

    /**
     * Constructs from components.
     * @param rowMapper RowMapper to use for {@link #extract(org.symqle.common.Row)}
     * @param sql provides {@link #getSqlText()} and {@link #setParameters(org.symqle.common.SqlParameters)}
     */
    public ComplexQuery(final RowMapper<T> rowMapper, final Sql sql) {
        this.extractor = rowMapper;
        this.sql = sql;
    }

    @Override
    public T extract(final Row row) throws SQLException {
        return extractor.extract(row);
    }

    @Override
    public String getSqlText() {
        return sql.getSqlText();
    }

    @Override
    public void setParameters(final SqlParameters p) throws SQLException {
        sql.setParameters(p);
    }
}
