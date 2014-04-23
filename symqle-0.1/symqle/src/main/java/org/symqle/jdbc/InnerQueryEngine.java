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

package org.symqle.jdbc;

import org.symqle.common.Callback;
import org.symqle.common.Sql;
import org.symqle.common.Row;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Implementation of QueryEngine, which re-uses existing connection.
 * @author lvovich
 */
class InnerQueryEngine extends AbstractQueryEngine {

    private final Connection connection;

    /**
     * Constructs from existing QueryEngine and connection.
     * Dialect, database name and default options are copied from parent.
     * @param parent provides dialect etc.
     * @param connection existing open connection
     */
    public InnerQueryEngine(final AbstractQueryEngine parent, final Connection connection) {
        super(parent);
        this.connection = connection;
    }

    @Override
    public int scroll(final Sql query, final Callback<Row> callback, final List<Option> options) throws SQLException {
        return scroll(connection, query, callback, options);
    }
}
