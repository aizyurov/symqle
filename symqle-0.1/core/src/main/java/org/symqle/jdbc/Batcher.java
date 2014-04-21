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

import org.symqle.common.Sql;

import java.sql.SQLException;
import java.util.List;

/**
 * Performs batch updates of database data.
 * @author lvovich
 */
public interface Batcher {

    /**
     * Submits an sql statement for execution.
     * May cause flush of pending updates at its discretion,
     * for example if number of pending updates exceeds batch size limit.
     * @param sql the SQL to submit
     * @param options statement options
     * @return same as {@link java.sql.PreparedStatement#executeBatch()}
     * empty array if nothing flushed.
     * @throws SQLException attempt of flush was unsuccessful.
     */
    int[] submit(Sql sql, List<Option> options) throws SQLException;

    /**
     * flushes all pending updates to database
     * @return same as {@link java.sql.PreparedStatement#executeBatch()},
     * empty array if queue was empty.
     * @throws java.sql.SQLException from jdbc driver
     */
    int[] flush() throws SQLException;

    /**
     * Engine, which executes batch updates.
     * @return associated Engine.
     */
    Engine getEngine();

}
