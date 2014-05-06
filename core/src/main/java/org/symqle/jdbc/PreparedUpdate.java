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

import org.symqle.common.CompiledSql;
import org.symqle.common.Sql;
import org.symqle.common.SqlBuilder;

import java.sql.SQLException;
import java.util.List;

/**
 * Compile and prepared for execution update statement.
 * @author lvovich
 */
public class PreparedUpdate {

    private final Engine engine;
    private final Sql update;
    private final List<Option> options;
    private final GeneratedKeys<?> generatedKeys;

    /**
     * Constructs PreparedUpdate.
     * @param engine the engine to use for execution
     * @param update sql builder to provide source
     * @param options options to use when executing
     * @param generatedKeys generated keys holder. May be null if generated keys are not needed.
     */
    public PreparedUpdate(final Engine engine,
                          final SqlBuilder update,
                          final List<Option> options,
                          final GeneratedKeys<?> generatedKeys) {
        this.engine = engine;
        final StringBuilder sourceSql = new StringBuilder();
        update.appendTo(sourceSql);

        this.update = new CompiledSql(update);
        this.options = options;
        this.generatedKeys = generatedKeys;
    }

    /**
     * Executes {@code this}.
     * @return number of affected rows
     * @throws SQLException from JDBC driver
     */
    public final int execute() throws SQLException {
        return engine.execute(update, generatedKeys, options);
    }

    /**
     * Submits {@code this} to a batcher.
     * @param batcher the batcher to use. should be associated with the same Engine.
     * @return same as {@link Batcher#submit(org.symqle.common.Sql, java.util.List)}
     * @throws SQLException if attempt of flush was unsuccessful
     */
    public final int[] submit(final Batcher batcher) throws SQLException {
        if (batcher.getEngine() != engine) {
            throw new IllegalArgumentException("Incompatible batcher");
        }
        return batcher.submit(update, options);
    }
}
