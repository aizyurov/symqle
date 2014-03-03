/* THIS IS GENERATED CODE. ALL CHANGES WILL BE LOST
   See Engine.sdl:20 */

package org.symqle.jdbc;

import org.symqle.common.CompiledSql;

import java.sql.SQLException;
import java.util.List;


public interface Engine extends QueryEngine {

    /**
     * Executes a sql.
     * @param sql the SQL to execute
     * @param options statement options
     * @return number of affected rows
     * @throws SQLException from jdbc driver
     */
    int execute(CompiledSql sql, List<Option> options) throws SQLException;

    /**
     * Executes an sql and reads generated keys.
     * @param sql the SQL to execute
     * @param keys generated keys holder
     * @param options statement options
     * @return number of affected rows
     * @throws SQLException from jdbc driver
     */
    int execute(CompiledSql sql, GeneratedKeys<?> keys, List<Option> options) throws SQLException;

    /**
     * Creates a new Batcher.
     * The batcher inherits dialect and options from {@code this}
     * @param batchSizeLimit max number of pending statements in the batcher queue.
     * @return new Batcher.
     */
    Batcher newBatcher(int batchSizeLimit);

}
