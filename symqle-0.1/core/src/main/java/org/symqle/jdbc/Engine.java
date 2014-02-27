/* THIS IS GENERATED CODE. ALL CHANGES WILL BE LOST
   See Engine.sdl:20 */

package org.symqle.jdbc;

import org.symqle.common.Sql;
import org.symqle.sql.ColumnName;

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
    int execute(Sql sql, List<Option> options) throws SQLException;

    /**
     * Executes an sql (insert) and returns generated key.
     * @param sql the SQL to execute
     * @param keyColumn the column, for which key is generated
     * @param options  statement options
     * @param <T> type of generated key
     * @return value of generated key
     * @throws SQLException from jdbc driver
     */
    <T> T executeReturnKey(Sql sql, ColumnName<T> keyColumn, List<Option> options) throws SQLException;

    /**
     * Creates a new Batcher.
     * The batcher inherits dialect and options from {@code this}
     * @param batchSizeLimit max number of pending statements in the batcher queue.
     * @return new Batcher.
     */
    Batcher newBatcher(int batchSizeLimit);

}
