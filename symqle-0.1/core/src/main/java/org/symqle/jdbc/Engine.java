/* THIS IS GENERATED CODE. ALL CHANGES WILL BE LOST
   See Engine.sdl:20 */

package org.symqle.jdbc;

import org.symqle.common.Sql;
import org.symqle.sql.ColumnName;

import java.sql.SQLException;


public interface Engine extends QueryEngine {


    /**
     * Executes a sql.
     * @param sql the SQL to execute
     * @param options statement options
     * @return number of affected rows
     * @throws SQLException from jdbc driver
     */
    int execute(Sql sql, Option... options) throws SQLException;

    /**
     * Executes an sql (insert) and returns generated key.
     * @param sql the SQL to execute
     * @param keyColumn the column, for which key is generated
     * @param options  statement options
     * @param <T> type of generated key
     * @return value of generated key
     * @throws SQLException from jdbc driver
     */
    <T> T executeReturnKey(Sql sql, ColumnName<T> keyColumn, Option... options) throws SQLException;

    /**
     * flushes all pending updates to database
     * @return if non-negative, number of affected rows. Else
     * {@link java.sql.Statement#SUCCESS_NO_INFO} or {@link java.sql.Statement#EXECUTE_FAILED}
     * {@link #NOTHING_FLUSHED} if queue was empty.
     * @throws SQLException from jdbc driver
     */
    int flush() throws SQLException;


    /**
     * Submits an sql statement for execution.
     * May cause flush of pending updates at its discretion,
     * for example if number of pending updates exceeds batch size.
     * @param sql the SQL to submit
     * @param options statement options
     * @return number of affected rows,{@link java.sql.Statement#SUCCESS_NO_INFO}
     * or {@link java.sql.Statement#EXECUTE_FAILED} if flush occured.
     * {@link #NOTHING_FLUSHED} if nothing flushed.
     * @throws SQLException
     */
    int submit(Sql sql, Option... options) throws SQLException;

    /**
     * Current max batch size.
     * @return batch size
     */
    int getBatchSize();

    /**
     * Sets new batch size. If new batch size is less than number of pending updates,
     * flush is called first.
     * @param batchSize new maximum batch size
     * @return same as {@link #flush()}
     * @throws SQLException from jdbc driver
     */
    int setBatchSize(int batchSize) throws SQLException;

    int NOTHING_FLUSHED = -100;

}
