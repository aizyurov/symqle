package org.symqle.jdbc;

import org.symqle.common.CompiledSql;

import java.sql.SQLException;
import java.util.List;

/**
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
     * @throws SQLException
     */
    int[] submit(CompiledSql sql, List<Option> options) throws SQLException;

    /**
     * flushes all pending updates to database
     * @return same as {@link java.sql.PreparedStatement#executeBatch()},
     * empty array if queue was empty.
     * @throws java.sql.SQLException from jdbc driver
     */
    int[] flush() throws SQLException;

    Engine getEngine();

}
