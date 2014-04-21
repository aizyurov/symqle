/* THIS IS GENERATED CODE. ALL CHANGES WILL BE LOST
   See Engine.sdl:6 */

package org.symqle.jdbc;

import org.symqle.common.Callback;
import org.symqle.common.Sql;
import org.symqle.common.Row;
import org.symqle.sql.Dialect;

import java.sql.SQLException;
import java.util.List;


/**
 * Symqle gate to JDBC for queries.
 */
public interface QueryEngine  {

    /**
     * The dialect, which is used by this engine.
     * @return the dialect
     */
    Dialect getDialect();

    /**
     * Options to use by default for each statement.
     * They may be overridden in {@link #scroll(org.symqle.common.Sql, org.symqle.common.Callback, java.util.List)}}
     * @return default options.
     */
    List<Option> getOptions();

    /**
     * Execute a query and process each row.
     * @param query the query to execute
     * @param callback called for each row until result set end of callback returns false
     * @param options local options, override defaults.
     * @return number of processed rows
     * @throws SQLException from JDBC driver
     */
    int scroll(Sql query, Callback<Row> callback, List<Option> options) throws SQLException;

    /**
     * Database name as reported by {@link java.sql.DatabaseMetaData#getDatabaseProductName()}
     * @return the database name
     */
    String getDatabaseName();
}
