/* THIS IS GENERATED CODE. ALL CHANGES WILL BE LOST
   See Engine.sdl:6 */

package org.symqle.jdbc;

import org.symqle.common.Callback;
import org.symqle.common.Row;
import org.symqle.common.Sql;
import org.symqle.sql.Dialect;

import java.sql.SQLException;
import java.util.List;


public interface QueryEngine  {

    Dialect getDialect();

    List<Option> getOptions();

    int scroll(Sql query, Callback<Row> callback, List<Option> options) throws SQLException;

    String getDatabaseName();
}
