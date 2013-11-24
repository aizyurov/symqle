/* THIS IS GENERATED CODE. ALL CHANGES WILL BE LOST
   See Engine.sdl:6 */

package org.symqle.jdbc;

import org.symqle.common.*;

import java.sql.SQLException;


public interface QueryEngine  {


    SqlContext initialContext();

    int scroll(Sql query, Callback<Row> callback, Option... options) throws SQLException;

    String getDatabaseName();
}
