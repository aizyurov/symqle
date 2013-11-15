/* THIS IS GENERATED CODE. ALL CHANGES WILL BE LOST
   See Engine.sdl:6 */

package org.symqle.jdbc;

import org.symqle.common.Callback;
import org.symqle.common.Query;
import org.symqle.common.SqlContext;

import java.sql.SQLException;


public interface QueryEngine  {


    SqlContext initialContext();


    <T> int scroll(Query<T> query, Callback<T> callback, Option... options) throws SQLException;

}
