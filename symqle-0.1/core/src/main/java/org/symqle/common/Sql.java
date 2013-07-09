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

package org.symqle.common;

import java.sql.SQLException;

/**
 * This interface represents text of a syntax element of SQL language, which may contain dynamic parameters.
 * The interface also provides values for the parameters.
 * T
 */
public interface Sql {

    /**
     * The text of this Sql, may contain dynamic parameters (?).
      * @return the text
     */
    String sql();

    /**
     * Provide values for dynamic parameters.
     * @param p SqlParameters to write parameter values into
     * @throws SQLException if jdbc driver cannot set parameters
     */
    void setParameters(SqlParameters p) throws SQLException;

}
