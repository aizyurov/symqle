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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * An object, whcih can convert one of predefined java types to its internal representation.
  * Example implementation is a parameter of {@link java.sql.PreparedStatement}. The abstraction isolates
  * the supplier of parameter value from other PreparedStatement methods and knowledge of parameter position
  * The methods for setting parameter value are basically the same as those of PreparedStatement
  * For convenience, primitive type arguments have been replaced with object type wrappers and
  * setNull omitted. setXxx(null) can be used instead..
 */
public interface OutBox {
    // TODO add more supported types (Blob etc.)

    /**
     * Sets Boolean value to parameter.
     * @param x the value, null is OK
     * @throws SQLException if a database access error occurs
     */
    void setBoolean(Boolean x) throws SQLException;

    /**
     * Sets Byte value to parameter.
     * @param x the value, null is OK
     * @throws SQLException if a database access error occurs
     */
    void setByte(Byte x) throws SQLException;

    /**
     * Sets Short value to parameter.
     * @param x the value, null is OK
     * @throws SQLException if a database access error occurs
     */
    void setShort(Short x) throws SQLException;

    /**
     * Sets Integer value to parameter.
     * @param x the value, null is OK
     * @throws SQLException if a database access error occurs
     */
    void setInt(Integer x) throws SQLException;

    /**
     * Sets Long value to parameter.
     * @param x the value, null is OK
     * @throws SQLException if a database access error occurs
     */
    void setLong(Long x) throws SQLException;

    /**
     * Sets Float value to parameter.
     * @param x the value, null is OK
     * @throws SQLException if a database access error occurs
     */
    void setFloat(Float x) throws SQLException;

    /**
     * Sets Double value to parameter.
     * @param x the value, null is OK
     * @throws SQLException if a database access error occurs
     */
    void setDouble(Double x) throws SQLException;

    /**
     * Sets BigDecimal value to parameter.
     * @param x the value, null is OK
     * @throws SQLException if a database access error occurs
     */
    void setBigDecimal(BigDecimal x) throws SQLException;

    /**
     * Sets String value to parameter.
     * @param x the value, null is OK
     * @throws SQLException if a database access error occurs
     */
    void setString(String x) throws SQLException;

    /**
     * Sets Date value to parameter.
     * @param x the value, null is OK
     * @throws SQLException if a database access error occurs
     */
    void setDate(Date x) throws SQLException;

    /**
     * Sets Time value to parameter.
     * @param x the value, null is OK
     * @throws SQLException if a database access error occurs
     */
    void setTime(Time x) throws SQLException;

    /**
     * Sets Timestamp value to parameter.
     * @param x the value, null is OK
     * @throws SQLException if a database access error occurs
     */
    void setTimestamp(Timestamp x) throws SQLException;

    /**
     * Sets byte array value to parameter.
     * @param x the value, null is OK
     * @throws SQLException if a database access error occurs
     */
    void setBytes(byte[] x) throws SQLException;

}
