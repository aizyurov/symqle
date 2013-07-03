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
 * An abstraction of a single column in a single row of result set. It isolates the value consumer from
 * details of position or label of the column in the row and from scrolling over result set.
 * Unlike {@link java.sql.ResultSet}},
 * its methods return object wrappers rather than primitive values.
 * @author Alexander Izyurov
 */
public interface Element {
    // TODO add more methods (Blob etc.)
    /**
     * gets the value of Element as Boolean.
     * @return the value; <code>null</code> if the column value was SQL <code>NULL</code>
     * @throws SQLException the value is not convertible to Boolean
     */
    Boolean getBoolean() throws SQLException;

    /**
     * gets the value of Element as Byte.
     * @return the value; <code>null</code> if the column value was SQL <code>NULL</code>
     * @throws SQLException the value is not convertible to Byte
     */
    Byte getByte() throws SQLException;

    /**
     * gets the value of Element as Short.
     * @return the value; <code>null</code> if the column value was SQL <code>NULL</code>
     * @throws SQLException the value is not convertible to Short
     */
    Short getShort() throws SQLException;

    /**
     * gets the value of Element as Integer.
     * @return the value; <code>null</code> if the column value was SQL <code>NULL</code>
     * @throws SQLException the value is not convertible to Integer
     */
    Integer getInt() throws SQLException;

    /**
     * gets the value of Element as Long.
     * @return the value; <code>null</code> if the column value was SQL <code>NULL</code>
     * @throws SQLException the value is not convertible to Long
     */
    Long getLong() throws SQLException;

    /**
     * gets the value of Element as Float.
     * @return the value; <code>null</code> if the column value was SQL <code>NULL</code>
     * @throws SQLException the value is not convertible to Float
     */
    Float getFloat() throws SQLException;

    /**
     * gets the value of Element as Double.
     * @return the value; <code>null</code> if the column value was SQL <code>NULL</code>
     * @throws SQLException the value is not convertible to Double
     */
    Double getDouble() throws SQLException;

    /**
     * gets the value of Element as BigDecimal.
     * @return the value; <code>null</code> if the column value was SQL <code>NULL</code>
     * @throws SQLException the value is not convertible to BigDecimal
     */
    BigDecimal getBigDecimal() throws SQLException;

    /**
     * gets the value of Element as String.
     * @return the value; <code>null</code> if the column value was SQL <code>NULL</code>
     * @throws SQLException the value is not convertible to String
     */
    String getString() throws SQLException;

    /**
     * gets the value of Element as Date.
     * @return the value; <code>null</code> if the column value was SQL <code>NULL</code>
     * @throws SQLException the value is not convertible to Date
     */
    Date getDate() throws SQLException;

    /**
     * gets the value of Element as Time.
     * @return the value; <code>null</code> if the column value was SQL <code>NULL</code>
     * @throws SQLException the value is not convertible to Time
     */
    Time getTime() throws SQLException;

    /**
     * gets the value of Element as Timestamp.
     * @return the value; <code>null</code> if the column value was SQL <code>NULL</code>
     * @throws SQLException the value is not convertible to Timestamp
     */
    Timestamp getTimestamp() throws SQLException;

}
