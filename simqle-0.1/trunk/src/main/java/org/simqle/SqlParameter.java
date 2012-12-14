/*
* Copyright Alexander Izyurov 2010
*/
package org.simqle;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

/**
  * An abstraction of parameters of {@link java.sql.PreparedStatement}. It isolates
  * the supplier of parameter value from other PreparedStatement methods and knowledge of parameter position
  * The methods for setting parameter value are basically the same as those of PreparedStatement
  * For convenience, primitive type arguments have been replaced with object type wrappers and
  * setNull omitted. setXxx(null) calls setNull on the underlying PreparedStatement.
 * Each setXxx method sets parameter for current position and advances the position index.
 * Initial position is 1.
 *
 * @author Alexander Izyurov
 * TODO addSort more supported types (Blob etc.)
 */
public interface SqlParameter {

    /**
     * Sets Boolean value to parameter.
     * @param x the value, null is OK
     */
    void setBoolean(Boolean x) throws SQLException;

    /**
     * Sets Byte value to parameter.
     * @param x the value, null is OK
     */
    void setByte(Byte x) throws SQLException;

    /**
     * Sets Short value to parameter.
     * @param x the value, null is OK
     */
    void setShort(Short x) throws SQLException;

    /**
     * Sets Integer value to parameter.
     * @param x the value, null is OK
     */
    void setInt(Integer x) throws SQLException;

    /**
     * Sets Long value to parameter.
     * @param x the value, null is OK
     */
    void setLong(Long x) throws SQLException;

    /**
     * Sets Float value to parameter.
     * @param x the value, null is OK
     */
    void setFloat(Float x) throws SQLException;

    /**
     * Sets Double value to parameter.
     * @param x the value, null is OK
     */
    void setDouble(Double x) throws SQLException;

    /**
     * Sets BigDecimal value to parameter.
     * @param x the value, null is OK
     */
    void setBigDecimal(BigDecimal x) throws SQLException;

    /**
     * Sets String value to parameter.
     * @param x the value, null is OK
     */
    void setString(String x) throws SQLException;

    /**
     * Sets Date value to parameter.
     * @param x the value, null is OK
     */
    void setDate(Date x) throws SQLException;

    /**
     * Sets Time value to parameter.
     * @param x the value, null is OK
     */
    void setTime(Time x) throws SQLException;

    /**
     * Sets Timestamp value to parameter.
     * @param x the value, null is OK
     */
    void setTimestamp(Timestamp x) throws SQLException;

}
