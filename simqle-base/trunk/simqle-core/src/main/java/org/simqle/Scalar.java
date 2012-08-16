/*
* Copyright Alexander Izyurov 2010
*/
package org.simqle;

import java.sql.SQLException;

/**
 * <br/>05.12.2010
 *
 * @author Alexander Izyurov
 */
public interface Scalar<T> {
    T value(Element element) throws SQLException;

    Scalar<Boolean> BOOLEAN_SCALAR = new Scalar<Boolean>() {
        public Boolean value(final Element element) throws SQLException {
            return element.getBoolean();
        }
    };

    Scalar<String> STRING_SCALAR = new Scalar<String>() {
        public String value(Element element) throws SQLException {
            return element.getString();
        }
    };
}
