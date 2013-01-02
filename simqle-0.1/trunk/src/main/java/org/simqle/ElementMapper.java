package org.simqle;

import java.sql.SQLException;

/**
 * Extracts value from an Element.
 * @author lvovich
 * @param <T> the type of extracted value
 */
public interface ElementMapper<T> {
    /**
     * Extracts value from an Element.
     * @param element the element to extract value from
     * @return the value
     */
    T value(Element element) throws SQLException;
    
    void setValue(SqlParameter param, T value) throws SQLException;
}
