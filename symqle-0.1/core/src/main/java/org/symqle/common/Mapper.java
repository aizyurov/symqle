package org.symqle.common;

import org.symqle.common.Element;
import org.symqle.common.SqlParameter;

import java.sql.SQLException;

/**
 * Extracts value from an Element.
 * @author lvovich
 * @param <T> the type of extracted value
 */
public interface Mapper<T> {
    /**
     * Extracts value from an Element.
     * @param element the element to extract value from
     * @return the value
     */
    T value(Element element) throws SQLException;
    
    void setValue(SqlParameter param, T value) throws SQLException;
}
