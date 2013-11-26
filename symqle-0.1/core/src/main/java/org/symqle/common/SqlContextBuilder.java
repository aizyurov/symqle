package org.symqle.common;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lvovich
 */
public class SqlContextBuilder {

    private final Map<Class<?>, Object> theContext = new HashMap<Class<?>, Object>();

    public SqlContextBuilder() {
    }

    public SqlContextBuilder(SqlContext source) {

    }

}
