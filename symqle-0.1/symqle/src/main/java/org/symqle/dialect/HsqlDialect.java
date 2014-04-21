package org.symqle.dialect;

import org.symqle.sql.GenericDialect;

/**
 * @author lvovich
 */
public class HsqlDialect extends GenericDialect {

    public HsqlDialect() {
        throw new IllegalStateException(getName() + " is not supported by Symqle");
    }

    @Override
    public String getName() {
        return "HSQL Database Engine";
    }
}
