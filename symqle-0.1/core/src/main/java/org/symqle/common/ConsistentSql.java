package org.symqle.common;

/**
 * @author lvovich
 */
public abstract class ConsistentSql implements Sql {

    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        appendTo(builder);
        return builder.toString();
    }
}
