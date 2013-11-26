package org.symqle.common;

/**
 * @author lvovich
 */
public class StringSqlBuilder implements SqlBuilder {

    private StringBuilder stringBuilder = new StringBuilder();

    @Override
    public void append(final String a) {
        stringBuilder.append(a);
    }

    @Override
    public void append(final char c) {
        stringBuilder.append(c);
    }

    public String toString() {
        return stringBuilder.toString();
    }
}
