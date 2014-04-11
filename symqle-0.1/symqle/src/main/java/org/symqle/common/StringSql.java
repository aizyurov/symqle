package org.symqle.common;

import java.sql.SQLException;

/**
 * @author lvovich
 */
public class StringSql implements Sql {

    private final String text;

    public StringSql(final String text) {
        this.text = text;
    }

    @Override
    public String text() {
        return text;
    }

    @Override
    public void setParameters(final SqlParameters p) throws SQLException {
        // do nothing
    }
}
