package org.symqle.sql;

import org.symqle.common.Sql;
import org.symqle.common.SqlParameters;

import java.sql.SQLException;

/**
 * An implementation of Sql, which accepts any String as its text.
 * May be used for ad-hoc statements, which are not supported by Symqle classes,
 * e.g. DDL statements.
 * <p/>
 * If the text contains parameter placeholders, override setParameters properly.
 * @author lvovich
 */
public class StringSql implements Sql {

    private final String text;

    /**
     * Constructs from any text.
     * @param text the SQL text, will be used as is.
     */
    public StringSql(final String text) {
        this.text = text;
    }

    @Override
    public final String text() {
        return text;
    }

    @Override
    public void setParameters(final SqlParameters p) throws SQLException {
        // do nothing
    }
}
