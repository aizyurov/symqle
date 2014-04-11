package org.symqle.common;

import java.sql.SQLException;

/**
 * @author lvovich
 */
public class CompiledSql implements Parameterizer {

    private String text;
    private Parameterizer parameterizer;

    public CompiledSql(final SqlBuilder source) {
        final StringBuilder stringBuilder = new StringBuilder();
        source.appendTo(stringBuilder);
        this.text = stringBuilder.toString();
        this.parameterizer = source;
    }

    public String text() {
        return text;
    }

    @Override
    public void setParameters(final SqlParameters p) throws SQLException {
        parameterizer.setParameters(p);
    }
}
