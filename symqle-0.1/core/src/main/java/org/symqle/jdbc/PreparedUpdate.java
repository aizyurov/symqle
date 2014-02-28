package org.symqle.jdbc;

import org.symqle.common.Sql;
import org.symqle.common.SqlParameters;
import org.symqle.querybuilder.SqlFormatter;

import java.sql.SQLException;
import java.util.List;

/**
 * @author lvovich
 */
public class PreparedUpdate {

    private final Engine engine;
    private final Sql update;
    private final List<Option> options;
    private final GeneratedKeys<?> generatedKeys;

    public PreparedUpdate(final Engine engine, final Sql update, final List<Option> options, final GeneratedKeys<?> generatedKeys) {
        this.engine = engine;
        this.update = new Sql() {
            private final String text = SqlFormatter.formatText(update);

            @Override
            public String toString() {
                return text;
            }

            @Override
            public void appendTo(final StringBuilder builder) {
                builder.append(text);
            }

            @Override
            public void setParameters(final SqlParameters p) throws SQLException {
                update.setParameters(p);
            }
        };
        this.options = options;
        this.generatedKeys = generatedKeys;
    }

    public int execute() throws SQLException {
        return engine.execute(update, generatedKeys, options);
    }

    public int[] submit(final Batcher batcher) throws SQLException {
        if (batcher.getEngine() != engine) {
            throw new IllegalArgumentException("Incompatible batcher");
        }
        return batcher.submit(update, options);
    }
}
