package org.symqle.jdbc;

import org.symqle.common.CompiledSql;
import org.symqle.common.SqlBuilder;

import java.sql.SQLException;
import java.util.List;

/**
 * @author lvovich
 */
public class PreparedUpdate {

    private final Engine engine;
    private final CompiledSql update;
    private final List<Option> options;
    private final GeneratedKeys<?> generatedKeys;

    public PreparedUpdate(final Engine engine, final SqlBuilder update, final List<Option> options, final GeneratedKeys<?> generatedKeys) {
        this.engine = engine;
        final StringBuilder sourceSql = new StringBuilder();
        update.appendTo(sourceSql);

        this.update = new CompiledSql(update);
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
