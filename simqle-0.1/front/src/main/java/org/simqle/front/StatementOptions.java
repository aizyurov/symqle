package org.simqle.front;

import org.simqle.jdbc.StatementOption;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author lvovich
 */
public class StatementOptions {

    private StatementOptions() {}

    static { new StatementOptions(); }

    public static StatementOption setFetchDirection(final int direction) {
        return new StatementOption() {
            @Override
            public void apply(final Statement statement) throws SQLException {
                statement.setFetchDirection(direction);
            }
        };
    }

    public static StatementOption setFetchSize(final int rows) {
        return new StatementOption() {
            @Override
            public void apply(final Statement statement) throws SQLException {
                statement.setFetchSize(rows);
            }
        };
    }

    public static StatementOption setMaxFieldSize(final int max) {
        return new StatementOption() {
            @Override
            public void apply(final Statement statement) throws SQLException {
                statement.setMaxFieldSize(max);
            }
        };
    }

    public static StatementOption setMaxRows(final int max) {
        return new StatementOption() {
            @Override
            public void apply(final Statement statement) throws SQLException {
                statement.setMaxRows(max);
            }
        };
    }

    public static StatementOption setQueryTimeout(final int seconds) {
        return new StatementOption() {
            @Override
            public void apply(final Statement statement) throws SQLException {
                statement.setQueryTimeout(seconds);
            }
        };
    }

}
