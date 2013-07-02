package org.symqle.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * An option, which can be applied to a Statement, e.g. fetch size, query timeout etc.
 * @author lvovich
 */
public abstract class Option {
    public abstract void apply(Statement statement) throws SQLException;
    public abstract void apply(UpdatableConfiguration configuration);

    private abstract static class StatementOption extends Option {
        @Override
        public void apply(UpdatableConfiguration configuration) {
            // do nothing
        }
    }

    private abstract static class ConfigurationOption extends Option {
        @Override
        public void apply(Statement statement) throws SQLException {
            // do nothing
        }
    }

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

    public static ConfigurationOption allowNoTables(final boolean allow) {
        return new ConfigurationOption() {
            @Override
            public void apply(UpdatableConfiguration configuration) {
                configuration.setNoFromOk(allow);
            }
        };
    }

    public static ConfigurationOption allowImplicitCrossJoins(final boolean allow) {
        return new ConfigurationOption() {
            @Override
            public void apply(UpdatableConfiguration configuration) {
                configuration.setImplicitCrossJoinsOk(allow);
            }
        };
    }
}
