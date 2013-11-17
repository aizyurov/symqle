/*
   Copyright 2010-2013 Alexander Izyurov

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.package org.symqle.common;
*/

package org.symqle.jdbc;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * An option, which can be applied to a Statement, e.g. fetch size, query timeout etc.
 * or to Configuration.
 * Static methods of this class provide available options.
 */
public abstract class Option {
    public abstract void apply(Statement statement) throws SQLException;
    public abstract void apply(UpdatableConfiguration configuration);

    /**
     * Subclasses should implement {@link #apply(java.sql.Statement)}
     */
    private abstract static class StatementOption extends Option {
        @Override
        public void apply(final UpdatableConfiguration configuration) {
            // do nothing
        }
    }

    /**
     * Subclasses should implement {@link #apply(UpdatableConfiguration)}
     */
    private abstract static class ConfigurationOption extends Option {
        @Override
        public void apply(final Statement statement) throws SQLException {
            // do nothing
        }
    }

    /**
     * An option to set fetch direction for result set returned by a query.
     * See {@link Statement#setFetchDirection(int)}
     * @param direction one of <code>ResultSet.FETCH_FORWARD</code>,
          * <code>ResultSet.FETCH_REVERSE</code>, or <code>ResultSet.FETCH_UNKNOWN</code>
     * @return new option
     */
    public static StatementOption setFetchDirection(final int direction) {
        return new FetchDirectionOption(direction);
    }

    /**
     * An option to set fetch size for result set returned by a query.
     * See {@link Statement#setFetchSize(int)}
     * @param rows the size
     * @return new option
     */
    public static StatementOption setFetchSize(final int rows) {
        return new FetchSizeOption(rows);
    }

    /**
     * An option to set max field size in the result set returned by a query.
     * See {@link Statement#setMaxFieldSize(int)}
     * @param max field size limit
     * @return new option
     */
    public static StatementOption setMaxFieldSize(final int max) {
        return new MaxFieldSizeOption(max);
    }


    /**
     * An option to set max number of returned rows in the result set returned by a query.
     * See {@link Statement#setMaxRows(int)}
     * @param max row number limit
     * @return new option
     */
    public static StatementOption setMaxRows(final int max) {
        return new MaxRowsOption(max);
    }

    /**
     * An option to set query timeout for a statement.
     * See {@link Statement#setQueryTimeout(int)}
     * @param seconds the timeout to set
     * @return new option
     */
    public static StatementOption setQueryTimeout(final int seconds) {
        return new QueryTimeoutOption(seconds);
    }

    /**
     * Determines the behavior of query builder if there are not tables in
     * query/subquery context. If {@code allow} is false, the builder will
     * throw an exception. If it is true, the builder will do its best to construct
     * valid query/subquery, either by omitting FROM clause (if it is allowed by current dialect)
     * or by constructing appropriate FROM clause (like "FROM dual" for Oracle dialect).
     * If unable to construct valid SQL for current dialect, throws an exception.
     * @param allow true of no tables is fine
     * @return new option
     */
    public static ConfigurationOption allowNoTables(final boolean allow) {
        return new NoTablesOption(allow);
    }

    /**
     * Determines the behavior of query builder if implicit cross join
     * is constructed ("SELECT T1.a, T2.b FROM T1, T2..."). It does not always mean
     * retrieving full cross product if there is WHERE clause, but this style is unsafe and using joins
     * is better.
     * Default setting for query builder is to prohibit implicit cross joins and throw an exception.
     * @param allow true if implicit cross joins are OK
     * @return new options
     */
    public static ConfigurationOption allowImplicitCrossJoins(final boolean allow) {
        return new ImplicitCrossJoinsOption(allow);
    }

    private static class FetchDirectionOption extends StatementOption {
        private final int direction;

        public FetchDirectionOption(final int direction) {
            this.direction = direction;
        }

        @Override
        public void apply(final Statement statement) throws SQLException {
            statement.setFetchDirection(direction);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final FetchDirectionOption that = (FetchDirectionOption) o;

            if (direction != that.direction) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return direction;
        }
    }

    private static class FetchSizeOption extends StatementOption {
        private final int rows;

        public FetchSizeOption(final int rows) {
            this.rows = rows;
        }

        @Override
        public void apply(final Statement statement) throws SQLException {
            statement.setFetchSize(rows);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final FetchSizeOption that = (FetchSizeOption) o;

            if (rows != that.rows) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return rows;
        }
    }

    private static class MaxFieldSizeOption extends StatementOption {
        private final int max;

        public MaxFieldSizeOption(final int max) {
            this.max = max;
        }

        @Override
        public void apply(final Statement statement) throws SQLException {
            statement.setMaxFieldSize(max);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final MaxFieldSizeOption that = (MaxFieldSizeOption) o;

            if (max != that.max) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return max;
        }
    }

    private static class MaxRowsOption extends StatementOption {
        private final int max;

        public MaxRowsOption(final int max) {
            this.max = max;
        }

        @Override
        public void apply(final Statement statement) throws SQLException {
            statement.setMaxRows(max);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final MaxRowsOption that = (MaxRowsOption) o;

            if (max != that.max) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return max;
        }
    }

    private static class QueryTimeoutOption extends StatementOption {
        private final int seconds;

        public QueryTimeoutOption(final int seconds) {
            this.seconds = seconds;
        }

        @Override
        public void apply(final Statement statement) throws SQLException {
            statement.setQueryTimeout(seconds);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final QueryTimeoutOption that = (QueryTimeoutOption) o;

            if (seconds != that.seconds) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return seconds;
        }
    }

    private static class NoTablesOption extends ConfigurationOption {
        private final boolean allow;

        public NoTablesOption(final boolean allow) {
            this.allow = allow;
        }

        @Override
        public void apply(final UpdatableConfiguration configuration) {
            configuration.setNoFromOk(allow);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final NoTablesOption that = (NoTablesOption) o;

            if (allow != that.allow) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return (allow ? 1 : 0);
        }
    }

    private static class ImplicitCrossJoinsOption extends ConfigurationOption {
        private final boolean allow;

        public ImplicitCrossJoinsOption(final boolean allow) {
            this.allow = allow;
        }

        @Override
        public void apply(final UpdatableConfiguration configuration) {
            configuration.setImplicitCrossJoinsOk(allow);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final ImplicitCrossJoinsOption that = (ImplicitCrossJoinsOption) o;

            if (allow != that.allow) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return (allow ? 1 : 0);
        }
    }
}
