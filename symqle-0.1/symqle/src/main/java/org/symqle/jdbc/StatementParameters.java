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

import org.symqle.common.OutBox;
import org.symqle.common.SqlParameters;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * An implementation of SqlParameters, which is a proxy to a PreparedStatement.
 */
public class StatementParameters implements SqlParameters {
    private final PreparedStatement statement;
    private int position;

    /**
     * Constructs from a PreparedStatement
     * @param statement the statement to proxy
     */
    public StatementParameters(final PreparedStatement statement) {
        this.statement = statement;
    }

    @Override
    public final OutBox next() {
        return new StatementParameter(++position);
    }

    private class StatementParameter implements OutBox {
        private final int position;

        private StatementParameter(final int position) {
            this.position = position;
        }

        @Override
        public void setBoolean(final Boolean x) throws SQLException {
            if (x == null) {
                statement.setNull(position, Types.BOOLEAN);
            } else {
                statement.setBoolean(position, x);
            }
        }

        @Override
        public void setByte(final Byte x) throws SQLException {
            if (x == null) {
                statement.setNull(position, Types.TINYINT);
            } else {
                statement.setByte(position, x);
            }
        }

        @Override
        public void setShort(final Short x) throws SQLException {
            if (x == null) {
                statement.setNull(position, Types.SMALLINT);
            } else {
                statement.setShort(position, x);
            }
        }

        @Override
        public void setInt(final Integer x) throws SQLException {
            if (x == null) {
                statement.setNull(position, Types.INTEGER);
            } else {
                    statement.setInt(position, x);
            }
        }

        @Override
        public void setLong(final Long x) throws SQLException {
            if (x == null) {
                statement.setNull(position, Types.BIGINT);
            } else {
                    statement.setLong(position, x);
            }
        }

        @Override
        public void setFloat(final Float x) throws SQLException {
            if (x == null) {
                statement.setNull(position, Types.FLOAT);
            } else {
                    statement.setFloat(position, x);
            }
        }

        @Override
        public void setDouble(final Double x) throws SQLException {
            if (x == null) {
                statement.setNull(position, Types.DOUBLE);
            } else {
                    statement.setDouble(position, x);
            }
        }

        @Override
        public void setBigDecimal(final BigDecimal x) throws SQLException {
            statement.setBigDecimal(position, x);
        }

        @Override
        public void setString(final String x) throws SQLException {
            statement.setString(position, x);
        }

        @Override
        public void setDate(final Date x) throws SQLException {
            statement.setDate(position, x);
        }

        @Override
        public void setTime(final Time x) throws SQLException {
            statement.setTime(position, x);
        }

        @Override
        public void setTimestamp(final Timestamp x) throws SQLException {
            statement.setTimestamp(position, x);
        }

        @Override
        public void setBytes(final byte[] x) throws SQLException {
            statement.setBytes(position, x);
        }
    }
}

