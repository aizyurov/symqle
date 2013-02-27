package org.simqle.jdbc;

import org.simqle.SqlParameter;
import org.simqle.SqlParameters;

import java.math.BigDecimal;
import java.sql.*;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 14.12.2012
 * Time: 23:08:40
 * To change this template use File | Settings | File Templates.
 */
public class StatementParameters implements SqlParameters {
    private final PreparedStatement statement;
    private int position;

    public StatementParameters(PreparedStatement statement) {
        this.statement = statement;
    }

    @Override
    public SqlParameter next() {
        return new StatementParameter(++position);
    }

    private class StatementParameter implements SqlParameter {
        private final int position;

        private StatementParameter(int position) {
            this.position = position;
        }

        @Override
        public void setBoolean(Boolean x) throws SQLException {
            if (x == null) {
                statement.setNull(position, Types.BOOLEAN);
            } else {
                statement.setBoolean(position, x);
            }
        }

        @Override
        public void setByte(Byte x) throws SQLException {
            if (x == null) {
                statement.setNull(position, Types.TINYINT);
            } else {
                statement.setByte(position, x);
            }
        }

        @Override
        public void setShort(Short x) throws SQLException {
            if (x == null) {
                statement.setNull(position, Types.SMALLINT);
            } else {
                statement.setShort(position, x);
            }
        }

        @Override
        public void setInt(Integer x) throws SQLException {
            if (x == null) {
                statement.setNull(position, Types.INTEGER);
            } else {
                    statement.setInt(position, x);
            }
        }

        @Override
        public void setLong(Long x) throws SQLException {
            if (x == null) {
                statement.setNull(position, Types.BIGINT);
            } else {
                    statement.setLong(position, x);
            }
        }

        @Override
        public void setFloat(Float x) throws SQLException {
            if (x == null) {
                statement.setNull(position, Types.FLOAT);
            } else {
                    statement.setFloat(position, x);
            }
        }

        @Override
        public void setDouble(Double x) throws SQLException {
            if (x == null) {
                statement.setNull(position, Types.DOUBLE);
            } else {
                    statement.setDouble(position, x);
            }
        }

        @Override
        public void setBigDecimal(BigDecimal x) throws SQLException {
            statement.setBigDecimal(position, x);
        }

        @Override
        public void setString(String x) throws SQLException {
            statement.setString(position, x);
        }

        @Override
        public void setDate(Date x) throws SQLException {
            statement.setDate(position, x);
        }

        @Override
        public void setTime(Time x) throws SQLException {
            statement.setTime(position, x);
        }

        @Override
        public void setTimestamp(Timestamp x) throws SQLException {
            statement.setTimestamp(position, x);
        }
    }
}

