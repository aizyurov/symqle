package org.simqle.jdbc;

import org.simqle.Element;
import org.simqle.Row;

import java.math.BigDecimal;
import java.sql.*;

/**
 * A transparent view to a ResultSet.
 * Scrolling the underlying ResultSet is reflected in the ResultSetRow.
 * User: aizyurov
 */
public class ResultSetRow implements Row {
    private final ResultSet resultSet;

    public ResultSetRow(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public Element getValue(String label) {
        return new LabeledElement(label);
    }

    private class LabeledElement implements Element {
        private final String label;

        private LabeledElement(String label) {
            this.label = label;
        }

        @Override
        public Boolean getBoolean() throws SQLException {
            final boolean result = resultSet.getBoolean(label);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Byte getByte() throws SQLException {
            final byte result = resultSet.getByte(label);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Short getShort() throws SQLException {
            final short result = resultSet.getShort(label);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Integer getInt() throws SQLException {
            final int result = resultSet.getInt(label);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Long getLong() throws SQLException {
            final long result = resultSet.getLong(label);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Float getFloat() throws SQLException {
            final float result = resultSet.getFloat(label);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Double getDouble() throws SQLException {
            final double result = resultSet.getDouble(label);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public BigDecimal getBigDecimal() throws SQLException {
            final BigDecimal result = resultSet.getBigDecimal(label);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public String getString() throws SQLException {
            final String result = resultSet.getString(label);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Date getDate() throws SQLException {
            final Date result = resultSet.getDate(label);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Time getTime() throws SQLException {
            final Time result = resultSet.getTime(label);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Timestamp getTimestamp() throws SQLException {
            final Timestamp result = resultSet.getTimestamp(label);
            return resultSet.wasNull() ? null : result;
        }
    }
}
