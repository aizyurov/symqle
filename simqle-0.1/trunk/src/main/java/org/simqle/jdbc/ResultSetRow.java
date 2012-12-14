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

    @Override
    public Element getValue(int position) {
        return new PositionedElement(position);
    }

    private class PositionedElement implements Element {
        private final int position;

        private PositionedElement(int position) {
            this.position = position;
        }

        @Override
        public Boolean getBoolean() throws SQLException {
            final boolean result = resultSet.getBoolean(position);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Byte getByte() throws SQLException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Short getShort() throws SQLException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Integer getInt() throws SQLException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Long getLong() throws SQLException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Float getFloat() throws SQLException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Double getDouble() throws SQLException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public BigDecimal getBigDecimal() throws SQLException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public String getString() throws SQLException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Date getDate() throws SQLException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Time getTime() throws SQLException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public Timestamp getTimestamp() throws SQLException {
            return null;  //To change body of implemented methods use File | Settings | File Templates.
        }
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
            throw new RuntimeException("Not implemented");
        }

        @Override
        public Short getShort() throws SQLException {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public Integer getInt() throws SQLException {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public Long getLong() throws SQLException {
            final long result = resultSet.getLong(label);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Float getFloat() throws SQLException {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public Double getDouble() throws SQLException {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public BigDecimal getBigDecimal() throws SQLException {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public String getString() throws SQLException {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public Date getDate() throws SQLException {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public Time getTime() throws SQLException {
            throw new RuntimeException("Not implemented");
        }

        @Override
        public Timestamp getTimestamp() throws SQLException {
            throw new RuntimeException("Not implemented");
        }
    }
}
