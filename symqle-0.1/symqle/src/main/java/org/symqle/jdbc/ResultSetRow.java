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

import org.symqle.common.InBox;
import org.symqle.common.Row;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * A Row, which is a view to a ResultSet.
 */
class ResultSetRow implements Row {
    private final ResultSet resultSet;
    private final QueryEngine innerEngine;

    /**
     * Constructs for a given ResultSet and inner engine
     * @param resultSet the result set to view
     */
    public ResultSetRow(ResultSet resultSet, QueryEngine engine) {
        this.resultSet = resultSet;
        this.innerEngine = engine;
    }

    @Override
    public final InBox getValue(String label) {
        return new LabeledInBox(label);
    }
    
    @Override
    public final InBox getValue(int position) {
        return new PositionedInBox(position);
    }

    @Override
    public QueryEngine getQueryEngine() {
        return innerEngine;
    }

    private class LabeledInBox implements InBox {
        private final String label;

        private LabeledInBox(final String label) {
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
            return resultSet.getBigDecimal(label);
        }

        @Override
        public String getString() throws SQLException {
            return resultSet.getString(label);
        }

        @Override
        public Date getDate() throws SQLException {
            return resultSet.getDate(label);
        }

        @Override
        public Time getTime() throws SQLException {
            return resultSet.getTime(label);
        }

        @Override
        public Timestamp getTimestamp() throws SQLException {
            return resultSet.getTimestamp(label);
        }

        @Override
        public byte[] getBytes() throws SQLException {
            return resultSet.getBytes(label);
        }
    }
    
    private class PositionedInBox implements InBox {
        private final int position;

        private PositionedInBox(final int position) {
            this.position = position;
        }
        
        @Override
        public Boolean getBoolean() throws SQLException {
            final boolean result = resultSet.getBoolean(position);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Byte getByte() throws SQLException {
            final byte result = resultSet.getByte(position);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Short getShort() throws SQLException {
            final short result = resultSet.getShort(position);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Integer getInt() throws SQLException {
            final int result = resultSet.getInt(position);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Long getLong() throws SQLException {
            final long result = resultSet.getLong(position);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Float getFloat() throws SQLException {
            final float result = resultSet.getFloat(position);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public Double getDouble() throws SQLException {
            final double result = resultSet.getDouble(position);
            return resultSet.wasNull() ? null : result;
        }

        @Override
        public BigDecimal getBigDecimal() throws SQLException {
            return resultSet.getBigDecimal(position);
        }

        @Override
        public String getString() throws SQLException {
            return resultSet.getString(position);
        }

        @Override
        public Date getDate() throws SQLException {
            return resultSet.getDate(position);
        }

        @Override
        public Time getTime() throws SQLException {
            return resultSet.getTime(position);
        }

        @Override
        public Timestamp getTimestamp() throws SQLException {
            return resultSet.getTimestamp(position);
        }

        @Override
        public byte[] getBytes() throws SQLException {
            return resultSet.getBytes(position);
        }
    }

}
