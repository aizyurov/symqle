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

package org.symqle.common;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * A collection of most common {@link Mapper}s.
 */
public final class Mappers {

    private Mappers() {
    }

    static {
        new Mappers();
    }

    /**
     * Mapper to Boolean.
     */
    public static final Mapper<Boolean> BOOLEAN = new Mapper<Boolean>() {
        @Override
        public Boolean value(final Element element) throws SQLException {
            return element.getBoolean();
        }

        @Override
        public void setValue(final SqlParameter param, final Boolean value) throws SQLException {
            param.setBoolean(value);
        }
    };

    /**
     * Mapper to Number.
     */
    public static  final Mapper<Number> NUMBER = new Mapper<Number>() {
        @Override
        public Number value(final Element element) throws SQLException {
            return element.getBigDecimal();
        }

        @Override
        public void setValue(SqlParameter param, Number value) throws SQLException {
            param.setBigDecimal(new BigDecimal(value.toString()));
        }
    };

    /**
     * Mapper to Long.
     */
    public static  final Mapper<Long> LONG = new Mapper<Long>() {
        @Override
        public Long value(final Element element) throws SQLException {
            return element.getLong();
        }

        @Override
        public void setValue(SqlParameter param, Long value) throws SQLException {
            param.setLong(value);
        }
    };

    /**
     * Mapper to String.
     */
    public static  final Mapper<String> STRING = new Mapper<String>() {
        @Override
        public String value(final Element element) throws SQLException {
            return element.getString();
        }

        @Override
        public void setValue(SqlParameter param, String value) throws SQLException {
            param.setString(value);
        }
    };

    /**
     * Mapper to Integer.
     */
    public static final Mapper<Integer> INTEGER = new Mapper<Integer>() {
        @Override
        public Integer value(final Element element) throws SQLException {
            return element.getInt();
        }

        @Override
        public void setValue(final SqlParameter param, final Integer value) throws SQLException {
            param.setInt(value);
        }
    };

    /**
     * Mapper to java.sql.Date.
     */
    public static final Mapper<Date> DATE = new Mapper<Date>() {
        @Override
        public Date value(final Element element) throws SQLException {
            return element.getDate();
        }

        @Override
        public void setValue(final SqlParameter param, final Date value) throws SQLException {
            param.setDate(value);
        }
    };

    /**
     * Mapper to java.sql.Time.
     */
    public static final Mapper<Time> TIME = new Mapper<Time>() {
        @Override
        public Time value(final Element element) throws SQLException {
            return element.getTime();
        }

        @Override
        public void setValue(final SqlParameter param, final Time value) throws SQLException {
            param.setTime(value);
        }
    };

    /**
     * Mapper to java.sql.Timestamp.
     */
    public static final Mapper<Timestamp> TIMESTAMP = new Mapper<Timestamp>() {
        @Override
        public Timestamp value(final Element element) throws SQLException {
            return element.getTimestamp();
        }

        @Override
        public void setValue(final SqlParameter param, final Timestamp value) throws SQLException {
            param.setTimestamp(value);
        }
    };

    /**
     * Mapper to Double.
     */

    public static final Mapper<Double> DOUBLE = new Mapper<Double>() {
        @Override
        public Double value(final Element element) throws SQLException {
            return element.getDouble();
        }

        @Override
        public void setValue(final SqlParameter param, final Double value) throws SQLException {
            param.setDouble(value);
        }
    };
}
