package org.simqle;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 02.01.2013
 * Time: 0:40:54
 * To change this template use File | Settings | File Templates.
 */
public class Mappers {

    private Mappers() {

    }

    static {
        new Mappers();
    }

    public static final Mapper<Boolean> BOOLEAN = new Mapper<Boolean>() {
        @Override
        public Boolean value(Element element) throws SQLException {
            return element.getBoolean();
        }

        @Override
        public void setValue(SqlParameter param, Boolean value) throws SQLException {
            param.setBoolean(value);
        }
    };

    public static  final Mapper<Number> NUMBER = new Mapper<Number>() {
        @Override
        public Number value(Element element) throws SQLException {
            return element.getBigDecimal();
        }

        @Override
        public void setValue(SqlParameter param, Number value) throws SQLException {
            param.setBigDecimal(new BigDecimal(value.toString()));
        }
    };

    public static  final Mapper<Long> LONG = new Mapper<Long>() {
        @Override
        public Long value(Element element) throws SQLException {
            return element.getLong();
        }

        @Override
        public void setValue(SqlParameter param, Long value) throws SQLException {
            param.setLong(value);
        }
    };

    public static  final Mapper<String> STRING = new Mapper<String>() {
        @Override
        public String value(Element element) throws SQLException {
            return element.getString();
        }

        @Override
        public void setValue(SqlParameter param, String value) throws SQLException {
            param.setString(value);
        }
    };

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
