package org.simqle;

import java.math.BigDecimal;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 02.01.2013
 * Time: 0:40:54
 * To change this template use File | Settings | File Templates.
 */
public class Mappers {

    public static final ElementMapper<Boolean> BOOLEAN = new ElementMapper<Boolean>() {
        @Override
        public Boolean value(Element element) throws SQLException {
            return element.getBoolean();
        }

        @Override
        public void setValue(SqlParameter param, Boolean value) throws SQLException {
            param.setBoolean(value);
        }
    };

    public static  final ElementMapper<Number> NUMBER = new ElementMapper<Number>() {
        @Override
        public Number value(Element element) throws SQLException {
            return element.getBigDecimal();
        }

        @Override
        public void setValue(SqlParameter param, Number value) throws SQLException {
            param.setBigDecimal(new BigDecimal(value.toString()));
        }
    };

    public static  final ElementMapper<Long> LONG = new ElementMapper<Long>() {
        @Override
        public Long value(Element element) throws SQLException {
            return element.getLong();
        }

        @Override
        public void setValue(SqlParameter param, Long value) throws SQLException {
            param.setLong(value);
        }
    };

    public static  final ElementMapper<String> STRING = new ElementMapper<String>() {
        @Override
        public String value(Element element) throws SQLException {
            return element.getString();
        }

        @Override
        public void setValue(SqlParameter param, String value) throws SQLException {
            param.setString(value);
        }
    };

}
