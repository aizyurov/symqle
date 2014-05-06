package org.symqle.integration.model;

import org.symqle.common.CoreMappers;
import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.Table;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @author lvovich
 */
public class AllTypes extends Table {

    @Override
    public String getTableName() {
        return "all_types";
    }

    public Column<Long> tBit() {
        return defineColumn(CoreMappers.LONG, "t_BIT");
    }

    public Column<Byte> tTinyint() {
        return defineColumn(Mappers.BYTE, "t_TINYINT");
    }

    public Column<Short> tSmallint() {
        return defineColumn(Mappers.SHORT, "t_SMALLINT");
    }

    public Column<Integer> tInteger() {
        return defineColumn(CoreMappers.INTEGER, "t_INTEGER");
    }

    public Column<Long> tBigint() {
        return defineColumn(CoreMappers.LONG, "t_BIGINT");
    }

    public Column<Float> tFloat() {
        return defineColumn(Mappers.FLOAT, "t_FLOAT");
    }

    public Column<Double> tDouble() {
        return defineColumn(CoreMappers.DOUBLE, "t_DOUBLE");
    }

    public Column<Float> tReal() {
        return defineColumn(Mappers.FLOAT, "t_REAL");
    }

    public Column<Number> tNumeric() {
        return defineColumn(CoreMappers.NUMBER, "t_NUMERIC");
    }

    public Column<Number> tDecimal() {
        return defineColumn(CoreMappers.NUMBER, "t_DECIMAL");
    }

    public Column<String> tChar() {
        return defineColumn(CoreMappers.STRING, "t_CHAR");
    }

    public Column<String> tVarchar() {
        return defineColumn(CoreMappers.STRING, "t_VARCHAR");
    }

    public Column<String> tLongvarchar() {
        return defineColumn(CoreMappers.STRING, "t_LONGVARCHAR");
    }

    public Column<Date> tDate() {
        return defineColumn(CoreMappers.DATE, "t_DATE");
    }

    public Column<Time> tTime() {
        return defineColumn(CoreMappers.TIME, "t_TIME");
    }

    public Column<Timestamp> tTimestamp() {
        return defineColumn(CoreMappers.TIMESTAMP, "t_TIMESTAMP");
    }

    public Column<Timestamp> tDatetime() {
        return defineColumn(CoreMappers.TIMESTAMP, "t_DATETIME");
    }

    public Column<byte[]> tBinary() {
        return defineColumn(Mappers.BYTES, "t_BINARY");
    }

    public Column<byte[]> tVarbinary() {
        return defineColumn(Mappers.BYTES, "t_VARBINARY");
    }

    public Column<byte[]> tLongvarbinary() {
        return defineColumn(Mappers.BYTES, "t_LONGVARBINARY");
    }

    public Column<byte[]> tBlob() {
        return defineColumn(Mappers.BYTES, "t_BLOB");
    }

    public Column<String> tClob() {
        return defineColumn(CoreMappers.STRING, "t_CLOB");
    }

    public Column<Boolean> tBoolean() {
        return defineColumn(CoreMappers.BOOLEAN, "t_BOOLEAN");
    }

    public Column<String> tNchar() {
        return defineColumn(CoreMappers.STRING, "t_NCHAR");
    }

    public Column<String> tNvarchar() {
        return defineColumn(CoreMappers.STRING, "t_NVARCHAR");
    }

    public Column<String> tLongnvarchar() {
        return defineColumn(CoreMappers.STRING, "t_LONGNVARCHAR");
    }

    public Column<String> tNclob() {
        return defineColumn(CoreMappers.STRING, "t_NCLOB");
    }

}
