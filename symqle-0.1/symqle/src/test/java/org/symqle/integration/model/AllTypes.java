package org.symqle.integration.model;

import org.symqle.common.Mappers;
import org.symqle.sql.Column;
import org.symqle.sql.MoreMappers;
import org.symqle.sql.Table;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @author lvovich
 */
public class AllTypes extends Table {

    public AllTypes() {
        super("all_types");
    }

    public Column<Long> tBit() {
        return defineColumn(Mappers.LONG, "t_BIT");
    }

    public Column<Byte> tTinyint() {
        return defineColumn(MoreMappers.BYTE, "t_TINYINT");
    }

    public Column<Short> tSmallint() {
        return defineColumn(MoreMappers.SHORT, "t_SMALLINT");
    }

    public Column<Integer> tInteger() {
        return defineColumn(Mappers.INTEGER, "t_INTEGER");
    }

    public Column<Long> tBigint() {
        return defineColumn(Mappers.LONG, "t_BIGINT");
    }

    public Column<Float> tFloat() {
        return defineColumn(MoreMappers.FLOAT, "t_FLOAT");
    }

    public Column<Double> tDouble() {
        return defineColumn(Mappers.DOUBLE, "t_DOUBLE");
    }

    public Column<Float> tReal() {
        return defineColumn(MoreMappers.FLOAT, "t_REAL");
    }

    public Column<Number> tNumeric() {
        return defineColumn(Mappers.NUMBER, "t_NUMERIC");
    }

    public Column<Number> tDecimal() {
        return defineColumn(Mappers.NUMBER, "t_DECIMAL");
    }

    public Column<String> tChar() {
        return defineColumn(Mappers.STRING, "t_CHAR");
    }

    public Column<String> tVarchar() {
        return defineColumn(Mappers.STRING, "t_VARCHAR");
    }

    public Column<String> tLongvarchar() {
        return defineColumn(Mappers.STRING, "t_LONGVARCHAR");
    }

    public Column<Date> tDate() {
        return defineColumn(Mappers.DATE, "t_DATE");
    }

    public Column<Time> tTime() {
        return defineColumn(Mappers.TIME, "t_TIME");
    }

    public Column<Timestamp> tTimestamp() {
        return defineColumn(Mappers.TIMESTAMP, "t_TIMESTAMP");
    }

    public Column<Timestamp> tDatetime() {
        return defineColumn(Mappers.TIMESTAMP, "t_DATETIME");
    }

    public Column<byte[]> tBinary() {
        return defineColumn(MoreMappers.BYTES, "t_BINARY");
    }

    public Column<byte[]> tVarbinary() {
        return defineColumn(MoreMappers.BYTES, "t_VARBINARY");
    }

    public Column<byte[]> tLongvarbinary() {
        return defineColumn(MoreMappers.BYTES, "t_LONGVARBINARY");
    }

    public Column<byte[]> tBlob() {
        return defineColumn(MoreMappers.BYTES, "t_BLOB");
    }

    public Column<String> tClob() {
        return defineColumn(Mappers.STRING, "t_CLOB");
    }

    public Column<Boolean> tBoolean() {
        return defineColumn(Mappers.BOOLEAN, "t_BOOLEAN");
    }

    public Column<String> tNchar() {
        return defineColumn(Mappers.STRING, "t_NCHAR");
    }

    public Column<String> tNvarchar() {
        return defineColumn(Mappers.STRING, "t_NVARCHAR");
    }

    public Column<String> tLongnvarchar() {
        return defineColumn(Mappers.STRING, "t_LONGNVARCHAR");
    }

    public Column<String> tNclob() {
        return defineColumn(Mappers.STRING, "t_NCLOB");
    }

}
