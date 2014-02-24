package org.symqle.integration;

import org.symqle.integration.model.AllTypes;
import org.symqle.sql.SmartSelector;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

/**
 * @author lvovich
 */
public class AllTypesTest extends AbstractIntegrationTestBase  {

    public void testInsertAndSelect() throws Exception {
        final AllTypes allTypes = new AllTypes();
        allTypes.delete().execute(getEngine());
        final long now = System.currentTimeMillis();
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        final Date today = new Date(calendar.getTimeInMillis());
        allTypes.insert(
                allTypes.tBit().set(1L),
                allTypes.tTinyint().set((byte) 123),
                allTypes.tSmallint().set((short) 10000),
                allTypes.tInteger().set(200000),
                allTypes.tBigint().set(30000000L),
                allTypes.tFloat().set(1.1f),
                allTypes.tDouble().set(2.2),
                allTypes.tReal().set(3.3f),
                allTypes.tNumeric().set(new BigDecimal("123.456")),
                allTypes.tDecimal().set(new BigDecimal("654.321")),
                allTypes.tChar().set("abcdefghij"),
                allTypes.tVarchar().set("this is a varchar"),
                allTypes.tLongvarchar().set("longvarchar sample"),
                allTypes.tDate().set(today),
                allTypes.tTime().set(new Time(7200000)),
                allTypes.tTimestamp().set(new Timestamp(now)),
                allTypes.tDatetime().set(new Timestamp(now)),
                allTypes.tBinary().set(new byte[] {(byte) 0, (byte) 1, (byte)2, (byte)3 }),
                allTypes.tVarbinary().set(new byte[] {(byte) 100, (byte) 101, (byte) 102}),
                allTypes.tLongvarbinary().set(new byte[] {(byte) 50, (byte) 51, (byte)52, (byte)53, (byte) 54 }),
                allTypes.tClob().set("clob clob clob clob clob"),
                allTypes.tBlob().set(new byte[] {(byte) 40, (byte) 41, (byte)42, (byte)43, (byte) 44 }),
                allTypes.tBoolean().set(true),
                allTypes.tNchar().set("nchar"),
                allTypes.tNvarchar().set("nvarchar") ,
                allTypes.tLongnvarchar().set("longnvarchar"),
                allTypes.tNclob().set("nclob")
        ).execute(getEngine());

        final List<AllTypesDTO> list = new AllTypesSelector(allTypes, now).list(getEngine());
        assertEquals(1, list.size());
        final AllTypesDTO dto = list.get(0);
        
        assertEquals(Long.valueOf(1L), dto.tBit);
        assertEquals(Byte.valueOf((byte) 123), dto.tTinyint);
        assertEquals(Short.valueOf((short)10000), dto.tSmallint);
        assertEquals(Integer.valueOf(200000), dto.tInteger);
        assertEquals(Long.valueOf(30000000L), dto.tBigint);
        assertEquals(1.1f, dto.tFloat);
        assertEquals(2.2, dto.tDouble);
        assertEquals(3.3f, dto.tReal);
        assertEquals(new BigDecimal("123.456").toString(), dto.tNumeric.toString());
        assertEquals(new BigDecimal("654.321").toString(), dto.tDecimal.toString());
        assertEquals("abcdefghij", dto.tChar);
        assertEquals("this is a varchar", dto.tVarchar);
        assertEquals("longvarchar sample", dto.tLongvarchar);
        assertEquals(today, dto.tDate);
        assertEquals(new Time(7200000), dto.tTime);
        assertEquals(new Timestamp(now), dto.tTimestamp);
        assertEquals(new Timestamp(now), dto.tDatetime);
        assertBytesEqual(new byte[] {(byte) 0, (byte) 1, (byte)2, (byte)3 }, dto.tBinary);
        assertBytesEqual(new byte[] {(byte) 100, (byte) 101, (byte) 102}, dto.tVarbinary);
        assertBytesEqual(new byte[] {(byte) 50, (byte) 51, (byte)52, (byte)53, (byte) 54 }, dto.tLongvarbinary);
        assertBytesEqual(new byte[] {(byte) 40, (byte) 41, (byte)42, (byte)43, (byte) 44 }, dto.tBlob);
        assertEquals(Boolean.TRUE, dto.tBoolean);
        assertEquals("clob clob clob clob clob", dto.tClob);
        assertEquals("nchar     ", dto.tNchar);
        assertEquals("nvarchar", dto.tNvarchar);
        assertEquals("longnvarchar", dto.tLongnvarchar);
        assertEquals("nclob", dto.tNclob);

    }
    
    private static class AllTypesDTO {
        private Long tBit;
        private Byte tTinyint;
        private Short tSmallint;
        private Integer tInteger;
        private Long tBigint;
        private Float tFloat;
        private Double tDouble;
        private Float tReal;
        private Number tNumeric;
        private Number tDecimal;
        private String tChar;
        private String tVarchar;
        private String tLongvarchar;
        private Date tDate;
        private Time tTime;
        private Timestamp tTimestamp;
        private Timestamp tDatetime;
        private byte[] tBinary;
        private byte[] tVarbinary;
        private byte[] tLongvarbinary;
        private byte[] tBlob;
        private Boolean tBoolean;
        private String tClob;
        private String tNchar;
        private String tNvarchar;
        private String tLongnvarchar;
        private String tNclob;
    }
    
    private static class AllTypesSelector extends SmartSelector<AllTypesDTO> {

        private final AllTypes allTypes;
        private final long now;

        private AllTypesSelector(final AllTypes allTypes, final long now) {
            this.allTypes = allTypes;
            this.now = now;
        }

        @Override
        protected AllTypesDTO create(final RowMap rowMap) throws SQLException {
            final AllTypesDTO dto = new AllTypesDTO();
            dto.tBit = rowMap.get(allTypes.tBit());
            dto.tTinyint = rowMap.get(allTypes.tTinyint());
            dto.tSmallint = rowMap.get(allTypes.tSmallint());
            dto.tInteger = rowMap.get(allTypes.tInteger());
            dto.tBigint = rowMap.get(allTypes.tBigint());
            dto.tFloat = rowMap.get(allTypes.tFloat());
            dto.tDouble =  rowMap.get(allTypes.tDouble());
            dto.tReal = rowMap.get(allTypes.tReal());
            dto.tNumeric = rowMap.get(allTypes.tNumeric());
            dto.tDecimal = rowMap.get(allTypes.tDecimal());
            dto.tChar =  rowMap.get(allTypes.tChar());
            dto.tVarchar = rowMap.get(allTypes.tVarchar());
            dto.tLongvarchar = rowMap.get(allTypes.tLongvarchar());
            dto.tDate = rowMap.get(allTypes.tDate());
            dto.tTime = rowMap.get(allTypes.tTime());
            dto.tTimestamp = rowMap.get(allTypes.tTimestamp());
            dto.tDatetime = rowMap.get(allTypes.tDatetime());
            dto.tBinary = rowMap.get(allTypes.tBinary());
            dto.tVarbinary = rowMap.get(allTypes.tVarbinary());
            dto.tLongvarbinary = rowMap.get(allTypes.tLongvarbinary());
            dto.tBlob = rowMap.get(allTypes.tBlob());
            dto.tBoolean = rowMap.get(allTypes.tBoolean());
            dto.tClob = rowMap.get(allTypes.tClob());
            dto.tNchar = rowMap.get(allTypes.tNchar());
            dto.tNvarchar = rowMap.get(allTypes.tNvarchar());
            dto.tLongnvarchar = rowMap.get(allTypes.tLongnvarchar());
            dto.tNclob = rowMap.get(allTypes.tNclob());
            return dto;
        }
    }

    private void assertBytesEqual(byte[] expected, byte[] actual) {
        if (!Arrays.equals(expected, actual)) {
            fail("expected: " + Arrays.toString(expected) +" but was: " + Arrays.toString(actual));
        }
    }


}
