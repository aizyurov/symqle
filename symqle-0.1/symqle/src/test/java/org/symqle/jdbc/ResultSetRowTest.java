package org.symqle.jdbc;

import junit.framework.TestCase;
import org.symqle.common.InBox;
import org.symqle.common.Mappers;
import org.symqle.common.OutBox;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class ResultSetRowTest extends TestCase {

    public void testLong() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getLong("A")).andReturn(12L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getLong("B")).andReturn(0L);
        expect(resultSet.wasNull()).andReturn(true);
        expect(resultSet.getLong("C")).andThrow(new SQLException("unknown column: C"));

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(12L, row.getValue("A").getLong().longValue());
        assertNull(row.getValue("B").getLong());

        try {
            final Long aLong = row.getValue("C").getLong();
            fail("SQLException expected but returned "+aLong);
        } catch (SQLException e) {
            // expected
        }
        verify(resultSet);
    }
    
    public void testInteger() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getInt("A")).andReturn(12);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getInt("B")).andReturn(0);
        expect(resultSet.wasNull()).andReturn(true);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(12L, row.getValue("A").getInt().intValue());
        assertNull(row.getValue("B").getInt());

        verify(resultSet);
    }
    
    public void testShort() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getShort("A")).andReturn((short) 12);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getShort("B")).andReturn((short) 0);
        expect(resultSet.wasNull()).andReturn(true);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(12L, row.getValue("A").getShort().intValue());
        assertNull(row.getValue("B").getShort());

        verify(resultSet);
        
    }    

    public void testByte() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getByte("A")).andReturn((byte) 12);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getByte("B")).andReturn((byte) 0);
        expect(resultSet.wasNull()).andReturn(true);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(12L, row.getValue("A").getByte().intValue());
        assertNull(row.getValue("B").getByte());

        verify(resultSet);

    }

    public void testBigDecimal() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getBigDecimal("A")).andReturn(new BigDecimal(123.456));
        expect(resultSet.getBigDecimal("B")).andReturn(null);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(123.456, row.getValue("A").getBigDecimal().doubleValue());
        assertNull(row.getValue("B").getBigDecimal());

        verify(resultSet);

    }

    public void testBoolean() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getBoolean("A")).andReturn(true);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getBoolean("B")).andReturn(false);
        expect(resultSet.wasNull()).andReturn(true);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(Boolean.TRUE, row.getValue("A").getBoolean());
        assertNull(row.getValue("B").getBoolean());

        verify(resultSet);

    }

    public void testString() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getString("A")).andReturn("test");
        expect(resultSet.getString("B")).andReturn(null);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals("test", row.getValue("A").getString());
        assertNull(row.getValue("B").getString());

        verify(resultSet);

    }

    public void testDate() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        final Date now = new Date(System.currentTimeMillis());
        expect(resultSet.getDate("A")).andReturn(now);
        expect(resultSet.getDate("B")).andReturn(null);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(now, row.getValue("A").getDate());
        assertNull(row.getValue("B").getDate());

        verify(resultSet);

    }

    public void testDateMapper() throws Exception {
        final OutBox param = createMock(OutBox.class);
        final Date date = new Date(System.currentTimeMillis());
        param.setDate(date);
        replay(param);
        Mappers.DATE.setValue(param, date);
        verify(param);

        InBox InBox = createMock(InBox.class);
        expect(InBox.getDate()).andReturn(date);

        replay(InBox);

        assertEquals(date, Mappers.DATE.value(InBox));
        verify(InBox);
    }

    public void testTimestampMapper() throws Exception {
        final OutBox param = createMock(OutBox.class);
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        param.setTimestamp(timestamp);
        replay(param);
        Mappers.TIMESTAMP.setValue(param, timestamp);
        verify(param);

        InBox InBox = createMock(InBox.class);
        expect(InBox.getTimestamp()).andReturn(timestamp);

        replay(InBox);

        assertEquals(timestamp, Mappers.TIMESTAMP.value(InBox));
        verify(InBox);
    }

    public void testTimeMapper() throws Exception {
        final OutBox param = createMock(OutBox.class);
        final Time time = new Time(System.currentTimeMillis());
        param.setTime(time);
        replay(param);
        Mappers.TIME.setValue(param, time);
        verify(param);

        InBox InBox = createMock(InBox.class);
        expect(InBox.getTime()).andReturn(time);

        replay(InBox);

        assertEquals(time, Mappers.TIME.value(InBox));
        verify(InBox);
    }

    public void testTimestamp() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        expect(resultSet.getTimestamp("A")).andReturn(now);
        expect(resultSet.getTimestamp("B")).andReturn(null);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(now, row.getValue("A").getTimestamp());
        assertNull(row.getValue("B").getTimestamp());

        verify(resultSet);

    }

    public void testTime() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        final Time now = new Time(36000000L);
        expect(resultSet.getTime("A")).andReturn(now);
        expect(resultSet.getTime("B")).andReturn(null);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(now, row.getValue("A").getTime());
        assertNull(row.getValue("B").getTime
                ());

        verify(resultSet);
    }

    public void testDouble() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getDouble("A")).andReturn(123.456);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getDouble("B")).andReturn(0.0);
        expect(resultSet.wasNull()).andReturn(true);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(123.456, row.getValue("A").getDouble());
        assertNull(row.getValue("B").getDouble());

        verify(resultSet);

        final OutBox param = createMock(OutBox.class);
        final Double d = 2.3e4;
        param.setDouble(d);
        replay(param);

        Mappers.DOUBLE.setValue(param, d);
        verify(param);

        final InBox InBox = createMock(InBox.class);
        expect(InBox.getDouble()).andReturn(d);
        replay(InBox);

        assertEquals(d, Mappers.DOUBLE.value(InBox));
        verify(InBox);
    }

    public void testFloat() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getFloat("A")).andReturn((float) 123.456);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getFloat("B")).andReturn((float) 0.0);
        expect(resultSet.wasNull()).andReturn(true);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals((float) 123.456, row.getValue("A").getFloat());
        assertNull(row.getValue("B").getFloat());

        verify(resultSet);

    }

}
