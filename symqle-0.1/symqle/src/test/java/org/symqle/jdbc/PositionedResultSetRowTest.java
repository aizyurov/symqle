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
public class PositionedResultSetRowTest extends TestCase {

    public void testLong() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getLong(1)).andReturn(12L);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getLong(2)).andReturn(0L);
        expect(resultSet.wasNull()).andReturn(true);
        expect(resultSet.getLong(3)).andThrow(new SQLException("unknown column: C"));

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(12L, row.getValue(1).getLong().longValue());
        assertNull(row.getValue(2).getLong());

        try {
            final Long aLong = row.getValue(3).getLong();
            fail("SQLException expected but returned "+aLong);
        } catch (SQLException e) {
            // expected
        }
        verify(resultSet);
    }
    
    public void testInteger() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getInt(1)).andReturn(12);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getInt(2)).andReturn(0);
        expect(resultSet.wasNull()).andReturn(true);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(12L, row.getValue(1).getInt().intValue());
        assertNull(row.getValue(2).getInt());

        verify(resultSet);
    }
    
    public void testShort() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getShort(1)).andReturn((short) 12);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getShort(2)).andReturn((short) 0);
        expect(resultSet.wasNull()).andReturn(true);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(12L, row.getValue(1).getShort().intValue());
        assertNull(row.getValue(2).getShort());

        verify(resultSet);
        
    }    

    public void testByte() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getByte(1)).andReturn((byte) 12);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getByte(2)).andReturn((byte) 0);
        expect(resultSet.wasNull()).andReturn(true);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(12L, row.getValue(1).getByte().intValue());
        assertNull(row.getValue(2).getByte());

        verify(resultSet);

    }

    public void testBigDecimal() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getBigDecimal(1)).andReturn(new BigDecimal(123.456));
        expect(resultSet.getBigDecimal(2)).andReturn(null);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(123.456, row.getValue(1).getBigDecimal().doubleValue());
        assertNull(row.getValue(2).getBigDecimal());

        verify(resultSet);

    }

    public void testBoolean() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getBoolean(1)).andReturn(true);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getBoolean(2)).andReturn(false);
        expect(resultSet.wasNull()).andReturn(true);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(Boolean.TRUE, row.getValue(1).getBoolean());
        assertNull(row.getValue(2).getBoolean());

        verify(resultSet);

    }

    public void testString() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getString(1)).andReturn("test");
        expect(resultSet.getString(2)).andReturn(null);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals("test", row.getValue(1).getString());
        assertNull(row.getValue(2).getString());

        verify(resultSet);

    }

    public void testDate() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        final Date now = new Date(System.currentTimeMillis());
        expect(resultSet.getDate(1)).andReturn(now);
        expect(resultSet.getDate(2)).andReturn(null);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(now, row.getValue(1).getDate());
        assertNull(row.getValue(2).getDate());

        verify(resultSet);

    }

    public void testDateMapper() throws Exception {
        final OutBox param = createMock(OutBox.class);
        final Date date = new Date(System.currentTimeMillis());
        param.setDate(date);
        replay(param);
        Mappers.DATE.setValue(param, date);
        verify(param);

        InBox inBox = createMock(InBox.class);
        expect(inBox.getDate()).andReturn(date);

        replay(inBox);

        assertEquals(date, Mappers.DATE.value(inBox));
        verify(inBox);
    }

    public void testTimestampMapper() throws Exception {
        final OutBox param = createMock(OutBox.class);
        final Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        param.setTimestamp(timestamp);
        replay(param);
        Mappers.TIMESTAMP.setValue(param, timestamp);
        verify(param);

        InBox inBox = createMock(InBox.class);
        expect(inBox.getTimestamp()).andReturn(timestamp);

        replay(inBox);

        assertEquals(timestamp, Mappers.TIMESTAMP.value(inBox));
        verify(inBox);
    }

    public void testTimeMapper() throws Exception {
        final OutBox param = createMock(OutBox.class);
        final Time time = new Time(System.currentTimeMillis());
        param.setTime(time);
        replay(param);
        Mappers.TIME.setValue(param, time);
        verify(param);

        InBox inBox = createMock(InBox.class);
        expect(inBox.getTime()).andReturn(time);

        replay(inBox);

        assertEquals(time, Mappers.TIME.value(inBox));
        verify(inBox);
    }

    public void testTimestamp() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        expect(resultSet.getTimestamp(1)).andReturn(now);
        expect(resultSet.getTimestamp(2)).andReturn(null);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(now, row.getValue(1).getTimestamp());
        assertNull(row.getValue(2).getTimestamp());

        verify(resultSet);

    }

    public void testTime() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        final Time now = new Time(36000000L);
        expect(resultSet.getTime(1)).andReturn(now);
        expect(resultSet.getTime(2)).andReturn(null);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(now, row.getValue(1).getTime());
        assertNull(row.getValue(2).getTime
                ());

        verify(resultSet);
    }

    public void testDouble() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getDouble(1)).andReturn(123.456);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getDouble(2)).andReturn(0.0);
        expect(resultSet.wasNull()).andReturn(true);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals(123.456, row.getValue(1).getDouble());
        assertNull(row.getValue(2).getDouble());

        verify(resultSet);

        final OutBox param = createMock(OutBox.class);
        final Double d = 2.3e4;
        param.setDouble(d);
        replay(param);

        Mappers.DOUBLE.setValue(param, d);
        verify(param);

        final InBox inBox = createMock(InBox.class);
        expect(inBox.getDouble()).andReturn(d);
        replay(inBox);

        assertEquals(d, Mappers.DOUBLE.value(inBox));
        verify(inBox);
    }

    public void testFloat() throws Exception {
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(resultSet.getFloat(1)).andReturn((float) 123.456);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.getFloat(2)).andReturn((float) 0.0);
        expect(resultSet.wasNull()).andReturn(true);

        final QueryEngine queryEngine = createMock(QueryEngine.class);

        final ResultSetRow row = new ResultSetRow(resultSet, queryEngine);
        replay(resultSet);

        assertEquals((float) 123.456, row.getValue(1).getFloat());
        assertNull(row.getValue(2).getFloat());

        verify(resultSet);

    }

}
