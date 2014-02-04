package org.symqle.jdbctest;

import junit.framework.TestCase;
import org.symqle.common.InBox;
import org.symqle.common.Mappers;
import org.symqle.common.OutBox;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;


/**
 * @author lvovich
 */
public class MappersTest extends TestCase {

    public void testDate() throws Exception {
        InBox inBox = createMock(InBox.class);
        final Date now = new Date(System.currentTimeMillis());
        expect(inBox.getDate()).andReturn(now);
        replay(inBox);
        assertEquals(now, Mappers.DATE.value(inBox));
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setDate(now);
        replay(parameter);
        Mappers.DATE.setValue(parameter, now);
        verify(parameter);
    }

    public void testTime() throws Exception {
        InBox inBox = createMock(InBox.class);
        final Time now = new Time(1000);
        expect(inBox.getTime()).andReturn(now);
        replay(inBox);
        assertEquals(now, Mappers.TIME.value(inBox));
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setTime(now);
        replay(parameter);
        Mappers.TIME.setValue(parameter, now);
        verify(parameter);
    }

    public void testTimestamp() throws Exception {
        InBox inBox = createMock(InBox.class);
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        expect(inBox.getTimestamp()).andReturn(now);
        replay(inBox);
        assertEquals(now, Mappers.TIMESTAMP.value(inBox));
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setTimestamp(now);
        replay(parameter);
        Mappers.TIMESTAMP.setValue(parameter, now);
        verify(parameter);
    }

    public void testBoolean() throws Exception {
        InBox inBox = createMock(InBox.class);
        expect(inBox.getBoolean()).andReturn(true);
        replay(inBox);
        assertEquals(Boolean.TRUE, Mappers.BOOLEAN.value(inBox));
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setBoolean(true);
        replay(parameter);
        Mappers.BOOLEAN.setValue(parameter, true);
        verify(parameter);
    }

    public void testNumber() throws Exception {
        InBox inBox = createMock(InBox.class);
        expect(inBox.getBigDecimal()).andReturn(new BigDecimal(123));
        replay(inBox);
        assertEquals(123, Mappers.NUMBER.value(inBox).intValue());
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setBigDecimal(new BigDecimal(123));
        replay(parameter);
        Mappers.NUMBER.setValue(parameter, 123);
        verify(parameter);
    }

    public void testInteger() throws Exception {
        InBox inBox = createMock(InBox.class);
        expect(inBox.getInt()).andReturn(123);
        replay(inBox);
        assertEquals(123, Mappers.INTEGER.value(inBox).intValue());
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setInt(123);
        replay(parameter);
        Mappers.INTEGER.setValue(parameter, 123);
        verify(parameter);
    }

    public void testDouble() throws Exception {
        InBox inBox = createMock(InBox.class);
        expect(inBox.getDouble()).andReturn(123.456);
        replay(inBox);
        assertEquals(123.456, Mappers.DOUBLE.value(inBox));
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setDouble(123.456);
        replay(parameter);
        Mappers.DOUBLE.setValue(parameter, 123.456);
        verify(parameter);
    }


}
