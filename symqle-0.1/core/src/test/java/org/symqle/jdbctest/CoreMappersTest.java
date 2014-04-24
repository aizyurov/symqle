package org.symqle.jdbctest;

import junit.framework.TestCase;
import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;
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
public class CoreMappersTest extends TestCase {

    public void testDate() throws Exception {
        InBox inBox = createMock(InBox.class);
        final Date now = new Date(System.currentTimeMillis());
        expect(inBox.getDate()).andReturn(now);
        replay(inBox);
        assertEquals(now, CoreMappers.DATE.value(inBox));
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setDate(now);
        replay(parameter);
        CoreMappers.DATE.setValue(parameter, now);
        verify(parameter);
    }

    public void testTime() throws Exception {
        InBox inBox = createMock(InBox.class);
        final Time now = new Time(1000);
        expect(inBox.getTime()).andReturn(now);
        replay(inBox);
        assertEquals(now, CoreMappers.TIME.value(inBox));
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setTime(now);
        replay(parameter);
        CoreMappers.TIME.setValue(parameter, now);
        verify(parameter);
    }

    public void testTimestamp() throws Exception {
        InBox inBox = createMock(InBox.class);
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        expect(inBox.getTimestamp()).andReturn(now);
        replay(inBox);
        assertEquals(now, CoreMappers.TIMESTAMP.value(inBox));
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setTimestamp(now);
        replay(parameter);
        CoreMappers.TIMESTAMP.setValue(parameter, now);
        verify(parameter);
    }

    public void testBoolean() throws Exception {
        InBox inBox = createMock(InBox.class);
        expect(inBox.getBoolean()).andReturn(true);
        replay(inBox);
        assertEquals(Boolean.TRUE, CoreMappers.BOOLEAN.value(inBox));
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setBoolean(true);
        replay(parameter);
        CoreMappers.BOOLEAN.setValue(parameter, true);
        verify(parameter);
    }

    public void testNumber() throws Exception {
        InBox inBox = createMock(InBox.class);
        expect(inBox.getBigDecimal()).andReturn(new BigDecimal(123));
        replay(inBox);
        assertEquals(123, CoreMappers.NUMBER.value(inBox).intValue());
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setBigDecimal(new BigDecimal(123));
        replay(parameter);
        CoreMappers.NUMBER.setValue(parameter, 123);
        verify(parameter);
    }

    public void testInteger() throws Exception {
        InBox inBox = createMock(InBox.class);
        expect(inBox.getInt()).andReturn(123);
        replay(inBox);
        assertEquals(123, CoreMappers.INTEGER.value(inBox).intValue());
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setInt(123);
        replay(parameter);
        CoreMappers.INTEGER.setValue(parameter, 123);
        verify(parameter);
    }

    public void testDouble() throws Exception {
        InBox inBox = createMock(InBox.class);
        expect(inBox.getDouble()).andReturn(123.456);
        replay(inBox);
        assertEquals(123.456, CoreMappers.DOUBLE.value(inBox));
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setDouble(123.456);
        replay(parameter);
        CoreMappers.DOUBLE.setValue(parameter, 123.456);
        verify(parameter);
    }


}
