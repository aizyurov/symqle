package org.symqle.jdbctest;

import junit.framework.TestCase;
import org.symqle.common.Element;
import org.symqle.common.Mappers;
import org.symqle.common.SqlParameter;

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
        Element element = createMock(Element.class);
        final Date now = new Date(System.currentTimeMillis());
        expect(element.getDate()).andReturn(now);
        replay(element);
        assertEquals(now, Mappers.DATE.value(element));
        verify(element);

        SqlParameter parameter = createMock(SqlParameter.class);
        parameter.setDate(now);
        replay(parameter);
        Mappers.DATE.setValue(parameter, now);
        verify(parameter);
    }

    public void testTime() throws Exception {
        Element element = createMock(Element.class);
        final Time now = new Time(1000);
        expect(element.getTime()).andReturn(now);
        replay(element);
        assertEquals(now, Mappers.TIME.value(element));
        verify(element);

        SqlParameter parameter = createMock(SqlParameter.class);
        parameter.setTime(now);
        replay(parameter);
        Mappers.TIME.setValue(parameter, now);
        verify(parameter);
    }

    public void testTimestamp() throws Exception {
        Element element = createMock(Element.class);
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        expect(element.getTimestamp()).andReturn(now);
        replay(element);
        assertEquals(now, Mappers.TIMESTAMP.value(element));
        verify(element);

        SqlParameter parameter = createMock(SqlParameter.class);
        parameter.setTimestamp(now);
        replay(parameter);
        Mappers.TIMESTAMP.setValue(parameter, now);
        verify(parameter);
    }

    public void testBoolean() throws Exception {
        Element element = createMock(Element.class);
        expect(element.getBoolean()).andReturn(true);
        replay(element);
        assertEquals(Boolean.TRUE, Mappers.BOOLEAN.value(element));
        verify(element);

        SqlParameter parameter = createMock(SqlParameter.class);
        parameter.setBoolean(true);
        replay(parameter);
        Mappers.BOOLEAN.setValue(parameter, true);
        verify(parameter);
    }

    public void testNumber() throws Exception {
        Element element = createMock(Element.class);
        expect(element.getBigDecimal()).andReturn(new BigDecimal(123));
        replay(element);
        assertEquals(123, Mappers.NUMBER.value(element).intValue());
        verify(element);

        SqlParameter parameter = createMock(SqlParameter.class);
        parameter.setBigDecimal(new BigDecimal(123));
        replay(parameter);
        Mappers.NUMBER.setValue(parameter, 123);
        verify(parameter);
    }

    public void testInteger() throws Exception {
        Element element = createMock(Element.class);
        expect(element.getInt()).andReturn(123);
        replay(element);
        assertEquals(123, Mappers.INTEGER.value(element).intValue());
        verify(element);

        SqlParameter parameter = createMock(SqlParameter.class);
        parameter.setInt(123);
        replay(parameter);
        Mappers.INTEGER.setValue(parameter, 123);
        verify(parameter);
    }

    public void testDouble() throws Exception {
        Element element = createMock(Element.class);
        expect(element.getDouble()).andReturn(123.456);
        replay(element);
        assertEquals(123.456, Mappers.DOUBLE.value(element));
        verify(element);

        SqlParameter parameter = createMock(SqlParameter.class);
        parameter.setDouble(123.456);
        replay(parameter);
        Mappers.DOUBLE.setValue(parameter, 123.456);
        verify(parameter);
    }


}
