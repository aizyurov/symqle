package org.symqle.misctest;

import junit.framework.TestCase;
import org.symqle.common.InBox;
import org.symqle.common.Mappers;
import org.symqle.common.OutBox;

import java.math.BigDecimal;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class MappersTest extends TestCase {

    public void testBigDecimal() throws Exception {
        InBox inBox = createMock(InBox.class);
        expect(inBox.getBigDecimal()).andReturn(new BigDecimal("123.456"));
        replay(inBox);
        assertEquals(new BigDecimal("123.456"), Mappers.DECIMAL.value(inBox));
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        parameter.setBigDecimal(new BigDecimal("123.456"));
        replay(parameter);
        Mappers.DECIMAL.setValue(parameter, new BigDecimal("123.456"));
        verify(parameter);
    }

    public void testVoid() throws Exception {
        InBox inBox = createMock(InBox.class);
        replay(inBox);
        assertNull(Mappers.VOID.value(inBox));
        verify(inBox);

        OutBox parameter = createMock(OutBox.class);
        replay(parameter);
        try {
            Mappers.VOID.setValue(parameter, null);
            fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
            // ok
        }
        verify(parameter);
    }

}
