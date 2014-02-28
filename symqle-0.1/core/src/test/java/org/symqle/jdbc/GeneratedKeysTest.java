package org.symqle.jdbc;

import junit.framework.TestCase;
import org.symqle.common.CoreMappers;
import org.symqle.common.InBox;

import java.util.Arrays;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class GeneratedKeysTest extends TestCase {

    public void testGeneratedKeys() throws Exception {
        final GeneratedKeys<Long> generatedKeys = GeneratedKeys.create(CoreMappers.LONG);
        final InBox inBox = createMock(InBox.class);
        final Long aLong = Long.valueOf(123L);
        expect(inBox.getLong()).andReturn(aLong);
        replay(inBox);
        generatedKeys.read(inBox);
        assertEquals(Arrays.asList(aLong), generatedKeys.all());
        assertEquals(aLong, generatedKeys.first());
        verify(inBox);
    }
}
