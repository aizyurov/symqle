package org.simqle;

import junit.framework.TestCase;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;




/**
 * @author lvovich
 */
public class NullDataExtractorTest extends TestCase {

    public void testThrowingException() throws Exception {
        final RowMapper<Long> extractor = new NullRowMapper<Long>();
        final Row row = createMock(Row.class);
        replay(row);
        try {
            final Long aLong = extractor.extract(row);
            fail("RuntimeException expected but returned "+aLong);
        } catch (RuntimeException e) {
            // Ok
        }
        verify(row);
    }
}
