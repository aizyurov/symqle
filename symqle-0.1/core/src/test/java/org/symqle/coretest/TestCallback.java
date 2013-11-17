package org.symqle.coretest;

import junit.framework.Assert;
import org.symqle.common.Callback;

/**
* @author lvovich
*/
public class TestCallback<T> extends Assert implements Callback<T> {
    int callCount = 0;
    private final T expected;

    TestCallback(final T expected) {
        this.expected = expected;
    }

    @Override
    public boolean iterate(final T value) {
        if (callCount++ != 0) {
            fail("One call expected, actually " + callCount);
        }
        assertEquals(expected, value);
        return true;
    }
}
