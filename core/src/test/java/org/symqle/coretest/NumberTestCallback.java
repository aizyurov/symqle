package org.symqle.coretest;

import junit.framework.Assert;
import org.symqle.common.Callback;

/**
* Created by IntelliJ IDEA.
* User: aizyurov
* Date: 24.11.2013
* Time: 17:01:51
* To change this template use File | Settings | File Templates.
*/
public class NumberTestCallback implements Callback<Number> {
    int callCount = 0;

    @Override
    public boolean iterate(final Number value) {
        if (callCount++ > 0) {
            Assert.fail("One row expected, actually " + callCount);
        }
        Assert.assertEquals(123, value.intValue());
        return true;
    }
}
