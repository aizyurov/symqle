package org.symqle.misctest;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.symqle.common.OnDemand;

/**
 * @author lvovich
 */
public class OnDemandTest extends TestCase {

    public void test() throws Exception {

        final OnDemand<Object> ref = new OnDemand<Object>() {
            @Override
            protected Object construct() {
                return new Object();
            }
        };

        final Object object1 = ref.get();
        final Object object2 = ref.get();
        Assert.assertEquals(object1, object2);
    }

    public void testNull() throws Exception {
        final OnDemand<Object> ref = new OnDemand<Object>() {
            @Override
            protected Object construct() {
                return null;
            }
        };

        final Object object1 = ref.get();
        final Object object2 = ref.get();
        Assert.assertEquals(object1, object2);
    }
}
