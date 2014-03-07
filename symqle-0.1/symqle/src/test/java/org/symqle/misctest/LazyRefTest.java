package org.symqle.misctest;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.symqle.util.OnDemand;

/**
 * @author lvovich
 */
public class LazyRefTest extends TestCase {

    public void test() throws Exception {

        final OnDemand<Object> ref = new OnDemand<Object>() {
            @Override
            protected Object init() {
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
            protected Object init() {
                return null;
            }
        };

        final Object object1 = ref.get();
        final Object object2 = ref.get();
        Assert.assertEquals(object1, object2);
    }
}
