package org.symqle.misctest;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.symqle.util.LazyRef;

/**
 * @author lvovich
 */
public class LazyRefTest extends TestCase {

    public void test() throws Exception {

        final LazyRef<Object> ref = new LazyRef<Object>() {
            @Override
            protected Object create() {
                return new Object();
            }
        };

        final Object object1 = ref.get();
        final Object object2 = ref.get();
        Assert.assertEquals(object1, object2);
    }

    public void testNull() throws Exception {
        final LazyRef<Object> ref = new LazyRef<Object>() {
            @Override
            protected Object create() {
                return null;
            }
        };

        final Object object1 = ref.get();
        final Object object2 = ref.get();
        Assert.assertEquals(object1, object2);
    }
}
