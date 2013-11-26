package org.symqle.misctest;

import junit.framework.TestCase;
import org.symqle.misc.SafeCallable;

import java.io.IOException;

/**
 * @author lvovich
 */
public class SafeCallableTest extends TestCase {

    public void testException() {
        try {
            new SafeCallable() {
                @Override
                public Void call() throws Exception {
                    throw new IOException();
                }
            }.run();
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            // fine
        }
    }
}
