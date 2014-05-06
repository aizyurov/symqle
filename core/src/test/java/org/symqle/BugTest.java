package org.symqle;

import junit.framework.TestCase;
import org.symqle.common.Bug;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 23.11.2013
 * Time: 22:12:39
 * To change this template use File | Settings | File Templates.
 */
public class BugTest extends TestCase {

    public void testTrue() {
        try {
            Bug.reportIf(true);
        } catch (IllegalStateException e) {
            assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains("symqle.org"));
        }
    }

    public void testFalse() {
        Bug.reportIf(false);
        assertTrue(true);
    }

    public void testIfNullNull() {
        try {
            Bug.reportIfNull(null);
        } catch (IllegalStateException e) {
            assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains("symqle.org"));
        }
    }

    public void testIfNullNotNull() {
        Bug.reportIfNull("a");
        assertTrue(true);
    }

    public void testIfNotNullNotNull() {
        try {
            Bug.reportIfNotNull("a");
        } catch (IllegalStateException e) {
            assertTrue("Unexpected message: " + e.getMessage(), e.getMessage().contains("symqle.org"));
        }
    }

    public void testIfNotNullNull() {
        Bug.reportIfNotNull(null);
        assertTrue(true);
    }

    public void testException() {
        try {
            Bug.reportException(new SQLException());
        } catch (IllegalStateException e) {
            assertEquals(e.getCause().getClass(), SQLException.class);
        }
    }

    public void testCallable() {
        try {
            final Callable<Void> callable = new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    throw new IOException();
                }
            };
            Bug.ifFails(callable);
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            // fine
        }
    }

}
