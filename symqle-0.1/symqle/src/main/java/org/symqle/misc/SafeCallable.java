package org.symqle.misc;

import org.symqle.common.Bug;

import java.util.concurrent.Callable;

/**
 * A Callable wrapped to Runnable.
 * We are absolutely sure that the exception will be never thrown form {@link #call()}.
 * It the impossible happens, we report a bug.
 */
public abstract class SafeCallable implements Runnable, Callable<Void> {
    public final void run() {
        try {
            call();
        } catch (Exception e) {
            Bug.reportException(e);
        }
    }

}
