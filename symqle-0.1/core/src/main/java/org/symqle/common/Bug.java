package org.symqle.common;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 23.11.2013
 * Time: 18:27:05
 * To change this template use File | Settings | File Templates.
 */
public class Bug {

    private Bug() {
    }

    static {
        new Bug();
    }

    public static final String MESSAGE = "Please report a bug th http://symqle.org/bugs";

    public static void reportIf(boolean condition) {
        if (condition) {
            throw new IllegalStateException(MESSAGE
            );
        }
    }

    public static void reportIfNull(Object o) {
        if (o == null) {
            throw new IllegalStateException(MESSAGE
            );
        }
    }

    public static void reportIfNotNull(Object o) {
        if (o != null) {
            throw new IllegalStateException(MESSAGE
            );
        }
    }

}
