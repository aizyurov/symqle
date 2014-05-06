/*
   Copyright 2010-2013 Alexander Izyurov

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.package org.symqle.common;
*/

package org.symqle.common;

import java.util.concurrent.Callable;

/**
 * A utility class to handle unexpected conditions.
 * Unexpected exception, null value where it should not be etc.
 * cause IllegalStateException with proper message.
 */
public final class Bug {

    private Bug() {
    }

    static {
        new Bug();
    }

    private static final String MESSAGE = "Please report a bug th http://symqle.org/bugs";

    /**
     * Reports a bug if condition is true.
     * @param condition condition to check
     * @throws IllegalStateException condition is true
     */
    public static void reportIf(final boolean condition) {
        if (condition) {
            throw new IllegalStateException(MESSAGE);
        }
    }

    /**
     * Reports a bug if argument is null.
     * @param o object co check for null
     * @throws IllegalStateException if null
     */
    public static void reportIfNull(final Object o) {
        if (o == null) {
            throw new IllegalStateException(MESSAGE);
        }
    }

    /**
     * Reports a bug if argument is not null.
     * @param o object co check for not null
     * @throws IllegalStateException if not null
     */
    public static void reportIfNotNull(final Object o) {
        if (o != null) {
            throw new IllegalStateException(MESSAGE);
        }
    }

    /**
     * Reports a bug with wrapped checked exception.
     * @param e exception to wrap
     * @throws IllegalStateException always
     */
    public static void reportException(final Exception e) {
        throw new IllegalStateException(MESSAGE, e);
    }

    /**
     * Executes a callable and returns its result.
     * Reports a bug if the call method throws a checked exception.
     * @param callable the callable to execute
     * @param <T> return type
     * @return the result of call method
     */
    public static <T> T ifFails(final Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new IllegalStateException(MESSAGE, e);
        }
    }

}
