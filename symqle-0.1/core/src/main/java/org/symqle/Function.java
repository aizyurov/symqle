package org.symqle;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 09.12.2012
 * Time: 21:27:41
 * To change this template use File | Settings | File Templates.
 */
public interface Function<A, R> {
    R apply(A arg);
}
