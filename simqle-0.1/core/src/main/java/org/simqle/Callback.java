/*
* Copyright Alexander Izyurov 2010
*/
package org.simqle;

/**
 * <br/>07.12.2010
 *
 * @author Alexander Izyurov
 */
public interface Callback<Arg> {

    /**
     * Called by iterating object in a loop. The iterating object passes values of arg one by one.
     * @param arg the callback argument
     * @return true to continue iterations, false to break
     */
    boolean iterate(Arg arg);

}
