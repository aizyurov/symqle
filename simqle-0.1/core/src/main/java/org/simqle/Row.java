/*
* Copyright Alexander Izyurov 2010
*/
package org.simqle;

/**
 * An abstraction of a single row of a result set.
 *
 * @author Alexander Izyurov
 */
public interface Row {
    /**
     * Accesses a value slot for a column in the row by label.
     * @param label column label
     * @return the value slot
     */
    Element getValue(String label);

}
