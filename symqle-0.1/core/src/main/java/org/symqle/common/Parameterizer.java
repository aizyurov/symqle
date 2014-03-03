package org.symqle.common;

import java.sql.SQLException;

/**
 * @author lvovich
 */
public interface Parameterizer {
    /**
     * Provide values for dynamic parameters.
     * @param p SqlParameters to write parameter values into
     * @throws java.sql.SQLException if jdbc driver cannot set parameters
     */
    void setParameters(SqlParameters p) throws SQLException;
}
