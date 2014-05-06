package org.symqle.common;

/**
 * Symqle query builder throws this exception if it cannot construct a valid statement.
 * Examples are: no tables in FROM clause, implicit cross join,
 * column not belonging to the target table in set list.
 */
public class MalformedStatementException extends RuntimeException {

    /**
     * Constructs the exception with a given message.
     * @param message the message
     */
    public MalformedStatementException(final String message) {
        super(message);
    }
}
