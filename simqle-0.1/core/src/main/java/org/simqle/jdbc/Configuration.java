package org.simqle.jdbc;

/**
 * Defined configurable options for statement construction
 *
 */
public interface Configuration {

    /**
    * Defines whether empty FROM is allowed.
    * @return true of no FROM clause is OK
    */
    boolean allowNoFrom();

    /**
    * Defines whether implicit cross joins are allowed,
    */
    boolean allowImplicitCrossJoins();
}
