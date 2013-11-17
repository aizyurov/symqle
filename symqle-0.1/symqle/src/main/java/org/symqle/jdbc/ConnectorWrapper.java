package org.symqle.jdbc;

/**
 * @author lvovich
 */
interface ConnectorWrapper {

    String getName();

    Connector wrap(Connector connector);

}
