package org.symqle.jdbc;

/**
 * @author lvovich
 */
interface ConnectorWrapperFactory {

    String getName();

    Connector wrap(Connector connector);

}
