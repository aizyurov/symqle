package org.symqle.jdbc;

/**
 * @author lvovich
 */
class MySqlConnectorWrapper implements ConnectorWrapper {

    @Override
    public Connector wrap(final Connector connector) {
        return new MySqlConnector(connector);
    }
}
