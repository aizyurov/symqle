package org.symqle.jdbc;

/**
 * @author lvovich
 */
class MySqlConnectorWrapper implements ConnectorWrapper {

    @Override
    public String getName() {
        return "MySQL";
    }

    @Override
    public Connector wrap(final Connector connector) {
        return new MySqlConnector(connector);
    }
}
