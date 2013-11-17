package org.symqle.jdbc;

import org.springframework.beans.factory.annotation.Required;
import org.symqle.sql.Dialect;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class SpringEngineFactory extends AbstractEngineFactory {

    private DataSource dataSource;
    private List<Option> options = Collections.emptyList();
    private Dialect dialect;

    @Required
    public void setDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setOptions(final List<Option> options) {
        this.options = options;
    }

    public void setDialect(final Dialect dialect) {
        this.dialect = dialect;
    }

    public Engine create() throws SQLException {
        final String databaseName = getDatabaseName(dataSource);
        return new ConnectorEngine(getConnector(databaseName, dataSource),
                dialect != null ? dialect : getDialect(databaseName),
                databaseName,
                options.toArray(new Option[options.size()]));
    }

    @Override
    protected Connector createConnector(final DataSource dataSource) {
        return new SpringConnector(dataSource);
    }
}
