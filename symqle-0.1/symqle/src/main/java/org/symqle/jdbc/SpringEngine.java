/*
   Copyright 2010-2013 Alexander Izyurov

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.package org.symqle.common;
*/

package org.symqle.jdbc;

import org.symqle.sql.Dialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Engine, which is aware of Spring transactions.
 * Statements, executed in this engine, will be executed in Spring transactions,
 * for example, if their execute() method is called inside
 * {@link org.springframework.transaction.support.TransactionTemplate#execute(org.springframework.transaction.support.TransactionCallback)}
 * @author lvovich
 */
public class SpringEngine extends AbstractEngine {

    private final Connector connector;

    /**
     * Constructs the engine, auto-detecting proper dialect.
     * @param dataSource provides connection to the database
     * @param options default options to apply for query building and execution
     */
    public SpringEngine(final DataSource dataSource, final Option... options) throws SQLException {
        super(DatabaseUtils.getDatabaseName(dataSource), options);
        final Connector connector = new SpringConnector(dataSource);
        this.connector = DatabaseUtils.wrap(connector, getDatabaseName());
    }

    /**
     * Constructs the engine.
     * @param dataSource provides connection to the database
     * @param dialect forces the use of this dialect, no auto-detection
     * @param options default options to apply for query building and execution
     */
    public SpringEngine(final DataSource dataSource, final Dialect dialect, final Option... options) throws SQLException {
        super(dialect, DatabaseUtils.getDatabaseName(dataSource), options);
        final Connector connector = new SpringConnector(dataSource);
        this.connector = DatabaseUtils.wrap(connector, getDatabaseName());
    }

    @Override
    protected final Connection getConnection() throws SQLException {
        return connector.getConnection();
    }

    @Override
    protected final void releaseConnection(final Connection connection) throws SQLException {
        connection.close();
    }

}
