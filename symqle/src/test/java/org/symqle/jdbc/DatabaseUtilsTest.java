package org.symqle.jdbc;

import junit.framework.TestCase;
import org.symqle.dialect.DerbyDialect;
import org.symqle.sql.Dialect;
import org.symqle.sql.GenericDialect;

import static org.easymock.EasyMock.createMock;

/**
 * @author lvovich
 */
public class DatabaseUtilsTest extends TestCase {

    public void testKnownDialect() throws Exception {
        final Dialect dialect = DatabaseUtils.getDialect("Apache Derby");
        assertEquals(DerbyDialect.class, dialect.getClass());
    }

    public void testUnknownDialect() throws Exception {
        final Dialect dialect = DatabaseUtils.getDialect("YourSQL");
        assertEquals(GenericDialect.class, dialect.getClass());
    }

    public void testKnownWrapper() throws Exception {
        final Connector connector = createMock(Connector.class);
        final Connector wrappedConnector = DatabaseUtils.wrap(connector, "MySQL");
        assertEquals(MySqlConnector.class, wrappedConnector.getClass());
    }

    public void testNoWrapper() throws Exception {
        final Connector connector = createMock(Connector.class);
        final Connector wrappedConnector = DatabaseUtils.wrap(connector, "YourSQL");
        assertEquals(connector, wrappedConnector);
    }

    public void testMissingResource() throws Exception {
        try {
            DatabaseUtils.readProperties("nothing.properties");
            fail("IllegalStateException expected");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("symqle.org"));
        }
    }
}
