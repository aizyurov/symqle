package org.symqle.jdbctest;

import junit.framework.TestCase;
import org.symqle.jdbc.Option;
import org.symqle.querybuilder.UpdatableConfiguration;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;


/**
 * @author lvovich
 */
public class OptionsTest extends TestCase {

    public void testCrossJoins() throws Exception {
        final UpdatableConfiguration configuration = createMock(UpdatableConfiguration.class);
        final Statement statement = createMock(Statement.class);
        configuration.setImplicitCrossJoinsOk(true);
        replay(configuration, statement);
        final Option option = Option.allowImplicitCrossJoins(true);
        option.apply(configuration);
        option.apply(statement);
        verify(configuration, statement);
        final Option option1 = Option.allowImplicitCrossJoins(true);
        assertEquals(option, option);
        assertEquals(option, option1);
        final Option option2 = Option.allowImplicitCrossJoins(false);
        assertFalse(option.equals(option2));
        final Option option3 = Option.allowNoTables(true);
        assertFalse(option.equals(option3));
        assertFalse(option.equals(null));
    }

    public void testNoTables() throws Exception {
        final UpdatableConfiguration configuration = createMock(UpdatableConfiguration.class);
        final Statement statement = createMock(Statement.class);
        configuration.setImplicitCrossJoinsOk(true);
        replay(configuration, statement);
        final Option option = Option.allowNoTables(true);
        option.apply(configuration);
        option.apply(statement);
        verify(configuration, statement);
        final Option option1 = Option.allowNoTables(true);
        final Option option2 = Option.allowNoTables(false);
        final Option option3 = Option.allowImplicitCrossJoins(false);
        assertEquals(option, option);
        assertEquals(option, option1);
        assertFalse(option.equals(option2));
        assertFalse(option.equals(option3));
        assertFalse(option.equals(null));
    }

    public void testFetchDirection() throws Exception {
        final UpdatableConfiguration configuration = createMock(UpdatableConfiguration.class);
        final Statement statement = createMock(Statement.class);
        statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        replay(configuration, statement);
        final Option option = Option.setFetchDirection(ResultSet.FETCH_FORWARD);
        option.apply(configuration);
        option.apply(statement);
        verify(configuration, statement);
        final Option option1 = Option.setFetchDirection(ResultSet.FETCH_FORWARD);
        final Option option2 = Option.setFetchDirection(ResultSet.FETCH_REVERSE);
        final Option option3 = Option.setFetchSize(100);
        assertEquals(option, option);
        assertEquals(option, option1);
        assertFalse(option.equals(option2));
        assertFalse(option.equals(option3));
        assertFalse(option.equals(null));
    }

    public void testFetchSize() throws Exception {
        final UpdatableConfiguration configuration = createMock(UpdatableConfiguration.class);
        final Statement statement = createMock(Statement.class);
        statement.setFetchSize(100);
        replay(configuration, statement);
        final Option option = Option.setFetchSize(100);
        option.apply(configuration);
        option.apply(statement);
        verify(configuration, statement);
        final Option option1 = Option.setFetchSize(100);
        final Option option2 = Option.setFetchSize(200);
        final Option option3 = Option.setFetchDirection(ResultSet.FETCH_FORWARD);
        assertEquals(option, option);
        assertEquals(option, option1);
        assertFalse(option.equals(option2));
        assertFalse(option.equals(option3));
        assertFalse(option.equals(null));
    }

    public void testFieldSize() throws Exception {
        final UpdatableConfiguration configuration = createMock(UpdatableConfiguration.class);
        final Statement statement = createMock(Statement.class);
        statement.setMaxFieldSize(100);
        replay(configuration, statement);
        final Option option = Option.setMaxFieldSize(100);
        option.apply(configuration);
        option.apply(statement);
        verify(configuration, statement);
        final Option option1 = Option.setMaxFieldSize(100);
        final Option option2 = Option.setMaxFieldSize(200);
        final Option option3 = Option.setFetchSize(100);
        assertEquals(option, option);
        assertEquals(option, option1);
        assertFalse(option.equals(option2));
        assertFalse(option.equals(option3));
        assertFalse(option.equals(null));
    }

    public void testMaxRows() throws Exception {
        final UpdatableConfiguration configuration = createMock(UpdatableConfiguration.class);
        final Statement statement = createMock(Statement.class);
        statement.setMaxRows(100);
        replay(configuration, statement);
        final Option option = Option.setMaxRows(100);
        option.apply(configuration);
        option.apply(statement);
        verify(configuration, statement);
        final Option option1 = Option.setMaxRows(100);
        final Option option2 = Option.setMaxRows(200);
        final Option option3 = Option.setFetchSize(100);
        assertEquals(option, option);
        assertEquals(option, option1);
        assertFalse(option.equals(option2));
        assertFalse(option.equals(option3));
        assertFalse(option.equals(null));
    }

    public void testQueryTimeout() throws Exception {
        final UpdatableConfiguration configuration = createMock(UpdatableConfiguration.class);
        final Statement statement = createMock(Statement.class);
        statement.setQueryTimeout(100);
        replay(configuration, statement);
        final Option option = Option.setQueryTimeout(100);
        option.apply(configuration);
        option.apply(statement);
        verify(configuration, statement);
        final Option option1 = Option.setQueryTimeout(100);
        final Option option2 = Option.setQueryTimeout(200);
        final Option option3 = Option.setMaxRows(100);
        assertEquals(option, option);
        assertEquals(option, option1);
        assertFalse(option.equals(option2));
        assertFalse(option.equals(option3));
        assertFalse(option.equals(null));
    }

    public void testSet() throws Exception {
        final Set<Option> options = new HashSet<Option>();
        options.add(Option.allowImplicitCrossJoins(true));
        options.add(Option.allowNoTables(true));
        options.add(Option.setFetchSize(100));
        options.add(Option.setFetchDirection(ResultSet.FETCH_FORWARD));
        options.add(Option.setMaxRows(100));
        options.add(Option.setMaxFieldSize(100));
        options.add(Option.setQueryTimeout(100));
        assertTrue(options.contains(Option.allowImplicitCrossJoins(true)));
        assertTrue(options.contains(Option.allowNoTables(true)));
        assertTrue(options.contains(Option.setFetchSize(100)));
        assertTrue(options.contains(Option.setFetchDirection(ResultSet.FETCH_FORWARD)));
        assertTrue(options.contains(Option.setMaxRows(100)));
        assertTrue(options.contains(Option.setMaxFieldSize(100)));
        assertTrue(options.contains(Option.setQueryTimeout(100)));
        assertFalse(options.contains(Option.setQueryTimeout(200)));
        assertFalse(options.contains(Option.allowImplicitCrossJoins(false)));
        assertFalse(options.contains(Option.allowNoTables(false)));

    }

}
