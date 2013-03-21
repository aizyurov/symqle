package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.AbstractCursorSpecification;
import org.simqle.sql.Column;
import org.simqle.sql.DialectDataSource;
import org.simqle.sql.GenericDialect;
import org.simqle.sql.TableOrView;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class CursorSpecificationTest extends SqlTestCase {


    public void testShow() throws Exception {
        final String sql = person.id.orderBy(person.id).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id", sql);
        final String sql2 = person.id.orderBy(person.id).show(GenericDialect.get());
        assertSimilar(sql, sql2);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.orderBy(person.id).forUpdate().show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.orderBy(person.id).forReadOnly().show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id FOR READ ONLY", sql);
    }

    public void testList() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DataSource datasource, final AbstractCursorSpecification<Long> cursorSpecification) throws SQLException {
                final List<Long> list = cursorSpecification.list(datasource);
                assertEquals(1, list.size());
                assertEquals(123L, list.get(0).longValue());
            }
        }.play();

        new Scenario() {
            @Override
            protected void runQuery(final DataSource datasource, final AbstractCursorSpecification<Long> cursorSpecification) throws SQLException {
                final List<Long> list = cursorSpecification.list(new DialectDataSource(GenericDialect.get(), datasource));
                assertEquals(1, list.size());
                assertEquals(123L, list.get(0).longValue());
            }
        }.play();
    }


    public void testScroll() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DataSource datasource, final AbstractCursorSpecification<Long> cursorSpecification) throws SQLException {
                cursorSpecification.scroll(datasource, new Callback<Long>() {
                    int callCount = 0;

                    @Override
                    public boolean iterate(final Long aNumber) {
                        if (callCount++ != 0) {
                            fail("One call expected, actually " + callCount);
                        }
                        assertEquals(123L, aNumber.longValue());
                        return true;
                    }
                });
            }
        }.play();

        new Scenario() {
            @Override
            protected void runQuery(final DataSource datasource, final AbstractCursorSpecification<Long> cursorSpecification) throws SQLException {
                cursorSpecification.scroll(new DialectDataSource(GenericDialect.get(), datasource), new Callback<Long>() {
                    int callCount = 0;

                    @Override
                    public boolean iterate(final Long aNumber) {
                        if (callCount++ != 0) {
                            fail("One call expected, actually " + callCount);
                        }
                        assertEquals(123L, aNumber.longValue());
                        return true;
                    }
                });
            }
        }.play();
    }

    private static abstract class Scenario {
        public void play() throws Exception {
            final DataSource datasource = createMock(DataSource.class);
            final Connection connection = createMock(Connection.class);
            final PreparedStatement statement = createMock(PreparedStatement.class);
            final ResultSet resultSet = createMock(ResultSet.class);
            final AbstractCursorSpecification<Long> cursorSpecification = person.id.orderBy(person.id);
            final String queryString = cursorSpecification.show();
            expect(datasource.getConnection()).andReturn(connection);
            expect(connection.prepareStatement(queryString)).andReturn(statement);
            expect(statement.executeQuery()).andReturn(resultSet);
            expect(resultSet.next()).andReturn(true);
            expect(resultSet.getLong(matches("C[0-9]"))).andReturn(123L);
            expect(resultSet.wasNull()).andReturn(false);
            expect(resultSet.next()).andReturn(false);
            resultSet.close();
            statement.close();
            connection.close();
            replay(datasource, connection, statement, resultSet);

            runQuery(datasource, cursorSpecification);
            verify(datasource, connection, statement, resultSet);
        }

        protected abstract void runQuery(final DataSource datasource, final AbstractCursorSpecification<Long> cursorSpecification) throws SQLException;
    }


    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
    }

    private static Person person = new Person();

}
