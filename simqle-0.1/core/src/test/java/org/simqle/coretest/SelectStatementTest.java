package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.AbstractSelectStatement;
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
public class SelectStatementTest extends SqlTestCase {

    public void testShow() throws Exception {
        final AbstractSelectStatement<Long> selectStatement = person.id.forUpdate();
        final String sql = selectStatement.show();
        final String sql2 = selectStatement.show(GenericDialect.get());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 FOR UPDATE", sql);
        assertSimilar(sql, sql2);
    }



    public void testList() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DataSource datasource, final AbstractSelectStatement<Long> selectStatement) throws SQLException {
                final List<Long> list = selectStatement.list(datasource);
                assertEquals(1, list.size());
                assertEquals(123L, list.get(0).longValue());
            }
        }.play();

        new Scenario() {
            @Override
            protected void runQuery(final DataSource datasource, final AbstractSelectStatement<Long> selectStatement) throws SQLException {
                final List<Long> list = selectStatement.list(new DialectDataSource(GenericDialect.get(), datasource));
                assertEquals(1, list.size());
                assertEquals(123L, list.get(0).longValue());
            }
        }.play();
    }


    public void testScroll() throws Exception {
        new Scenario() {
            @Override
            protected void runQuery(final DataSource datasource, final AbstractSelectStatement<Long> selectStatement) throws SQLException {
                selectStatement.scroll(datasource, new Callback<Long>() {
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
            protected void runQuery(final DataSource datasource, final AbstractSelectStatement<Long> selectStatement) throws SQLException {
                selectStatement.scroll(new DialectDataSource(GenericDialect.get(), datasource), new Callback<Long>() {
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
            final AbstractSelectStatement<Long> selectStatement = person.id.forUpdate();
            final String queryString = selectStatement.show();
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

            runQuery(datasource, selectStatement);
            verify(datasource, connection, statement, resultSet);
        }

        protected abstract void runQuery(final DataSource datasource, final AbstractSelectStatement<Long> selectStatement) throws SQLException;
    }


    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
    }

    private static Person person = new Person();

}
