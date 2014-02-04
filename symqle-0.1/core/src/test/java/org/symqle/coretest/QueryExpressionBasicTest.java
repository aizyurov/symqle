package org.symqle.coretest;

import org.symqle.common.*;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractQueryExpressionBasic;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class QueryExpressionBasicTest extends SqlTestCase {


    public void testShow() throws Exception {
        final AbstractQueryExpressionBasic<Long> queryExpressionBasic = person.id.orderBy(person.id);
        final String sql = queryExpressionBasic.show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id", sql);
        final String sql2 = person.id.orderBy(person.id).show(new GenericDialect());
        assertSimilar(sql, sql2);
    }

    public void testAdapt() throws Exception {
        final String sql = AbstractQueryExpressionBasic.adapt(person.id).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0", sql);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.orderBy(person.id).forUpdate().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.orderBy(person.id).forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id FOR READ ONLY", sql);
    }

    public void testList() throws Exception {
        final AbstractQueryExpressionBasic<Long> queryExpressionBasic = person.id.orderBy(person.id);
        new Scenario(queryExpressionBasic) {
            @Override
            void use(AbstractQueryExpressionBasic<Long> query, QueryEngine engine) throws SQLException {
                final List<Long> expected = Arrays.asList(123L);
                assertEquals(expected, query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario(person.id.orderBy(person.id)) {
            @Override
            void use(AbstractQueryExpressionBasic<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, new TestCallback<Long>(123L)));
            }
        }.play();
    }

    private static abstract class Scenario extends AbstractQueryScenario<Long, AbstractQueryExpressionBasic<Long>> {

        private Scenario(AbstractQueryExpressionBasic<Long> query) {
            super(query);
        }

        @Override
        List<OutBox> parameterExpectations(SqlParameters parameters) throws SQLException {
            return Collections.emptyList();
        }

        @Override
        void elementCall(InBox inBox) throws SQLException {
            expect(inBox.getLong()).andReturn(123L);
        }
    }



    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
    }

    private static Person person = new Person();

}
