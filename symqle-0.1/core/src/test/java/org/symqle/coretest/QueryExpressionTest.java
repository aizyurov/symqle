package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.common.Pair;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractQueryExpression;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 15.12.2013
 * Time: 19:18:40
 * To change this template use File | Settings | File Templates.
 */
public class QueryExpressionTest extends SqlTestCase {

    private AbstractQueryExpression<Pair<Long, String>> createQueryExpression() {
        return person.id.pair(person.name).limit(10);
    }

    public void testForUpdate() throws Exception {
        final String sql = createQueryExpression().forUpdate().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 FETCH FIRST 10 ROWS ONLY FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = createQueryExpression().forReadOnly().show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 FETCH FIRST 10 ROWS ONLY FOR READ ONLY", sql);
    }

    public void testAdapt() throws Exception {
        final String sql = AbstractQueryExpression.adapt(person.id.pair(person.name)).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0", sql);
    }

    public void testList() throws Exception {
        new PairScenario<AbstractQueryExpression<Pair<Long, String>>>(createQueryExpression()) {
            @Override
            protected void use(AbstractQueryExpression<Pair<Long, String>> query, QueryEngine engine) throws SQLException {
                final List<Pair<Long, String>> expected = Arrays.asList(Pair.make(123L, "John"));
                assertEquals(expected, query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new PairScenario<AbstractQueryExpression<Pair<Long, String>>>(createQueryExpression()) {
            @Override
            protected void use(AbstractQueryExpression<Pair<Long, String>> query, QueryEngine engine) throws SQLException {
                final int processedRows = query.scroll(engine, new TestCallback<Pair<Long, String>>(Pair.make(123L, "John")));
                assertEquals(1, processedRows);
            }
        }.play();
        
    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Long> age = defineColumn(Mappers.LONG, "age");
    }

    private static Person person = new Person();

}
