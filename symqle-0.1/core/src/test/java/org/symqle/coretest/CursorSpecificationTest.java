package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.sql.AbstractCursorSpecification;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.util.Arrays;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class CursorSpecificationTest extends SqlTestCase {


    public void testShow() throws Exception {
        final String sql = person.id.orderBy(person.id).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.id", sql);
        final String sql2 = person.id.orderBy(person.id).show(new GenericDialect());
        assertSimilar(sql, sql2);
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
        final AbstractCursorSpecification<Long> cursorSpecification = person.id.orderBy(person.id);
        final String queryString = cursorSpecification.show(new GenericDialect());
        final List<Long> expected = Arrays.asList(123L);
        final SqlParameters parameters = createMock(SqlParameters.class);
        replay(parameters);
        final List<Long> list = cursorSpecification.list(
            new MockQueryEngine<Long>(new SqlContext(), expected, queryString, parameters));
        assertEquals(expected, list);
        verify(parameters);
    }

    public void testScroll() throws Exception {
        final AbstractCursorSpecification<Long> cursorSpecification = person.id.orderBy(person.id);
        final String queryString = cursorSpecification.show(new GenericDialect());
        final List<Long> expected = Arrays.asList(123L);
        final SqlParameters parameters = createMock(SqlParameters.class);
        replay(parameters);
        int rows = cursorSpecification.scroll(
            new MockQueryEngine<Long>(new SqlContext(),
                    expected, queryString, parameters),
                new TestCallback<Long>(123L));
        assertEquals(1, rows);
        verify(parameters);
    }


    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
    }

    private static Person person = new Person();

}
