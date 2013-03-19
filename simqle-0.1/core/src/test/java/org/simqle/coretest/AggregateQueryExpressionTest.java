package org.simqle.coretest;

import org.simqle.Callback;
import org.simqle.Mappers;
import org.simqle.sql.AbstractAggregateFunction;
import org.simqle.sql.AbstractAggregateQuerySpecification;
import org.simqle.sql.Column;
import org.simqle.sql.DynamicParameter;
import org.simqle.sql.TableOrView;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
 * @author lvovich
 */
public class AggregateQueryExpressionTest extends SqlTestCase  {

    public void testShow() throws Exception {
        final String show = person.id.count().where(person.age.gt(20L)).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.age > ?", show);
    }

    public void testUnion() throws Exception {
        final String show = person.id.count().where(person.age.gt(20L)).union(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.age > ? UNION SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testUnionAll() throws Exception {
        final String show = person.id.count().where(person.age.gt(20L)).unionAll(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.age > ? UNION ALL SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }
    public void testUnionDistinct() throws Exception {
        final String show = person.id.count().where(person.age.gt(20L)).unionDistinct(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.age > ? UNION DISTINCT SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testExcept() throws Exception {
        final String show = person.id.count().where(person.age.gt(20L)).except(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.age > ? EXCEPT SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testExceptAll() throws Exception {
        final String show = person.id.count().where(person.age.gt(20L)).exceptAll(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.age > ? EXCEPT ALL SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testExceptDistinct() throws Exception {
        final String show = person.id.count().where(person.age.gt(20L)).exceptDistinct(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.age > ? EXCEPT DISTINCT SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testIntersect() throws Exception {
        final String show = person.id.count().where(person.age.gt(20L)).intersect(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.age > ? INTERSECT SELECT COUNT(T2.parent_id) AS C1 FROM person AS T2", show);
    }

    public void testIntersectAll() throws Exception {
        final String show = person.id.count().where(person.age.gt(20L)).intersectAll(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.age > ? INTERSECT ALL SELECT COUNT(T2.parent_id) AS C2 FROM person AS T2", show);
    }

    public void testIntersectDistinct() throws Exception {
        final AbstractAggregateFunction<Integer> count = person.id.count();
        final String show = count.where(person.age.gt(20L)).intersectDistinct(person.parentId.count()).show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.age > ? INTERSECT DISTINCT SELECT COUNT(T2.parent_id) AS C2 FROM person AS T2", show);
    }

    public void testForUpdate() throws Exception {
        final String sql = person.id.count().where(person.age.gt(20L)).forUpdate().show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.age > ? FOR UPDATE", sql);
    }

    public void testForReadOnly() throws Exception {
        final String sql = person.id.count().where(person.age.gt(20L)).forReadOnly().show();
        assertSimilar("SELECT COUNT(T1.id) AS C1 FROM person AS T1 WHERE T1.age > ? FOR READ ONLY", sql);
    }

    public void testExists() throws Exception {
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.where(child.age.sum().where(child.parentId.eq(parent.id)).exists()).show();
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE EXISTS(SELECT SUM(T2.age) FROM person AS T2 WHERE T2.parent_id = T1.id)", sql);
    }

    public void testIn() throws Exception {
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.where(DynamicParameter.create(Mappers.INTEGER, 1).in(child.id.count().where(child.parentId.eq(parent.id)))).show();
        assertSimilar("SELECT T1.name AS C1 FROM person AS T1 WHERE ? IN(SELECT COUNT(T2.id) FROM person AS T2 WHERE T2.parent_id = T1.id)", sql);
    }

    public void testQueryValue() throws Exception {
        final Person parent = new Person();
        final Person child = new Person();
        final String sql = parent.name.pair(child.id.count().where(child.age.gt(20L)).queryValue()).show();
        assertSimilar("SELECT T1.name AS C1,(SELECT COUNT(T2.id) FROM person AS T2 WHERE T2.age > ?) AS C2 FROM person AS T1", sql);
    }

    public void testList() throws Exception {
        final AbstractAggregateQuerySpecification<Integer> count = person.id.count().where(person.age.gt(20L));
        final String queryString = count.show();
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setLong(1, 20L);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getInt(matches("C[0-9]"))).andReturn(123);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);

        final List<Integer> list = count.list(datasource);
        assertEquals(1, list.size());
        assertEquals(123, list.get(0).intValue());
        verify(datasource, connection, statement, resultSet);
    }

    public void testScroll() throws Exception {
        final AbstractAggregateQuerySpecification<Integer> count = person.id.count().where(person.age.gt(20L));
        final String queryString = count.show();
        final DataSource datasource = createMock(DataSource.class);
        final Connection connection = createMock(Connection.class);
        final PreparedStatement statement = createMock(PreparedStatement.class);
        final ResultSet resultSet = createMock(ResultSet.class);
        expect(datasource.getConnection()).andReturn(connection);
        expect(connection.prepareStatement(queryString)).andReturn(statement);
        statement.setLong(1, 20L);
        expect(statement.executeQuery()).andReturn(resultSet);
        expect(resultSet.next()).andReturn(true);
        expect(resultSet.getInt(matches("C[0-9]"))).andReturn(123);
        expect(resultSet.wasNull()).andReturn(false);
        expect(resultSet.next()).andReturn(false);
        resultSet.close();
        statement.close();
        connection.close();
        replay(datasource, connection,  statement, resultSet);

        count.scroll(datasource, new Callback<Integer>() {
            private int callCount = 0;
            @Override
            public boolean iterate(final Integer integer) {
                if (callCount > 0) {
                    fail("Only one call expected");
                }
                assertEquals(integer.intValue(), 123);
                return true;
            }
        });
    }





    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<Long> age = defineColumn(Mappers.LONG, "age");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Long> parentId = defineColumn(Mappers.LONG, "parent_id");
    }

    private static Person person = new Person();

}
