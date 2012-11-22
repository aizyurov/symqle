package org.simqle.sql;

import junit.framework.TestCase;
import org.simqle.Element;

import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: lvovich
 * Date: 21.11.12
 * Time: 20:50
 * To change this template use File | Settings | File Templates.
 */
public class ColumnTest extends TestCase {


    public void testValueFunctionality() throws Exception {
        final LongColumn col = createId();
        Element element = new ElementAdapter() {
            @Override
            public Long getLong() throws SQLException {
                return 1L;
            }
        };
        assertEquals(Long.valueOf(1), col.value(element));
        assertEquals("SELECT T0.id AS C0 FROM person AS T0", col.show());

    }

    public void testSelectStatementFunctionality() throws Exception {
        final LongColumn col = createId();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0", col.show());
    }

    private LongColumn createId() {
        return new LongColumn("id", person);
    }

    private LongColumn createAge() {
        return new LongColumn("age", person);
    }

    public void testSelectAll() throws Exception {
        final LongColumn col = createId();
        assertEquals("SELECT ALL T0.id AS C0 FROM person AS T0", col.all().show());

    }

    public void testSelectDistinct() throws Exception {
        final LongColumn col = createId();
        assertEquals("SELECT DISTINCT T0.id AS C0 FROM person AS T0", col.distinct().show());
    }

    public void testAsFunctionArgument() throws Exception {
        final String sql = new Function<Long>("abs") {
            @Override
            public Long value(final Element element) throws SQLException {
                return element.getLong();
            }
        }.apply(createId()).show();
        assertEquals("SELECT abs(T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testAsFunctionMultipleArguments() throws Exception {
        final LongColumn column = createId();
        final String sql = new Function<Long>("max") {
            @Override
            public Long value(final Element element) throws SQLException {
                return element.getLong();
            }
        }.apply(column, column).show();
        assertEquals("SELECT max(T0.id, T0.id) AS C0 FROM person AS T0", sql);
    }

    public void testAsCondition() throws Exception {
        final LongColumn id = createId();
        final String sql = id.where(id.asCondition()).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id", sql);
    }

    public void testEq() throws Exception {
        final LongColumn id = createId();
        final LongColumn age = createAge();
        final String sql = id.where(id.eq(age)).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id = T0.age", sql);
    }

    public void testNe() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(column.ne(age)).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id <> T0.age", sql);
    }

    public void testGt() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(column.gt(age)).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id > T0.age", sql);
    }

    public void testGe() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(column.ge(age)).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id >= T0.age", sql);
    }

    public void testLt() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(column.lt(age)).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id < T0.age", sql);
    }

    public void testLe() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.where(column.le(age)).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 WHERE T0.id <= T0.age", sql);
    }

    public void testExceptAll() throws Exception {
        final LongColumn column = createId();
        final String sql = column.exceptAll(new LongColumn("age", person2)).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 EXCEPT ALL SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testExceptDistinct() throws Exception {
        final LongColumn column = createId();
        final String sql = column.exceptDistinct(new LongColumn("age", person2)).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 EXCEPT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnionAll() throws Exception {
        final LongColumn column = createId();
        final String sql = column.unionAll(new LongColumn("age", person2)).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 UNION ALL SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testUnionDistinct() throws Exception {
        final LongColumn column = createId();
        final String sql = column.unionDistinct(new LongColumn("age", person2)).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 UNION DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);
    }

    public void testIntersectAll() throws Exception {
        final LongColumn column = createId();
        final String sql = column.intersectAll(new LongColumn("age", person2)).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 INTERSECT ALL SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testIntersectDistinct() throws Exception {
        final LongColumn column = createId();
        final String sql = column.intersectDistinct(new LongColumn("age", person2)).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    public void testUseSameTableInDistinct() throws Exception {
        final LongColumn column = createId();
        final LongColumn age = createAge();
        final String sql = column.intersectDistinct(age).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 INTERSECT DISTINCT SELECT T1.age AS C0 FROM person AS T1", sql);

    }

    private static class Person extends Table {
        private Person() {
            super("person");
        }
    }

    private static Person person = new Person();

    private static Person person2 = new Person();

    private static class LongColumn extends Column<Long> {
        private LongColumn(final String name, final Table owner) {
            super(name, owner);
        }

        @Override
        public Long value(final Element element) throws SQLException {
            return element.getLong();
        }
    }

    private static class StringColumn extends Column<String> {
        private StringColumn(final String name, final Table owner) {
            super(name, owner);
        }

        @Override
        public String value(final Element element) throws SQLException {
            return element.getString();
        }
    }

    public void testSelectForUpdate() throws Exception {
        final LongColumn col = createId();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 FOR UPDATE", col.forUpdate().show());
    }

    public void testSelectForReadOnly() throws Exception {
        final LongColumn col = createId();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 FOR READ ONLY", col.forReadOnly().show());
    }

    public void testExists() throws Exception {
        final LongColumn id  =  createId();
        final LongColumn age = createAge();
        // find all but the most old
        final LongColumn age2 = new LongColumn("age", person2);
        String sql = id.where(age2.where(age2.gt(age)).exists()).show();
        assertEquals("SELECT T0.id AS C0 FROM person AS T0 WHERE EXISTS(SELECT T1.age FROM person AS T1 WHERE T1.age > T0.age)", sql);

    }

}
