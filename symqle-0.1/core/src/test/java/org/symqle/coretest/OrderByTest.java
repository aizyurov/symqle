package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.sql.AbstractSortOrderingSpecification;
import org.symqle.sql.AbstractSortSpecification;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 02.01.2013
 * Time: 18:04:19
 * To change this template use File | Settings | File Templates.
 */
public class OrderByTest extends SqlTestCase {

    public void testOrderByAscNullsFirst() throws Exception {
        String sql = person.id.orderBy(person.age.asc().nullsFirst()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age ASC NULLS FIRST", sql);
    }

    public void testOrderByAscNullsLast() throws Exception {
        String sql = person.id.orderBy(person.age.asc().nullsLast()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age ASC NULLS LAST", sql);
    }

    public void testOrderByMultiple() throws Exception {
        String sql = person.id.orderBy(person.name.nullsFirst(), person.age.desc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.name NULLS FIRST, T0.age DESC", sql);
    }

    public void testPairOrderByMultiple() throws Exception {
        String sql = person.id.pair(person.name).orderBy(person.name.nullsFirst(), person.age.desc()).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0, T0.name AS C1 FROM person AS T0 ORDER BY T0.name NULLS FIRST, T0.age DESC", sql);
    }

    public void testAdaptToAbstractSortOrderingSpecification() throws Exception {
        AbstractSortOrderingSpecification sort = AbstractSortOrderingSpecification.adapt(person.age);
        final String sql = person.id.orderBy(sort).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age", sql);
    }

    public void testAdaptToAbstractSortSpecification() throws Exception {
        AbstractSortSpecification sort = AbstractSortSpecification.adapt(person.age);
        final String sql = person.id.orderBy(sort).show(new GenericDialect());
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age", sql);
    }




    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
        public Column<Long> age = defineColumn(CoreMappers.LONG, "age");
    }

    private static Person person = new Person();
    private static class Employee extends TableOrView {
        @Override
        public String getTableName() {
            return "employee";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
    }

    private static Employee employee = new Employee();

}
