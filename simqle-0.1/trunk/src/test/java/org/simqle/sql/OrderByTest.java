package org.simqle.sql;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static org.easymock.EasyMock.*;
import static org.easymock.EasyMock.verify;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 02.01.2013
 * Time: 18:04:19
 * To change this template use File | Settings | File Templates.
 */
public class OrderByTest extends SqlTestCase {

    public void testOrderByAscNullsFirst() throws Exception {
        String sql = person.id.orderBy(person.age.asc().nullsFirst()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age ASC NULLS FIRST", sql);
    }

    public void testOrderByAscNullsLast() throws Exception {
        String sql = person.id.orderBy(person.age.asc().nullsLast()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.age ASC NULLS LAST", sql);
    }

    public void testOrderByMultiple() throws Exception {
        String sql = person.id.orderBy(person.name.nullsFirst(), person.age.desc()).show();
        assertSimilar("SELECT T0.id AS C0 FROM person AS T0 ORDER BY T0.name NULLS FIRST, T0.age DESC", sql);
    }



    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
        public Column<Long> age = new LongColumn("age", this);
    }

    private static Person person = new Person();
    private static class Employee extends Table {
        private Employee() {
            super("employee");
        }
        public Column<Long> id = new LongColumn("id", this);
        public Column<String> name = new StringColumn("name", this);
        public Column<Long> age = new LongColumn("age", this);
    }

    private static Employee employee = new Employee();

}
