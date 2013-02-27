package org.simqle.hsqldb;

import junit.framework.TestCase;
import org.hsqldb.jdbcDriver;
import org.simqle.Mappers;
import org.simqle.Pair;
import org.simqle.sql.Column;
import org.simqle.sql.TableOrView;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;

/**
 * @author lvovich
 */
public class ColumnTest extends TestCase {


    @Override
    public void setUp() throws Exception {
        DriverManager.registerDriver(new jdbcDriver());
        final Connection connection = dataSource.getConnection();
        final PreparedStatement createStatement = connection.prepareStatement("CREATE TABLE person (id BIGINT PRIMARY KEY, name VARCHAR, age INTEGER, alive BOOLEAN)");
        createStatement.executeUpdate();
        createStatement.close();
        final PreparedStatement insert = connection.prepareStatement("INSERT INTO person (id, name, age, alive) VALUES(?,?,?,?)");
        insert.setLong(1, 1);
        insert.setString(2, "Alice");
        insert.setInt(3, 23);
        insert.setBoolean(4, true);
        insert.addBatch();

        insert.setLong(1, 2);
        insert.setString(2, "Bob");
        insert.setInt(3, 46);
        insert.setBoolean(4, true);
        insert.addBatch();

        insert.setLong(1, 3);
        insert.setString(2, "Leonardo");
        insert.setInt(3, 546);
        insert.setBoolean(4, false);
        insert.addBatch();

        assertEquals(3, insert.executeBatch().length);
        insert.close();

    }

    @Override
    public void tearDown() throws Exception {
        final Connection connection = dataSource.getConnection();
        final PreparedStatement createStatement = connection.prepareStatement("DROP TABLE person");
        createStatement.executeUpdate();
        createStatement.close();
    }

    private DataSource dataSource = new DriverManagerDataSource("jdbc:hsqldb:mem:simqle", "SA", "");

    public void testSelect() throws Exception {
        Person person = new Person();
        final List<Pair<String,Integer>> list = person.name.pair(person.age).list(dataSource);
        assertEquals(3, list.size());
        System.out.println(list);
        assertTrue(list.contains(Pair.of("Alice", 23)));
    }

    public void testWhereEq() throws Exception {
        Person person = new Person();
        final List<Pair<String,Integer>> list = person.name.pair(person.age).where(person.id.eq(2L)).list(dataSource);
        assertEquals(1, list.size());
        assertTrue(Pair.of("Bob", 46).equals(list.get(0)));
    }

    public void testWhereLike() throws Exception {
        Person person = new Person();
        final List<Pair<String,Integer>> list = person.name.pair(person.age).where(person.name.like("Bo_")).list(dataSource);
        assertEquals(1, list.size());
        assertTrue(Pair.of("Bob", 46).equals(list.get(0)));
    }

    public void testWhereBoolean() throws Exception {
        Person person = new Person();
        final List<Pair<String,Integer>> list = person.name.pair(person.age).where(person.alive.booleanValue().negate()).list(dataSource);
        assertEquals(1, list.size());
        assertTrue(Pair.of("Leonardo", 546).equals(list.get(0)));
    }

    public void testSubquery() throws Exception {
        Person person = new Person();
        Person sample = new Person();
        final List<Pair<String,Integer>> list = person.name.pair(person.age).where(person.id.in(sample.id.where(sample.name.eq("Leonardo")))).list(dataSource);
        assertEquals(1, list.size());
        assertTrue(Pair.of("Leonardo", 546).equals(list.get(0)));
    }

    public void testSubqueryAsValue() throws Exception {
        Person person = new Person();
        Person sample = new Person();
        final List<Pair<String,Integer>> list = person.name.pair(sample.age.where(sample.id.eq(person.id)).queryValue()).where(person.age.gt(500)).list(dataSource);
        assertEquals(1, list.size());
        assertTrue(Pair.of("Leonardo", 546).equals(list.get(0)));

    }

    public void testPairOfPairs() throws Exception {
        Person person = new Person();
        final List<Pair<Pair<Long, String>, Pair<Integer, Boolean>>> list = person.id.pair(person.name).pair(person.age.pair(person.alive)).where(person.age.notLike("%6")).list(dataSource);
        assertEquals(1, list.size());
        assertEquals(Pair.of(Pair.of(1L, "Alice"), Pair.of(23, true)), list.get(0));
    }

    public void testCaseExpression() throws Exception {
        Person person = new Person();
        final List<String> list = person.alive.booleanValue()
                .then(person.name)
                .orElse(person.name.concat(" +"))
                .orderBy(person.name.desc()).list(dataSource);
        assertEquals(Arrays.asList("Leonardo +", "Bob", "Alice"), list);
    }

    public void testUnion() throws Exception {
        Person person = new Person();
        final List<String> list = person.name.unionAll(person.name.concat(" ").concat(person.age)).list(dataSource);
        assertEquals(Arrays.asList("Alice", "Bob", "Leonardo", "Alice 23", "Bob 46", "Leonardo 546"), list);

    }

    public void testExcept() throws Exception {
        Person person = new Person();
        final List<String> list = person.name.except(person.name.where(person.alive.booleanValue().negate())).list(dataSource);
        assertEquals(Arrays.asList("Alice", "Bob"), list);
    }

    private class Person extends TableOrView {
        private Person() {
            super("person");
        }

        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
        public Column<Integer> age = defineColumn(Mappers.INTEGER, "age");
        public Column<Boolean> alive = defineColumn(Mappers.BOOLEAN, "alive");
    }
}
