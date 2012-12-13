package org.simqle.sql;

import org.simqle.Element;

import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: lvovich
 * Date: 21.11.12
 * Time: 18:37
 * To change this template use File | Settings | File Templates.
 */
public class AliveTest extends SqlTestCase {


    public void testFunction() throws Exception {
        final Person person = new Person();
        final StringColumn name = new StringColumn("name", person);
        final StringColumn surname = new StringColumn("surname", person);
        assertSimilar("SELECT concat(T1.name, T1.surname) AS C1 FROM person AS T1", concat(name, surname).show());

    }

    private static class Person extends Table {
        private Person() {
            super("person");
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

    private AbstractRoutineInvocation<String> concat(ValueExpression<String> v1, ValueExpression<String> v2) {
        return new Concat().apply(v1, v2);
    }

    private static class Concat extends FunctionCall<String> {
        public Concat() {
            super("concat");
        }

        @Override
        public String value(final Element element) throws SQLException {
            return element.getString();
        }
    }
}
