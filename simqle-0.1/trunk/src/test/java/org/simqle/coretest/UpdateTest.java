package org.simqle.coretest;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.Table;

/**
 * @author lvovich
 */
public class UpdateTest extends SqlTestCase {

    public void testOneColumn() throws Exception {
        final String sql = person.update(person.parentId.set(person.id)).show();
        assertSimilar("UPDATE person SET parent_id = person.id", sql);
    }

    public void testSetNull() throws Exception {
        final String sql = person.update(person.parentId.setNull()).show();
        assertSimilar("UPDATE person SET parent_id = NULL", sql);
    }

    public void testSetDefault() throws Exception {
        final String sql = person.update(person.parentId.setDefault()).show();
        assertSimilar("UPDATE person SET parent_id = DEFAULT", sql);
    }

    public void testMultipleColumns() throws Exception {
        final String sql = person.update(person.parentId.set(person.id), person.name.set("John")).show();
        assertSimilar("UPDATE person SET parent_id = person.id, name = ?", sql);
    }

    public void testWhere() throws Exception {
        final String sql = person.update(person.parentId.set(person.id), person.name.set("John")).where(person.id.eq(1L)).show();
        assertSimilar("UPDATE person SET parent_id = person.id, name = ? WHERE person.id = ?", sql);
    }

    public void testSubqueryInWhere() throws Exception {
        final Person children = new Person();
        final String sql = person.update(person.parentId.set(person.id)).where(children.id.where(children.parentId.eq(person.id)).exists()).show();
        assertSimilar("UPDATE person SET parent_id = person.id WHERE EXISTS(SELECT T0.id FROM person AS T0 WHERE T0.parent_id = person.id)", sql);
    }

    private static class Person extends Table {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
        public Column<Long> age = defineColumn(Mappers.LONG, "age");
        public Column<Long> parentId = defineColumn(Mappers.LONG, "parent_id");
        public Column<String> name = defineColumn(Mappers.STRING, "name");
    }

    private static Person person = new Person();


}
