package org.symqle.coretest;

import org.symqle.common.CoreMappers;
import org.symqle.sql.AbstractInValueList;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

/**
 * @author lvovich
 */
public class InValueListTest extends SqlTestCase {

    public void testAdapt() throws Exception {
        final String sql = person.id.where(person.name.in(AbstractInValueList.adapt(person.nick.asInValueList()))).showQuery(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 WHERE T1.name IN(T1.nick)", sql);
    }

    private static class Person extends TableOrView {
        @Override
        public String getTableName() {
            return "person";
        }
        public Column<Long> id = defineColumn(CoreMappers.LONG, "id");
        public Column<String> name = defineColumn(CoreMappers.STRING, "name");
        public Column<String> nick = defineColumn(CoreMappers.STRING, "nick");
    }

    private static Person person = new Person();
}
