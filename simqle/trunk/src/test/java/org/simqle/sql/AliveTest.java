package org.simqle.sql;

import junit.framework.TestCase;
import org.simqle.ColumnNameProvider;
import org.simqle.Element;
import org.simqle.SqlContext;

import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: lvovich
 * Date: 21.11.12
 * Time: 18:37
 * To change this template use File | Settings | File Templates.
 */
public class AliveTest extends TestCase {

    public void test() throws Exception {
        final Person person = new Person();
        final LongColumn col = new LongColumn("id", person);
        Element element = new ElementAdapter() {
            @Override
            public Long getLong() throws SQLException {
                return 1L;
            }
        };
        assertEquals(Long.valueOf(1), col.value(element));
        final SqlContext context = new SqlContext();
        context.set(FromClause.class, new FromClause());
        context.set(ColumnNameProvider.class, new ColumnNameProvider());
        context.set(SqlFactory.class, new GenericSqlFactory());
        col.z$prepare$zSelectStatement(context);
        System.out.println(col.z$create$zSelectStatement(context).getSqlText());

    }

    private static class Person extends Table {
        private Person() {
            super("person");
        }


    }

    private static class LongColumn extends Column<Long> {
        private LongColumn(final String name, final Table owner) {
            super(name, owner);
        }

        @Override
        public Long value(final Element element) throws SQLException {
            return element.getLong();
        }
    }
}
