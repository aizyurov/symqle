package org.symqle.coretest;

import org.symqle.common.Mappers;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractSelectStatement;
import org.symqle.sql.Column;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.TableOrView;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

/**
 * @author lvovich
 */
public class SelectStatementTest extends SqlTestCase {

    public void testShow() throws Exception {
        final AbstractSelectStatement<Long> selectStatement = person.id.forUpdate();
        final String sql = selectStatement.show(new GenericDialect());
        final String sql2 = selectStatement.show(new GenericDialect());
        assertSimilar("SELECT T1.id AS C1 FROM person AS T1 FOR UPDATE", sql);
        assertSimilar(sql, sql2);
    }



    public void testList() throws Exception {
        new Scenario123<AbstractSelectStatement<Long>>(person.id.forUpdate()) {
            @Override
            void use(AbstractSelectStatement<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(getExpected(), query.list(engine));
            }
        }.play();
    }

    public void testScroll() throws Exception {
        new Scenario123<AbstractSelectStatement<Long>>(person.id.forUpdate()) {
            @Override
            void use(AbstractSelectStatement<Long> query, QueryEngine engine) throws SQLException {
                assertEquals(1, query.scroll(engine, getCallback()));
            }
        }.play();
    }

    private static class Person extends TableOrView {
        private Person() {
            super("person");
        }
        public Column<Long> id = defineColumn(Mappers.LONG, "id");
    }

    private static Person person = new Person();

}
