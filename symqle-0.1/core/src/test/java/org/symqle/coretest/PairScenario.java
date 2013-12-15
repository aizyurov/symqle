package org.symqle.coretest;

import org.symqle.common.*;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.SelectStatement;
import org.symqle.sql.Symqle;

import java.sql.SQLException;
import java.util.Collections;

import static org.easymock.EasyMock.*;

/**
* Created by IntelliJ IDEA.
* User: aizyurov
* Date: 15.12.2013
* Time: 19:25:58
* To change this template use File | Settings | File Templates.
*/
public abstract class PairScenario<StatementType extends SelectStatement<Pair<Long,String>>>  {
    private final StatementType query;

    protected abstract void use(
            final StatementType query,
            final QueryEngine engine)  throws SQLException;

    PairScenario(StatementType query) {
        this.query = query;
    }

    public void play () throws SQLException {
        final String queryString = Symqle.show(query, new GenericDialect());
        final SqlParameters parameters = createMock(SqlParameters.class);
        final Row row = createMock(Row.class);
        final Element element1 = createMock(Element.class);
        final Element element2 = createMock(Element.class);
        expect(row.getValue(matches("C[0-9]"))).andReturn(element1);
        expect(row.getValue(matches("C[0-9]"))).andReturn(element2);
        expect(element1.getLong()).andReturn(123L);
        expect(element2.getString()).andReturn("John");
        final Object[] mocks = new Object[] {parameters, row, element1, element2};
        replay(mocks);
        final SqlContext context = new SqlContext.Builder().toSqlContext();
        final MockQueryEngine engine = new MockQueryEngine(context, Collections.singletonList(row), queryString, parameters);

        use(query, engine);

        verify(mocks);
    }
}
