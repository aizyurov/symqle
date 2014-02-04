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
        final InBox inBox1 = createMock(InBox.class);
        final InBox inBox2 = createMock(InBox.class);
        expect(row.getValue(matches("C[0-9]"))).andReturn(inBox1);
        expect(row.getValue(matches("C[0-9]"))).andReturn(inBox2);
        expect(inBox1.getLong()).andReturn(123L);
        expect(inBox2.getString()).andReturn("John");
        final Object[] mocks = new Object[] {parameters, row, inBox1, inBox2};
        replay(mocks);
        final SqlContext context = new SqlContext.Builder().toSqlContext();
        final MockQueryEngine engine = new MockQueryEngine(context, Collections.singletonList(row), queryString, parameters);

        use(query, engine);

        verify(mocks);
    }
}
