package org.symqle.coretest;

import org.symqle.common.InBox;
import org.symqle.common.OutBox;
import org.symqle.common.Row;
import org.symqle.common.SqlContext;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.Option;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.Dialect;
import org.symqle.sql.GenericDialect;
import org.symqle.sql.SelectStatement;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.*;

/**
* Created by IntelliJ IDEA.
* User: aizyurov
* Date: 24.11.2013
* Time: 13:37:16
* To change this template use File | Settings | File Templates.
*/
public abstract class AbstractQueryScenario<T, StatementType extends SelectStatement<T>> {

    private final StatementType query;
    private final String columnNamePattern;
    private final Dialect dialect;
    private final Option[] options;

    AbstractQueryScenario(StatementType query, String columnNamePattern, final Dialect dialect, final Option... options) {
        this.query = query;
        this.columnNamePattern = columnNamePattern;
        this.dialect = dialect;
        this.options = options;
    }

    AbstractQueryScenario(StatementType query, String columnNamePattern) {
        this(query, columnNamePattern, new GenericDialect());
    }

    AbstractQueryScenario(StatementType query) {
        this(query, "C[0-9]");
    }

    abstract void use(final StatementType query, final QueryEngine engine) throws SQLException;

    abstract List<OutBox> parameterExpectations(final SqlParameters parameters) throws SQLException;

    abstract void elementCall(final InBox inBox) throws SQLException ;

    public final void play() throws SQLException {
        final String queryString = query.showQuery(dialect, options);
        final SqlParameters parameters = createMock(SqlParameters.class);
        final List<OutBox> parameterList = parameterExpectations(parameters);
        final Row row = createMock(Row.class);
        final InBox inBox = createMock(InBox.class);
        expect(row.getValue(matches(columnNamePattern))).andReturn(inBox);
        elementCall(inBox);
        final List<Object> mockList = new ArrayList<Object>();
        mockList.add(parameters);
        mockList.addAll(parameterList);
        mockList.add(row);
        mockList.add(inBox);
        final Object[] mocks = mockList.toArray(new Object[mockList.size()]);
        replay(mocks);
        final SqlContext context = new SqlContext.Builder().put(Dialect.class, dialect).toSqlContext();
        final MockQueryEngine engine = new MockQueryEngine(context, Collections.singletonList(row), queryString, parameters, Arrays.asList(options));

        use(query, engine);

        verify(mocks);
    }

}
