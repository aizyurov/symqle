package org.symqle.coretest;

import org.symqle.common.Callback;
import org.symqle.common.Element;
import org.symqle.common.SqlParameter;
import org.symqle.common.SqlParameters;
import org.symqle.jdbc.QueryEngine;
import org.symqle.sql.AbstractQueryBaseScalar;
import org.symqle.sql.SelectStatement;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.easymock.EasyMock.expect;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 24.11.2013
 * Time: 16:05:44
 * To change this template use File | Settings | File Templates.
 */
public abstract class Scenario123<StatementType extends SelectStatement<Long>> extends AbstractQueryScenario<Long, StatementType> {

    protected final List<Long> getExpected() {
        return Arrays.asList(123L);
    }

    protected final Callback<Long> getCallback() {
        return new TestCallback<Long>(123L);
    }

    protected Scenario123(StatementType query) {
        super(query);
    }

    @Override
    List<SqlParameter> parameterExpectations(SqlParameters parameters) throws SQLException {
        return Collections.emptyList();
    }

    @Override
    void elementCall(Element element) throws SQLException {
        expect(element.getLong()).andReturn(123L);
    }

}
