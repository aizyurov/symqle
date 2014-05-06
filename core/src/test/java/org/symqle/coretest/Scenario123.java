package org.symqle.coretest;

import org.symqle.common.Callback;
import org.symqle.common.InBox;
import org.symqle.common.OutBox;
import org.symqle.common.SqlParameters;
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
    List<OutBox> parameterExpectations(SqlParameters parameters) throws SQLException {
        return Collections.emptyList();
    }

    @Override
    void elementCall(InBox inBox) throws SQLException {
        expect(inBox.getLong()).andReturn(123L);
    }

}
