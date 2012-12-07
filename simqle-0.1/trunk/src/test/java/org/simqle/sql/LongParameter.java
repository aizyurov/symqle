package org.simqle.sql;

import org.simqle.Element;
import org.simqle.SqlParameters;

import java.sql.SQLException;

/**
* Created by IntelliJ IDEA.
* User: lvovich
* Date: 23.11.12
* Time: 18:34
* To change this template use File | Settings | File Templates.
*/
public class LongParameter extends DynamicParameter<Long> {
    private final Long value;

    LongParameter(final Long value) {
        this.value = value;
    }

    @Override
    protected void setParameter(final SqlParameters p) {
        p.setLong(value);
    }

    @Override
    public Long value(final Element element) throws SQLException {
        return element.getLong();
    }
}
