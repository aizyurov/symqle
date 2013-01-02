package org.simqle.sql;

import org.simqle.Element;
import org.simqle.ElementMapper;
import org.simqle.Mappers;
import org.simqle.SqlParameter;

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
    protected void set(final SqlParameter p) throws SQLException {
        p.setLong(value);
    }

    @Override
    public ElementMapper<Long> getElementMapper() {
        return Mappers.LONG;
    }
}
