package org.simqle.sql;

import org.simqle.Element;

import java.sql.SQLException;

/**
* Created by IntelliJ IDEA.
* User: lvovich
* Date: 23.11.12
* Time: 18:27
* To change this template use File | Settings | File Templates.
*/
public class LongColumn extends Column<Long> {
    LongColumn(final String name, final Table owner) {
        super(name, owner);
    }

    @Override
    public Long value(final Element element) throws SQLException {
        return element.getLong();
    }
}
