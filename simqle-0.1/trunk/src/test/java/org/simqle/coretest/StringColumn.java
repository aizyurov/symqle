package org.simqle.coretest;

import org.simqle.Mappers;
import org.simqle.sql.Column;
import org.simqle.sql.Table;

/**
* Created by IntelliJ IDEA.
* User: lvovich
* Date: 23.11.12
* Time: 18:26
* To change this template use File | Settings | File Templates.
*/
public class StringColumn extends Column<String> {
    StringColumn(final String name, final Table owner) {
        super(Mappers.STRING, name, owner);
    }
}
