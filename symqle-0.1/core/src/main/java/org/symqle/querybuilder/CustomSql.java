package org.symqle.querybuilder;

import org.symqle.common.Sql;
import org.symqle.common.SqlParameters;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 09.12.2012
 * Time: 21:45:31
 * To change this template use File | Settings | File Templates.
 */
public class CustomSql implements Sql {
    final String text;

    public CustomSql(String text) {
        this.text = text;
    }

    @Override
    public String getSqlText() {
        return text;
    }

    @Override
    public void setParameters(SqlParameters p) {
        // do nothing
    }
}
