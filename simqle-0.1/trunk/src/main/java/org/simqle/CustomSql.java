package org.simqle;

import org.simqle.Sql;
import org.simqle.SqlParameters;

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
