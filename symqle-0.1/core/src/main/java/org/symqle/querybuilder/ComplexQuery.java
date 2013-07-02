package org.symqle.querybuilder;

import org.symqle.common.Query;
import org.symqle.common.Row;
import org.symqle.common.RowMapper;
import org.symqle.common.Sql;
import org.symqle.common.SqlParameters;

import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 08.12.2012
 * Time: 23:41:34
 * To change this template use File | Settings | File Templates.
 */
public class ComplexQuery<T> implements Query<T> {
    final RowMapper<T> extractor;
    final Sql sql;

    public ComplexQuery(RowMapper<T> rowMapper, Sql sql) {
        this.extractor = rowMapper;
        this.sql = sql;
    }

    /**
     * Creates a JavaType object frm Row data.
     *
     * @param row the Row providing the data
     * @return constructed JAvaType object
     */
    public T extract(Row row) throws SQLException {
        return extractor.extract(row);
    }

    @Override
    public String getSqlText() {
        return sql.getSqlText();
    }

    @Override
    public void setParameters(final SqlParameters p) throws SQLException {
        sql.setParameters(p);
    }
}
