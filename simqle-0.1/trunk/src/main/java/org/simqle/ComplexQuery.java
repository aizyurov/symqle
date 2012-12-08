package org.simqle;

import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: aizyurov
 * Date: 08.12.2012
 * Time: 23:41:34
 * To change this template use File | Settings | File Templates.
 */
public class ComplexQuery<T> extends CompositeSql implements Query<T> {
    private final DataExtractor<T> extractor;

    public ComplexQuery(DataExtractor<T> extractor, Sql first, Sql... other) {
        super(first, other);
        this.extractor = extractor;
    }

    @Override
    public T extract(Row row) throws SQLException {
        return extractor.extract(row);
    }
}
