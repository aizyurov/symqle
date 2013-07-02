package org.symqle.querybuilder;

import org.symqle.common.Row;
import org.symqle.common.RowMapper;

/**
 * Created by IntelliJ IDEA.
 * User: lvovich
 * Date: 21.10.11
 * Time: 12:14
 * To change this template use File | Settings | File Templates.
 */
public class NullRowMapper<T> implements RowMapper<T> {
    @Override
    public T extract(final Row row) {
        // must never get here
        throw new RuntimeException("Not implemented");
    }
}
