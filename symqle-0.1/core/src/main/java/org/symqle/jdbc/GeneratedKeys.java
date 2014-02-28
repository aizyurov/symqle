package org.symqle.jdbc;

import org.symqle.common.InBox;
import org.symqle.common.Mapper;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class GeneratedKeys<T> {

    private final Mapper<T> mapper;
    private final List<T> keys = new ArrayList<T>();

    private GeneratedKeys(final Mapper<T> mapper) {
        this.mapper = mapper;
    }

    public static <T> GeneratedKeys<T> create(final Mapper<T> mapper) {
        return new GeneratedKeys<T>(mapper);
    }

    void read(InBox inBox) throws SQLException {
        keys.add(mapper.value(inBox));
    }

    public List<T> all() {
        return Collections.unmodifiableList(keys);
    }

    public T first() {
        return keys.get(0);
    }
}
