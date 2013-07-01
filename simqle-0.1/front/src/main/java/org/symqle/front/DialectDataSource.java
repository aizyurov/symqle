package org.symqle.front;

import org.symqle.jdbc.Option;
import org.symqle.sql.Dialect;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author lvovich
 */
public class DialectDataSource extends  AbstractAdaptiveDatabaseGate {
    private final DataSource dataSource;
    private final List<Option> options;

    public DialectDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
        options = Collections.emptyList();
    }

    public DialectDataSource(final DataSource dataSource, final Dialect dialect) {
        super(dialect);
        this.dataSource = dataSource;
        options = Collections.emptyList();
    }

    public DialectDataSource(final DataSource dataSource, final Dialect dialect, final Option... options) {
        super(dialect);
        this.dataSource = dataSource;
        this.options = Arrays.asList(options);
    }

    @Override
    protected Connection connect() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public List<Option> getOptions() {
        return new ArrayList<Option>(options);
    }
}
