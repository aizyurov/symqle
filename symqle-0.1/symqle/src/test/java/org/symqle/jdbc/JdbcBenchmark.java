package org.symqle.jdbc;

import org.symqle.common.Row;
import org.symqle.common.RowMapper;
import org.symqle.integration.AbstractIntegrationTestBase;
import org.symqle.integration.model.InsertTable;
import org.symqle.sql.AbstractInsertStatement;
import org.symqle.sql.AbstractSelector;
import org.symqle.sql.DynamicParameter;
import org.symqle.sql.SmartSelector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lvovich
 */
public class JdbcBenchmark extends AbstractIntegrationTestBase {

    private static final int NUM_ITERATIONS = 50000;

    public void testExecutePerformance() throws Exception {
        final int warmIterations = 5;
        for (int i=0; i< warmIterations; i++) {
            cleanup();
            jdbcInsert();
        }

        for (int i=0; i< warmIterations; i++) {
            cleanup();
            symqleInsert();
        }

        for (int i=0; i< warmIterations; i++) {
            cleanup();
            symqlePrecompiled();
        }

        for (int i=0; i< warmIterations; i++) {
            cleanup();
            symqleBatching();
        }

        for (int i=0; i< warmIterations; i++) {
            cleanup();
            symqlePrecompiledBatching();
        }

    }

    public void testFetchPerformance() throws Exception {
        cleanup();
        // Direct JDBC
        jdbcInsert();

        final Engine engine = getEngine();
        final InsertTable insertTable = new InsertTable();
        for (int i=0; i<10; i++) {
            final long start = System.nanoTime();
            final List<InsertDto> list = new InsertSelector(insertTable).list(engine);
            assertEquals(NUM_ITERATIONS, list.size());
            final long end = System.nanoTime();
            System.out.println("Symqle fetch: " + (end - start) / NUM_ITERATIONS + " nanos");
        }
        for (int i=0; i<10; i++) {
            final long start = System.nanoTime();
            final List<InsertDto> list = new InsertFullSelector(insertTable).list(engine);
            assertEquals(NUM_ITERATIONS, list.size());
            final long end = System.nanoTime();
            System.out.println("Symqle fetch full: " + (end - start) / NUM_ITERATIONS + " nanos");
        }
        for (int i=0; i<10; i++) {
            final long start = System.nanoTime();
            final List<InsertDto> list = jdbcList();
            assertEquals(NUM_ITERATIONS, list.size());
            final long end = System.nanoTime();
            System.out.println("JDBC fetch: " + (end - start) / NUM_ITERATIONS + " nanos");
        }
    }

    private List<InsertDto> jdbcList() throws Exception {
        final Connection connection = getDataSource().getConnection();
        final PreparedStatement preparedStatement = connection.prepareStatement("SELECT id, text, active, payload FROM insert_test");
        final ResultSet resultSet = preparedStatement.executeQuery();
        final List<InsertDto> list = new ArrayList<>();
        while (resultSet.next()) {
            list.add(new InsertDto(resultSet.getInt(1), resultSet.getString(2), resultSet.getBoolean(3), resultSet.getInt(4)));
        }
        return list;
    }

    private static class InsertDto {
        private final Integer id;
        private final String text;
        private final Boolean active;
        private final Integer payload;

        private InsertDto(final Integer id, final String text, final Boolean active, final Integer payload) {
            this.id = id;
            this.text = text;
            this.active = active;
            this.payload = payload;
        }
    }

    private static class InsertFullSelector extends AbstractSelector<InsertDto> {
        private final RowMapper<Integer> id;
        private final RowMapper<String> text;
        private final RowMapper<Boolean> active;
        private final RowMapper<Integer> payload;

        private InsertFullSelector(final InsertTable insertTable) {
            id = map(insertTable.id);
            text = map(insertTable.text);
            active = map(insertTable.active);
            payload = map(insertTable.payload);
        }

        @Override
        protected InsertDto create(final Row row) throws SQLException {
            return new InsertDto(id.extract(row), text.extract(row), active.extract(row), payload.extract(row));
        }
    }

    private static class InsertSelector extends SmartSelector<InsertDto> {
        private final InsertTable insertTable;

        private InsertSelector(final InsertTable insertTable) {
            this.insertTable = insertTable;
        }

        @Override
        protected InsertDto create() throws SQLException {
            return new InsertDto(get(insertTable.id), get(insertTable.text), get(insertTable.active), get(insertTable.payload));
        }
    }

    private void symqleInsert() throws SQLException {
        final long start = System.nanoTime();
        final Engine engine = getEngine();
        final InsertTable insertTable = new InsertTable();
        for (int i=0; i<NUM_ITERATIONS; i++) {
            insertTable.insert(
                    insertTable.id.set(i)
                    .also(insertTable.text.set(String.valueOf(i)))
                    .also(insertTable.active.set((i & 1) == 0))
                    .also(insertTable.payload.set(i))
            ).execute(engine);
        }
        final long end = System.nanoTime();
        System.out.println("Symqle compile: " + (end - start) / NUM_ITERATIONS + " nanos");
    }

    public void testCompilation() throws Exception {
        final Engine engine = getEngine();
        final InsertTable insertTable = new InsertTable();
        for (int k=0; k<40; k++) {
            final long start = System.nanoTime();
            for (int i = 0; i<NUM_ITERATIONS; i++)
            insertTable.insert(
                                insertTable.id.set(i)
                                .also(insertTable.text.set(String.valueOf(i)))
                                .also(insertTable.active.set((i & 1) == 0))
                                .also(insertTable.payload.set(i))
                        ).compileUpdate(engine);
            final long end = System.nanoTime();
            System.out.println("Symqle compile: " + (end - start) / NUM_ITERATIONS + " nanos");
        }
    }

    private void symqlePrecompiled() throws SQLException {
        final long start = System.nanoTime();
        final Engine engine = getEngine();
        final InsertTable insertTable = new InsertTable();
        final DynamicParameter<Integer> idParam = insertTable.id.param();
        final DynamicParameter<String> textParam = insertTable.text.param();
        final DynamicParameter<Boolean> activeParam = insertTable.active.param();
        final DynamicParameter<Integer> payloadParam = insertTable.payload.param();
        final AbstractInsertStatement insert = insertTable.insert(
                insertTable.id.set(idParam)
                        .also(insertTable.text.set(textParam))
                        .also(insertTable.active.set(activeParam))
                        .also(insertTable.payload.set(payloadParam))
        );
        final PreparedUpdate preparedUpdate = insert.compileUpdate(engine);
        for (int i=0; i<NUM_ITERATIONS; i++) {
            idParam.setValue(i);
            textParam.setValue(String.valueOf(i));
            activeParam.setValue((i & 1) == 0);
            payloadParam.setValue(i);
            preparedUpdate.execute();
        }
        final long end = System.nanoTime();
        System.out.println("Symqle prepared: " + (end - start) / NUM_ITERATIONS  + " nanos");
    }


    private void jdbcInsert() throws SQLException {
        final long start = System.nanoTime();
        final Connection connection = getDataSource().getConnection();
        for (int i=0; i<NUM_ITERATIONS; i++) {
            final PreparedStatement insert = connection.prepareStatement("INSERT INTO insert_test(id, text, active, payload) VALUES(?,?,?,?)");
            insert.setInt(1, i);
            insert.setString(2, String.valueOf(i));
            insert.setBoolean(3, (i & 1) == 0);
            insert.setInt(4, i);
            insert.execute();
            insert.close();
        }
        connection.close();
        final long end = System.nanoTime();
        System.out.println("Plain JDBC: " + (end - start) / NUM_ITERATIONS  + " nanos");
    }

    private void cleanup() throws SQLException {
        final Connection connection = getDataSource().getConnection();
        final PreparedStatement cleanup = connection.prepareStatement("DELETE FROM insert_test");
        cleanup.executeUpdate();
        cleanup.close();
        connection.close();
    }

    private void symqleBatching() throws Exception {
        final long start = System.nanoTime();
        final Engine engine = getEngine();
        final Batcher batcher = engine.newBatcher(1000);
        final InsertTable insertTable = new InsertTable();
        for (int i=0; i<NUM_ITERATIONS; i++) {
            insertTable.insert(
                    insertTable.id.set(i)
                    .also(insertTable.text.set(String.valueOf(i)))
                    .also(insertTable.active.set((i & 1) == 0))
                    .also(insertTable.payload.set(i))
            ).submit(batcher);
        }
        batcher.flush();
        final long end = System.nanoTime();
        System.out.println("Symqle batching: " + (end - start) / NUM_ITERATIONS + " nanos");
    }

    private void symqlePrecompiledBatching() throws SQLException {
        final long start = System.nanoTime();
        final Engine engine = getEngine();
        final InsertTable insertTable = new InsertTable();
        final DynamicParameter<Integer> idParam = insertTable.id.param();
        final DynamicParameter<String> textParam = insertTable.text.param();
        final DynamicParameter<Boolean> activeParam = insertTable.active.param();
        final DynamicParameter<Integer> payloadParam = insertTable.payload.param();
        final AbstractInsertStatement insert = insertTable.insert(
                insertTable.id.set(idParam)
                        .also(insertTable.text.set(textParam))
                        .also(insertTable.active.set(activeParam))
                        .also(insertTable.payload.set(payloadParam))
        );
        final PreparedUpdate preparedUpdate = insert.compileUpdate(engine);
        final Batcher batcher = engine.newBatcher(1000);
        for (int i=0; i<NUM_ITERATIONS; i++) {
            idParam.setValue(i);
            textParam.setValue(String.valueOf(i));
            activeParam.setValue((i & 1) == 0);
            payloadParam.setValue(i);
            preparedUpdate.submit(batcher);
        }
        batcher.flush();
        final long end = System.nanoTime();
        System.out.println("Symqle prepared batching: " + (end - start) / NUM_ITERATIONS  + " nanos");
    }

}
