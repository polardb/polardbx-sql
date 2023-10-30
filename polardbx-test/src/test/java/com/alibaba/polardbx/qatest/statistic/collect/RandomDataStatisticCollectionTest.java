package com.alibaba.polardbx.qatest.statistic.collect;

import com.alibaba.polardbx.qatest.dql.sharding.type.numeric.NumericTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.google.common.collect.Lists;
import org.glassfish.jersey.internal.guava.Sets;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.dql.sharding.type.numeric.NumericTestBase.SQL_CREATE_TEST_TABLE;
import static com.alibaba.polardbx.qatest.statistic.collect.AccuracyQuantifier.FREQUENCY_SIZE;

/**
 * collection test by mock data
 *
 * @author fangwu
 */
public class RandomDataStatisticCollectionTest extends StatisticCollectionTest {

    private static final String RANDOM_STATISTIC_COLLECTION_DB = "random_statistic_collection_db";
    private static final String RANDOM_STATISTIC_COLLECTION_TABLE = "random_statistic_collection_table";
    private static final String INSERT_SQL = "insert into %s(%s) values(%s)";
    private static int BATCH_SIZE = 10000;

    public RandomDataStatisticCollectionTest(String schema, String tbl, String rows) {
        super(schema, tbl, rows);
    }

    @Parameterized.Parameters(name = "{index}: [{0}] {1} {2}")
    public static Iterable<Object[]> params() throws SQLException {
        List<Object[]> params = Lists.newLinkedList();
        try (Connection c = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            IntStream.builder()
                .add(50000)
                .add(500000)
                .add(2000000)
                .build()
                .forEach(rows -> prepareData(params, c, rows));
            log.info("RandomData statistic collection test:" + params.size());
            return params;
        }
    }

    private static void prepareData(List<Object[]> params, Connection c, int rows) {
        // build schema&table
        String tableName = RANDOM_STATISTIC_COLLECTION_TABLE + "_" + rows;
        try {
            c.createStatement()
                .execute("create database if not exists " + RANDOM_STATISTIC_COLLECTION_DB + " mode=auto");

            c.createStatement().execute("use " + RANDOM_STATISTIC_COLLECTION_DB);
            c.createStatement().execute("drop table if exists " + tableName);
            c.createStatement().execute(String.format(SQL_CREATE_TEST_TABLE, tableName, ""));

            // INSERT MOCK DATA
            mockData(RANDOM_STATISTIC_COLLECTION_DB, tableName, rows);
        } catch (SQLException e) {
            throw new RuntimeException("prepare data error, test out", e);
        }
        params.add(new Object[] {RANDOM_STATISTIC_COLLECTION_DB, tableName, rows + ""});
    }

    private static void mockData(String randomStatisticCollectionDb, String randomStatisticCollectionTable, long rows)
        throws SQLException {
        try (Connection c = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            // build schema&table
            c.createStatement().execute("use " + randomStatisticCollectionDb);
            ResultSet rs = c.createStatement().executeQuery("desc " + randomStatisticCollectionTable);

            List<Supplier<Object>> colGenerators = Lists.newLinkedList();
            StringBuilder cols = new StringBuilder();
            StringBuilder quest = new StringBuilder();
            List<String> colNames = Lists.newArrayList();
            while (rs.next()) {
                if (rs.getString("Extra").equalsIgnoreCase("auto_increment")) {
                    continue;
                }
                String colName = rs.getString("field");
                colGenerators.add(NumericTestBase.getGenerator(colName, true));
                cols.append(colName).append(",");
                colNames.add(colName);
                quest.append("?,");
            }

            cols.setLength(cols.length() - 1);
            quest.setLength(quest.length() - 1);
            Supplier<Object>[] generators = colGenerators.toArray(new Supplier[0]);

            String insertSql = String.format(INSERT_SQL, randomStatisticCollectionTable, cols, quest);
            // batch insert
            long mockRows = 0L;
            PreparedStatement ps = c.prepareStatement(insertSql);
            while (true) {
                if (rows <= 0) {
                    break;
                }
                long currentBatchSize = rows > BATCH_SIZE ? BATCH_SIZE : rows;
                if (currentBatchSize <= 0) {
                    break;
                }
                rows -= currentBatchSize;
                for (int i = 0; i < currentBatchSize; i++) {
                    IntStream.range(1, generators.length + 1).forEach(j -> {
                        try {
                            Object o = generators[j - 1].get();
                            String key = randomStatisticCollectionDb + ":"
                                + randomStatisticCollectionTable + ":"
                                + colNames.get(j - 1);
                            if (!sampleObjects.containsKey(key)) {
                                sampleObjects.put(key, Sets.newHashSet());
                            }
                            if (o != null &&
                                sampleObjects.get(key).size() < FREQUENCY_SIZE &&
                                !sampleObjects.get(key).contains(o)) {
                                sampleObjects.get(key).add(o);
                            }
                            ps.setObject(j, o);
                        } catch (SQLException e) {
                            throw new RuntimeException("mock data error", e);
                        }
                    });
                    ps.addBatch();
                }
                ps.executeBatch();
                mockRows += currentBatchSize;
                log.info("mock data:" + mockRows);
            }
            ps.close();
            log.info("mock data finished");
        }
    }
}
