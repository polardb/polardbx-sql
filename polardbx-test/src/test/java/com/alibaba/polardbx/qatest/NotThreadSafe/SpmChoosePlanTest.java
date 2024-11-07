package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public class SpmChoosePlanTest extends BaseTestCase {
    private static final String DB = "spmChoose";

    private static final String SPM_TABLE = "full_table_big";

    private static final String CREATE_TABLE_TEMPLATE = "CREATE TABLE {0} (\n"
        + "    `c_int_32` int(32) NOT NULL DEFAULT '0',\n"
        + "    `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "    `c_bigint_64_un` bigint(64) UNSIGNED DEFAULT NULL,\n"
        + "    `c_bigint_1` bigint(1) DEFAULT NULL,\n"
        + "    `c_int_1` int(1) DEFAULT NULL,\n"
        + "    PRIMARY KEY (`id`),\n"
        + "    GLOBAL INDEX `c_bigint_1_idx` (`c_bigint_1`, `id`) PARTITION BY KEY(`c_bigint_1`, `id`) PARTITIONS 16,\n"
        + "    GLOBAL INDEX `c_bigint_64_un_c_int_1_idx` (`c_bigint_64_un`, `c_int_1`, `id`) PARTITION BY KEY(`c_bigint_64_un`, `c_int_1`, `id`) PARTITIONS 16,\n"
        + "    LOCAL KEY `_local_c_bigint_1_idx` (`c_bigint_1`),\n"
        + "    LOCAL KEY `_local_c_bigint_64_un_c_int_1_idx` (`c_bigint_64_un`, `c_int_1`)\n"
        + ") ENGINE = InnoDB AUTO_INCREMENT = 9999976 DEFAULT CHARSET = utf8mb4 PARTITION BY KEY(`id`) PARTITIONS 16";

    private static final String SQL = "select * from %s where c_bigint_1 in %s and c_bigint_64_un in %s";

    private static final String INDEX1 = "c_bigint_1_idx";

    private static final String INDEX2 = "c_bigint_64_un_c_int_1_idx";

    private static final String INDEX_HINT1 = String.format("/*+TDDL:index(%s, %s)*/ ", SPM_TABLE, INDEX1);

    private static final String INDEX_HINT2 = String.format("/*+TDDL:index(%s, %s)*/", SPM_TABLE, INDEX2);

    private static final String CON_SHORT = "(1)";

    private static final String CON_LONG = "(1, 2)";

    private static final String CLEAR_CACHE = "clear plancache";

    private static final String SPM_CLEAR = "baseline delete_all";

    private static final String SPM_ADD = "baseline add sql ";

    private static final String SPM_LIST = "baseline list ";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @BeforeClass
    public static void initDb() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.createPartDatabase(tmpConnection, DB);
            JdbcUtil.useDb(tmpConnection, DB);
            JdbcUtil.dropTable(tmpConnection, SPM_TABLE);
            JdbcUtil.executeUpdateSuccess(tmpConnection,
                MessageFormat.format(CREATE_TABLE_TEMPLATE, SPM_TABLE));
        }
    }

    @AfterClass
    public static void deleteDb() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.dropDatabase(tmpConnection, DB);
        }
    }

    @Before
    public void before() {
        try (Connection conn = getPolardbxConnection(DB)) {
            JdbcUtil.executeSuccess(conn, CLEAR_CACHE);
            JdbcUtil.executeSuccess(conn, SPM_CLEAR);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testEvolution() {
        String sql;
        String baselineId;
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        try (Connection conn = getPolardbxConnection(DB)) {
            JdbcUtil.executeSuccess(conn, SPM_ADD + INDEX_HINT1 + String.format(SQL, SPM_TABLE, CON_SHORT, CON_SHORT));
            JdbcUtil.executeSuccess(conn, SPM_ADD + INDEX_HINT2 + String.format(SQL, SPM_TABLE, CON_SHORT, CON_SHORT));

            sql = String.format(SQL, SPM_TABLE, CON_SHORT, CON_SHORT);
            JdbcUtil.executeSuccess(conn, "explain " + sql);

            baselineId = getBaselineId(conn, sql);
            Truth.assertThat(baselineId).isNotEmpty();
            Truth.assertThat(getBaselineCount(conn, baselineId)).isEqualTo(2);

            String[] sqls = {
                String.format(SQL, SPM_TABLE, CON_SHORT, CON_LONG), String.format(SQL, SPM_TABLE, CON_LONG, CON_SHORT)};
            String[] indexes = {INDEX1, INDEX2};
            for (String testSql : sqls) {
                Truth.assertWithMessage("base explain " + sql).that(getBaselineId(conn, testSql)).contains(baselineId);
            }

            Callable<String> task =
                () -> {
                    try (Connection testConn = getPolardbxConnection(DB)) {
                        Random r1 = new Random();
                        for (int i = 0; i < 15; i++) {
                            int loc = r1.nextInt(2);
                            checkGsi(testConn, sqls[loc], indexes[loc]);
                        }
                        return "success";
                    } catch (SQLException e) {
                        e.printStackTrace();
                        return (e.toString());
                    }
                };
            List<Future<String>> futures = new ArrayList<>();
            IntStream.range(0, 10).forEach(
                i -> futures.add(executorService.submit(task)));

            for (Future<String> future : futures) {
                future.get();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            executorService.shutdown();
        }
    }

    private void checkGsi(Connection conn, String sql, String index) {
        final List<List<String>> res =
            JdbcUtil.getAllStringResult(JdbcUtil.executeQuery("explain " + sql, conn), false, ImmutableList.of());
        index = index.toUpperCase();
        String source = "IndexScan".toUpperCase();
        for (List<String> row : res) {
            if (row.isEmpty()) {
                continue;
            }
            String value = row.get(0).toUpperCase();
            if (value.contains(source)) {
                Truth.assertWithMessage("base explain " + sql).that(value).contains(index);
                return;
            }
        }
        Truth.assertWithMessage("base explain " + sql).that(true).isFalse();
    }

    private String getBaselineId(Connection conn, String sql) {
        final List<List<String>> res =
            JdbcUtil.getAllStringResult(JdbcUtil.executeQuery("explain " + sql, conn), false, ImmutableList.of());
        String source = "BaselineInfo Id:".toUpperCase();

        for (List<String> row : res) {
            if (row.isEmpty()) {
                continue;
            }
            String value = row.get(0).toUpperCase();
            int sourceLoc = value.indexOf(source);
            if (sourceLoc < 0) {
                continue;
            }
            return value.substring(sourceLoc + source.length()).trim();
        }
        return "";
    }

    private int getBaselineCount(Connection conn, String baselineId) {
        return JdbcUtil.getAllResult(JdbcUtil.executeQuery(SPM_LIST + baselineId, conn), false)
            .size();
    }
}
