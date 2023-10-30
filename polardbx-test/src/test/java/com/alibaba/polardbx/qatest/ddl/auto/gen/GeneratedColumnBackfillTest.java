package com.alibaba.polardbx.qatest.ddl.auto.gen;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class GeneratedColumnBackfillTest extends DDLBaseNewDBTestCase {

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private final Boolean forceCnEval;

    @Parameterized.Parameters(name = "{index}:forceCnEval={0}")
    public static List<Object[]> prepareDate() {
        return ImmutableList.of(new Object[] {Boolean.TRUE}, new Object[] {Boolean.FALSE});
    }

    public GeneratedColumnBackfillTest(Boolean forceCnEval) {
        this.forceCnEval = forceCnEval;
    }

    private static final String SLOW_HINT =
        "GSI_BACKFILL_BATCH_SIZE=100, GSI_BACKFILL_SPEED_LIMITATION=1000, GSI_BACKFILL_PARALLELISM=4, GSI_DEBUG=\"slow\"";
    private static final String FORCE_CN_EVAL_HINT = "GEN_COL_FORCE_CN_EVAL=TRUE";

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    private static final String SINGLE_PK_TMPL = "create table {0}("
        + "id bigint not null auto_increment, "
        + "c1 int default null, "
        + "c2 int default null, "
        + "primary key(id)"
        + ") partition by hash(id) partitions 7";

    private static final String GSI_TMPL =
        "create global clustered index {0} on {1}(id) partition by hash(id) partitions 8";

    private static final String INSERT_TMPL = "insert into {0}(id, c1, c2) values(?, ?, ?)";

    private static final String ALTER_MULTI_TMPL =
        "alter table {0} add column c3 int as(c1*2) logical after id, add column c4 int as(c2*2) logical, add column c5 int as(c1*2) logical first";

    private static final String CHECK_MULTI_TMPL =
        "select * from {0} where (not c3 <=> c1*2) or (not c4 <=> c2*2) or (not c5 <=> c1*2)";

    private static final String ALTER_SINGLE_TMPL = "alter table {0} add column c3 int as(c1*2) logical after id";

    private static final String CHECK_SINGLE_TMPL = "select * from {0} where (not c3 <=> c1*2)";

    private static final int FILL_DATA_CNT = 3000;

    @Test
    public void multipleAlterTest() throws Exception {
        backfillTestInternal("column_backfill_single_pk_tbl" + RandomUtils.getStringBetween(1, 5), SINGLE_PK_TMPL,
            ALTER_MULTI_TMPL, CHECK_MULTI_TMPL, true, false);
        backfillTestInternal("column_backfill_single_pk_tbl" + RandomUtils.getStringBetween(1, 5), SINGLE_PK_TMPL,
            ALTER_MULTI_TMPL, CHECK_MULTI_TMPL, false, false);
    }

    @Test
    public void singleAlterTest() throws Exception {
        backfillTestInternal("column_backfill_single_pk_tbl" + RandomUtils.getStringBetween(1, 5), SINGLE_PK_TMPL,
            ALTER_SINGLE_TMPL, CHECK_SINGLE_TMPL, true, true);
        backfillTestInternal("column_backfill_single_pk_tbl" + RandomUtils.getStringBetween(1, 5), SINGLE_PK_TMPL,
            ALTER_SINGLE_TMPL, CHECK_SINGLE_TMPL, false, true);
        backfillTestInternal("column_backfill_single_pk_tbl" + RandomUtils.getStringBetween(1, 5), SINGLE_PK_TMPL,
            ALTER_SINGLE_TMPL, CHECK_SINGLE_TMPL, true, false);
        backfillTestInternal("column_backfill_single_pk_tbl" + RandomUtils.getStringBetween(1, 5), SINGLE_PK_TMPL,
            ALTER_SINGLE_TMPL, CHECK_SINGLE_TMPL, false, false);
    }

    public void backfillTestInternal(String tableName, String tableTmpl, String alterTmpl, String checkTmpl,
                                     boolean fillNull, boolean withGsi)
        throws Exception {
        String gsiName = tableName + "_idx";
        dropTableIfExists(tableName);
        final String tddlCreateTable = MessageFormat.format(tableTmpl, tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlCreateTable);

        if (withGsi) {
            String createGsiSql = MessageFormat.format(GSI_TMPL, gsiName, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql);
        }

        final String sqlInsert = MessageFormat.format(INSERT_TMPL, tableName);
        // Fill some data
        List<List<Object>> params = new ArrayList<>();
        for (int i = 0; i < FILL_DATA_CNT; i += 2) {
            List<Object> param = new ArrayList<>();
            param.add(i);
            param.add(fillNull ? null : i);
            param.add(fillNull ? null : i);
            params.add(param);
        }
        int[] nn = JdbcUtil.updateDataBatch(tddlConnection, sqlInsert, params);

        final ExecutorService threadPool = Executors.newFixedThreadPool(3);
        AtomicBoolean shouldStop = new AtomicBoolean(false);
        Callable<Void> insertTask1 = () -> {
            doBatchInsert(sqlInsert, -FILL_DATA_CNT, 0, 2, shouldStop, 1);
            return null;
        };
        Callable<Void> insertTask2 = () -> {
            doBatchInsert(sqlInsert, 0, FILL_DATA_CNT, 2, shouldStop, 2);
            return null;
        };
        Callable<Void> insertTask3 = () -> {
            doBatchInsert(sqlInsert, FILL_DATA_CNT, FILL_DATA_CNT * 2, 2, shouldStop, 3);
            return null;
        };

        ArrayList<Future<Void>> results = new ArrayList<>();
        results.add(threadPool.submit(insertTask1));
        results.add(threadPool.submit(insertTask2));
        results.add(threadPool.submit(insertTask3));
        Thread.sleep(500);

        // Alter table
        String hint;
        if (forceCnEval) {
            hint = buildCmdExtra(SLOW_HINT, FORCE_CN_EVAL_HINT);
        } else {
            hint = buildCmdExtra(SLOW_HINT);
        }

        String alterTableSql = hint + MessageFormat.format(alterTmpl, tableName);

        execDdlWithRetry(tddlDatabase1, tableName, alterTableSql, tddlConnection);
        Thread.sleep(500);

        shouldStop.set(true);
        for (Future<Void> result : results) {
            result.get();
        }

        // Check
        String querySql = MessageFormat.format(checkTmpl, tableName);
        System.out.println(querySql);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, querySql);
        Assert.assertFalse(rs.next());

        if (withGsi) {
            checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName));
        }
    }

    void doBatchInsert(String sqlInsert, int lowerBound, int upperBound, int batchSize, AtomicBoolean shouldStop,
                       int seed)
        throws Exception {
        Connection connection = getPolardbxConnection();
        try {
            Random rand = new Random(seed);
            while (!shouldStop.get()) {
                List<List<Object>> params = new ArrayList<>();
                for (int i = 0; i < batchSize; i++) {
                    List<Object> param = new ArrayList<>();
                    param.add(rand.nextInt(upperBound - lowerBound) + lowerBound);
                    param.add(rand.nextInt(upperBound - lowerBound) + lowerBound);
                    param.add(rand.nextInt(upperBound - lowerBound) + lowerBound);
                    params.add(param);
                }
                int[] nn = JdbcUtil.updateDataBatchIgnoreErr(connection, sqlInsert, params);
            }
        } finally {
            connection.close();
        }
    }
}
