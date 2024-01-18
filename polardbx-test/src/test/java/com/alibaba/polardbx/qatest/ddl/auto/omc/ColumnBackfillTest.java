/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.omc;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
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

import static org.junit.Assert.assertFalse;

public class ColumnBackfillTest extends DDLBaseNewDBTestCase {
    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(supportsAlterType);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private static final String DISABLE_UPDATE_RETURNING = "OMC_BACK_FILL_USE_RETURNING=FALSE";
    private static final String USE_UPDATE_RETURNING = "OMC_BACK_FILL_USE_RETURNING=TRUE";
    private static final String OMC_FORCE_TYPE_CONVERSION = "OMC_FORCE_TYPE_CONVERSION=TRUE";
    private static final String OMC_ALTER_TABLE_WITH_GSI = "OMC_ALTER_TABLE_WITH_GSI=TRUE";
    private static final String SLOW_HINT =
        "GSI_BACKFILL_BATCH_SIZE=100, GSI_BACKFILL_SPEED_LIMITATION=1000, GSI_BACKFILL_PARALLELISM=4, GSI_DEBUG=\"slow\"";

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    private static final String SINGLE_PK_TMPL = "create table {0}("
        + "id bigint not null auto_increment, "
        + "c1 int default null, "
        + "c1_1 int default null, "
        + "c2 varchar(256) default null, "
        + "id1 varchar(32) not null, "
        + "primary key(id)"
        + ") partition by hash(id) partitions 7";

    private static final String MULTI_PK_TMPL = "create table {0}("
        + "id varchar(32) not null, "
        + "c1 int default null, "
        + "c1_1 int default null, "
        + "c2 varchar(256) default null, "
        + "id1 varchar(32) not null, "
        + "primary key(id, id1)"
        + ") partition by hash(id) partitions 7";

    private static final String MULTI_PK_TMPL_1 = "create table {0}("
        + "id varchar(32) not null, "
        + "c1 int default null, "
        + "c1_1 int default null, "
        + "c2 varchar(256) default null, "
        + "id1 varchar(32) not null, "
        + "primary key(id1, id)"
        + ") partition by hash(id) partitions 7";

    private static final String NO_PK_TMPL = "create table {0}("
        + "id bigint not null, "
        + "c1 int default null, "
        + "c1_1 int default null, "
        + "c2 varchar(256) default null, "
        + "id1 varchar(32) not null "
        + ") partition by hash(id) partitions 7";

    private static final String GSI_TMPL =
        "create global clustered index {0} on {1}(id) partition by hash(id) partitions 8";

    private static final String INSERT_TMPL = "insert into {0}(id, id1, c1, c1_1, c2) values(?, ?, ?, ?, ?)";

    private static final String CHECK_TMPL = "select * from {0} where not c1 <=> c1_1";

    private static final String ALTER_TMPL = "alter table {0} modify column c1 bigint algorithm=omc";

    private static final int FILL_DATA_CNT = 3000;

    private final String returningHint;

    public ColumnBackfillTest(String returningHint) {
        this.returningHint = returningHint;
    }

    @Parameterized.Parameters(name = "{index}:returningHint={0}")
    public static List<Object[]> prepareDate() {
        return ImmutableList.of(new Object[] {DISABLE_UPDATE_RETURNING}, new Object[] {USE_UPDATE_RETURNING});
    }

    @Test
    public void singlePkTest() throws Exception {
        backfillTestInternal("column_backfill_single_pk_tbl" + RandomUtils.getStringBetween(1, 5), SINGLE_PK_TMPL, true,
            true);
        backfillTestInternal("column_backfill_single_pk_tbl" + RandomUtils.getStringBetween(1, 5), SINGLE_PK_TMPL,
            false, true);

        backfillTestInternal("column_backfill_single_pk_tbl" + RandomUtils.getStringBetween(1, 5), SINGLE_PK_TMPL, true,
            false);
        backfillTestInternal("column_backfill_single_pk_tbl" + RandomUtils.getStringBetween(1, 5), SINGLE_PK_TMPL,
            false, false);
    }

    @Test
    public void multiPkTest() throws Exception {
        backfillTestInternal("column_backfill_multi_pk_tbl" + RandomUtils.getStringBetween(1, 5), MULTI_PK_TMPL, true,
            true);
        backfillTestInternal("column_backfill_multi_pk_tbl" + RandomUtils.getStringBetween(1, 5), MULTI_PK_TMPL, false,
            true);

        backfillTestInternal("column_backfill_multi_pk_tbl" + RandomUtils.getStringBetween(1, 5), MULTI_PK_TMPL, true,
            false);
        backfillTestInternal("column_backfill_multi_pk_tbl" + RandomUtils.getStringBetween(1, 5), MULTI_PK_TMPL, false,
            false);

        backfillTestInternal("column_backfill_multi_pk_tbl" + RandomUtils.getStringBetween(1, 5), MULTI_PK_TMPL_1, true,
            true);
        backfillTestInternal("column_backfill_multi_pk_tbl" + RandomUtils.getStringBetween(1, 5), MULTI_PK_TMPL_1,
            false, true);

        backfillTestInternal("column_backfill_multi_pk_tbl" + RandomUtils.getStringBetween(1, 5), MULTI_PK_TMPL_1, true,
            false);
        backfillTestInternal("column_backfill_multi_pk_tbl" + RandomUtils.getStringBetween(1, 5), MULTI_PK_TMPL_1,
            false, false);
    }

    @Test
    public void noPkTest() throws Exception {
        backfillTestInternal("column_backfill_no_pk_tbl" + RandomUtils.getStringBetween(1, 5), NO_PK_TMPL, true, true);
        backfillTestInternal("column_backfill_no_pk_tbl" + RandomUtils.getStringBetween(1, 5), NO_PK_TMPL, false, true);

        backfillTestInternal("column_backfill_no_pk_tbl" + RandomUtils.getStringBetween(1, 5), NO_PK_TMPL, true, false);
        backfillTestInternal("column_backfill_no_pk_tbl" + RandomUtils.getStringBetween(1, 5), NO_PK_TMPL, false,
            false);
    }

    public void backfillTestInternal(String tableName, String tableTmpl, boolean fillNull, boolean withGsi)
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
            param.add(i);
            param.add(fillNull ? null : i);
            param.add(fillNull ? null : i);
            param.add(i);
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
        String alterTableSql =
            buildCmdExtra(returningHint, OMC_ALTER_TABLE_WITH_GSI, SLOW_HINT) + MessageFormat.format(ALTER_TMPL,
                tableName);

        execDdlWithRetry(tddlDatabase1, tableName, alterTableSql, tddlConnection);
        Thread.sleep(500);

        shouldStop.set(true);
        for (Future<Void> result : results) {
            result.get();
        }

        // Check
        String querySql = MessageFormat.format(CHECK_TMPL, tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, querySql);
        assertFalse(rs.next());

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
                    int c = rand.nextInt(upperBound - lowerBound) + lowerBound;
                    param.add(rand.nextInt(upperBound - lowerBound) + lowerBound);
                    param.add(rand.nextInt(upperBound - lowerBound) + lowerBound);
                    param.add(c);
                    param.add(c);
                    param.add(rand.nextInt(upperBound - lowerBound) + lowerBound);
                    params.add(param);
                }
                int[] nn = JdbcUtil.updateDataBatchIgnoreErr(connection, sqlInsert, params);
            }
        } finally {
            connection.close();
        }
    }

    @Test
    public void onUpdateTimestampTest() throws Exception {
        String tableName = "column_backfill_ts_tbl";
        dropTableIfExists(tableName);
        String createSql = "create table " + tableName + "("
            + "id bigint, "
            + "c1 timestamp(6) on update current_timestamp(6) default current_timestamp(6),"
            + "c1_1 timestamp(6) on update current_timestamp(6) default current_timestamp(6),"
            + "c1_2 timestamp(6) default current_timestamp(6),"
            + "primary key(id)"
            + ") partition by hash(id) partitions 7";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        final String sqlInsert = String.format("insert into %s (id) values (?)", tableName);
        // Fill some data
        List<List<Object>> params = new ArrayList<>();
        for (int i = 0; i < FILL_DATA_CNT; i++) {
            List<Object> param = new ArrayList<>();
            param.add(i);
            params.add(param);
        }
        int[] nn = JdbcUtil.updateDataBatch(tddlConnection, sqlInsert, params);

        // Alter
        String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + " alter table " + tableName
            + " modify column c1_1 timestamp(6) on update current_timestamp(6) default current_timestamp(6) algorithm=omc";
        execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);

        // Check
        String querySql =
            MessageFormat.format("select * from {0} where not c1 <=> c1_1 or not c1_2 <=> c1_1", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, querySql);
        assertFalse(rs.next());
    }
}
