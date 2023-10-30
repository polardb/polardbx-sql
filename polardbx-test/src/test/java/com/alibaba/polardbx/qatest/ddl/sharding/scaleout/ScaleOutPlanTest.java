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

package com.alibaba.polardbx.qatest.ddl.sharding.scaleout;

import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import net.jcip.annotations.NotThreadSafe;
import org.apache.calcite.util.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;

/**
 * @author luoyanxin
 */

@NotThreadSafe
public class ScaleOutPlanTest extends ScaleOutBaseTest {

    private static List<ComplexTaskMetaManager.ComplexTaskStatus> moveTableStatus =
        Stream.of(
            ComplexTaskMetaManager.ComplexTaskStatus.CREATING,
            ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
            ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY,
            ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG,
            ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC,
            ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC).collect(Collectors.toList());
    final ComplexTaskMetaManager.ComplexTaskStatus finalTableStatus;
    static boolean firstIn = true;
    static ComplexTaskMetaManager.ComplexTaskStatus currentStatus = ComplexTaskMetaManager.ComplexTaskStatus.CREATING;
    final Boolean isCache;
    final static int tbPartitions = 3;

    public ScaleOutPlanTest(StatusInfo tableStatus) {
        super("ScaleOutPlanTest", "polardbx_meta_db_polardbx",
            ImmutableList.of(tableStatus.getMoveTableStatus().toString()));
        finalTableStatus = tableStatus.getMoveTableStatus();
        isCache = tableStatus.isCache();
        if (!currentStatus.equals(finalTableStatus)) {
            firstIn = true;
            currentStatus = finalTableStatus;
        }
    }

    @Before
    public void setUpTables() {
        if (firstIn) {
            setUp(true, getTableDefinitions(), true);
            firstIn = false;
        }
        String tddlSql = "use " + logicalDatabase;
        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlSql);
    }

    @After
    public void tearDown() {
    }

    @Parameterized.Parameters(name = "{index}:TableStatus={0}")
    public static List<StatusInfo[]> prepareData() {
        List<StatusInfo[]> status = new ArrayList<>();
        moveTableStatus.stream().forEach(c -> {
            status.add(new StatusInfo[] {new StatusInfo(c, false)});
            status.add(new StatusInfo[] {new StatusInfo(c, true)});
            status.add(new StatusInfo[] {new StatusInfo(c, null)});
        });
        return status;
    }

    @Test
    public void insertIgnorePushDownOnNonScaleOutGroup() {

        if (!sourceGroupKey.contains("000001")) {
            return;
        }

        if (!finalTableStatus.isWriteOnly()) {
            return;
        }

        final String tableName = "mdb_mtb_mk1";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        String sql2 = "";
        executeDml(sql);

        // shard on scaleout-group
        sql =
            "  insert ignore into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123456+4, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";
        // shard on non-scale out-group, should be pushdown
        sql2 =
            "  insert ignore into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123456, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";

        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }

        executeDml("trace " + hintStr + sql2);
        final List<Pair<String, String>> topology2 = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace2 = getTrace(tddlConnection);

        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsertSqlCnt = 1;
        int scanForDuplicateCheck = 1;
        Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsertSqlCnt + 1));
        Assert.assertThat(trace2.toString(), trace2.size(), is(basePhyInsertSqlCnt));

    }

    @Test
    public void replacePushDownOnNonScaleOutGroup() {

        if (!sourceGroupKey.contains("000001")) {
            return;
        }

        if (!finalTableStatus.isWriteOnly()) {
            return;
        }

        final String tableName = "mdb_mtb_mk1";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        String sql2 = "";
        executeDml(sql);

        // shard on scaleout-group
        sql =
            "replace into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123456+4, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";
        // shard on non-scaleout-group, should be pushdown
        sql2 =
            "replace into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123456, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";

        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }

        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        executeDml("trace " + hintStr + sql2);
        final List<Pair<String, String>> topology2 = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace2 = getTrace(tddlConnection);

        int basePhyInsertSqlCnt = 1;
        int scanForDuplicateCheck = 1;
        Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsertSqlCnt + 1));
        Assert.assertThat(trace2.toString(), trace2.size(), is(basePhyInsertSqlCnt));

    }

    @Test
    public void updatePushDownOnNonScaleOutGroup() {

        if (!sourceGroupKey.contains("000001")) {
            return;
        }

        if (!finalTableStatus.isWriteOnly()) {
            return;
        }

        final String tableName = "mdb_mtb_mk1";
        int tbPartitionCnt = 4;
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        String sql2 = "";
        executeDml(sql);
        String insert1 =
            "  insert ignore into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123456+4, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";
        executeDml(insert1);
        String insert2 =
            "  insert ignore into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123456, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";
        executeDml(insert2);

        // shard on scaleout-group
        sql = "update mdb_mtb_mk1 set varchar_test='2' where pk=123456+4";
        // shard on non-scaleout-group, should be pushdown
        sql2 = "update mdb_mtb_mk1 set varchar_test='3' where pk=123456";

        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }

        executeDml("trace " + hintStr + sql2);
        final List<Pair<String, String>> topology2 = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace2 = getTrace(tddlConnection);

        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyUpdateSqlCnt = 1;
        Assert.assertThat(trace.toString(), trace.size(), is(topology.size() / 4 + basePhyUpdateSqlCnt + 1));
        Assert.assertThat(trace2.toString(), trace2.size(), is(basePhyUpdateSqlCnt));
    }

    @Test
    public void deletePushDownOnNonScaleOutGroup() {
        if (!sourceGroupKey.contains("000001")) {
            return;
        }

        if (!finalTableStatus.isWriteOnly()) {
            return;
        }

        final String tableName = "mdb_mtb_mk1";
        int tbPartitionCnt = 4;
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        String sql2 = "";
        executeDml(sql);
        String insert1 =
            "  insert ignore into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123456+4, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";
        executeDml(insert1);
        String insert2 =
            "  insert ignore into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123456, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";
        executeDml(insert2);

        // shard on scaleout-group
        sql = "delete from  mdb_mtb_mk1 where pk=123456+4";
        // shard on non-scaleout-group, should be pushdown
        sql2 = "delete from mdb_mtb_mk1 where pk=123456";

        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }

        executeDml("trace " + hintStr + sql2);
        final List<Pair<String, String>> topology2 = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace2 = getTrace(tddlConnection);

        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyUpdateSqlCnt = 1;
        Assert.assertThat(trace.toString(), trace.size(), is(topology.size() / 4 + basePhyUpdateSqlCnt + 1));
        Assert.assertThat(trace2.toString(), trace2.size(), is(basePhyUpdateSqlCnt));
    }

    @Test
    public void tablePkNoUkForInsert() {
        final String tableName = "test_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "insert ignore into " + tableName
            + "(a,b) values(1,1), (2,2), ((1+2), (2+1)), (4,4), (5,5),(6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + topology.size() + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + topology.size()));
        } else {
            //push insert ignore
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert));
        }
        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                + "(a,b) values(1,1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i * 2;
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = i;
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(1));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
            executeDml(sql);
        }
    }

    @Test
    public void tablePkNoUkForInsertSelect() {
        final String tableName = "test_tb_with_pk_no_uk";
        final String sourceTableName = "source_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + sourceTableName + " where 1=1";
        executeDml(sql);
        sql = "insert ignore into " + sourceTableName
            + "(a,b) values(1,1), (2,2), (( 3),( 3)), (4,4), (5,5),(6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false, MERGE_UNION=false)*/ ";
        if (isCache == null) {
            hintStr = " /*+TDDL:cmd_extra(MERGE_UNION=false)*/ ";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true,MERGE_UNION=false)*/ ";
        }
        executeDml(hintStr + sql);
        sql = "insert ignore into " + tableName + "(a,b) select a,b+100 from " + sourceTableName;
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + 2 * topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + 2 * topology.size() + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + 2 * topology.size()));
        } else {
            //select + push upsert
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + topology.size()));
        }
        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                + "(a,b) select a,( 1+2-2) from " + sourceTableName + " where a = 1+1-1";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i * 2;
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = i;
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
            } else {
                //select + push upsert
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + 1));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
            executeDml(sql);
        }
    }

    @Test
    public void tableWithPkAndUkForInsert() {
        final String tableName = "test_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "insert ignore into " + tableName
            + "(a,b,c) values(1,1,1), (1+1,2,2), (3,1+1*2,3), (4,4,4), (5,5,5),(6,6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int scanCount = 6;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + scanCount + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + scanCount));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert));
        }
        int scanForDuplicateCheck = 1;
        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                + "(a,b,c) values(1,1,1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i * 2;
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(2));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = i;
                Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(1));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
            executeDml(sql);
        }
    }

    @Test
    public void tableWithPkAndUkForInsertSelect() {
        final String tableName = "test_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);
        final String sourceTableName = "source_tb_with_pk_uk";
        sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + sourceTableName + " where 1=1";
        executeDml(sql);

        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false, MERGE_UNION=false)*/ ";
        if (isCache == null) {
            hintStr = " /*+TDDL:cmd_extra(MERGE_UNION=false)*/ ";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true,MERGE_UNION=false)*/ ";
        }
        sql = "insert ignore into " + sourceTableName
            + "(a,b,c) values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(6,6,6)";
        executeDml(sql);
        sql = "insert ignore into " + tableName
            + "(a,b,c) select a+1-1,b,c from " + sourceTableName;
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int scan = 6;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + scan + topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(scan + topology.size() + basePhyInsert));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
        }
        int scanForDuplicateCheck = 1;
        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                + "(a,b,c) select a,b+1-1,c from " + sourceTableName + " where a=(select 1) and b<>2+100";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i * 2;
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(),
                    is(scanForDuplicateCheck + topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + 2));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = i;
                Assert.assertThat(trace.toString(), trace.size(),
                    is(scanForDuplicateCheck + topology.size() + basePhyInsert));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + 1));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
            executeDml(sql);
        }
    }

    @Test
    public void brdTablePkNoUkForInsert() {
        final String tableName = "test_brc_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "insert ignore into " + tableName
            + "(a,b) values(1,1), (1+1,2), (3,3), (2+2,4), (5,5),(6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 1; //minus metaDb and scaleout target db
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size()));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount));
        }
        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                + "(a,b) values(1+1-1,1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i > 0 ? i * physicalDbCount + 1 : 0;
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
            } else if (finalTableStatus.isDeleteOnly()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + (i == 0 ? 0 : physicalDbCount)));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
            executeDml(sql);
        }
    }

    @Test
    public void brdTablePkNoUkForInsertSelect() {
        final String tableName = "test_brc_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);

        final String sourceTableName = "source_brc_tb_with_pk_no_uk";
        sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + sourceTableName + " where 1=1";
        executeDml(sql);

        sql = "insert ignore into " + sourceTableName
            + "(a,b) values(1,1), (2,2), (3,3), (4,4), (5,5),(6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml(hintStr + sql);
        sql = "insert ignore into " + tableName
            + "(a,b) select a+1-1,b+20 from " + sourceTableName + " where a<> ( 1000)";
        executeDml("trace " + hintStr + sql);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 1; //minus metaDb and scaleout target db
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 1 + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1 + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 2 * topology.size()));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 1));
        } else {
            //select + push insert ignore
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size()));
        }
        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                + "(a,b) select a+1-1,b+1 from " + sourceTableName + " where a=( 1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i > 0 ? i * physicalDbCount + 1 : 0;
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert + 1));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1 + 1));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = i > 0 ? i * physicalDbCount : 0;
                Assert
                    .assertThat(trace.toString(), trace.size(), is(topology.size() + topology.size() + basePhyInsert));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 1));
            } else {
                //select + push ig
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
            executeDml(sql);
        }
    }

    @Test
    public void brdTableWithPkAndUkForInsert() {
        final String tableName = "test_brc_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "insert ignore into " + tableName
            + "(a,b,c) values(1+1-1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(6,6,3+2+1-1+1)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml("trace " + hintStr + sql);
        int physicalDbCount = getDataSourceCount() - 1 - 1; //minus metaDb and scaleout target db
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size()));
        } else if ((finalTableStatus.isPublic())) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount));
        }
        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                + "(a,b,c) values(1,1,1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i > 0 ? i * physicalDbCount + 1 : 0;
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
            } else if (finalTableStatus.isDeleteOnly()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + (i == 0 ? 0 : physicalDbCount)));
            } else if ((finalTableStatus.isPublic())) {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
            executeDml(sql);
        }
    }

    @Test
    public void brdTableWithPkAndUkForInsertSelect() {
        final String tableName = "test_brc_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);
        final String sourceTableName = "source_brc_tb_with_pk_uk";
        sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + sourceTableName + " where 1=1";
        executeDml(sql);

        sql = "insert ignore into " + sourceTableName
            + "(a,b,c) values( 1+1-1, 1, 1), (2,2,2), (3, 3,3), (4,4,4), (5,5,5),(3+6-3,6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false, MERGE_UNION=false)*/ ";
        if (isCache == null) {
            hintStr = " /*+TDDL:cmd_extra(MERGE_UNION=false)*/ ";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true,MERGE_UNION=false)*/ ";
        }
        executeDml(hintStr + sql);

        sql = "insert ignore into " + tableName
            + "(a,b,c) select a+1-1,b+1-1,c from " + sourceTableName + " where a<>2000 and b<1+2+2000";
        executeDml("trace " + hintStr + sql);
        int physicalDbCount = getDataSourceCount() - 1 - 1; //minus metaDb and scaleout target db
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 1 + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1 + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 1));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 2));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        }
        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                + "(a,b,c) select a+1-1,b,c+1-1 from " + sourceTableName + " where a=1 and b<>1+9999+32";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i > 0 ? i * physicalDbCount + 1 : 0;
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert + 1));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1 + 1));
            } else if (finalTableStatus.isDeleteOnly()) {
                Assert.assertThat(trace.toString(), trace.size(),
                    is(topology.size() * 2 + (i == 0 ? 0 : physicalDbCount)));
            } else if ((finalTableStatus.isPublic())) {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 2));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
            executeDml(sql);
        }
    }

    @Test
    public void tablePkNoUkForReplace() {
        final String tableName = "test_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "replace into " + tableName
            + "(a,b) values(1+1-1,1), (2,2), (3,3), (4,4), (5,5),(6,6+8-8)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + topology.size() + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + topology.size()));
        } else {
            //push replace
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert));
        }
        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                + "(a,b) values(1+1-1,1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 4 : 2); //4:(delete + insert) * 2
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                basePhyInsert = 2; //(replace) * 2 or (insert) * 2
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = (i == 0 ? 3 : 1); //3:(delete + insert) + delete
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else {
                //push replace
                Assert.assertThat(trace.toString(), trace.size(), is(1));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "replace into " + tableName
            + "(a,b) values(1,2)";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = 4; //4:(delete + insert) * 2
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
        } else if (finalTableStatus.isDeleteOnly()) {
            basePhyInsert = 3; //3:(delete + insert) + delete
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
        } else {
            //push replace
            Assert.assertThat(trace.toString(), trace.size(), is(1));
        }
    }

    @Test
    public void tablePkNoUkForReplaceSelect() {
        final String tableName = "test_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);

        final String sourceTableName = "source_tb_with_pk_no_uk";
        sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + sourceTableName + " where 1=1";
        executeDml(sql);

        sql = "replace into " + sourceTableName
            + "(a,b) values(1+1-1,1), (2,2+1-1), (3,3), (4,4), (5,5),(6,6+1-1)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false, MERGE_UNION=false)*/ ";
        if (isCache == null) {
            hintStr = " /*+TDDL:cmd_extra(MERGE_UNION=false)*/ ";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true,MERGE_UNION=false)*/ ";
        }
        executeDml(hintStr + sql);

        sql = "replace into " + tableName
            + "(a,b) select a,b+20 from " + sourceTableName + " where b<>( 200+321)";
        executeDml("trace " + hintStr + sql);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + 2 * topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + 2 * topology.size() + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            //select + insert
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + 2 * topology.size()));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + topology.size()));
        }
        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                + "(a,b) select a+1-1,b from " + sourceTableName + " where a=1+1+2-3 and b<>( 1234+3456)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 4 : 2); //4:(delete + insert) * 2
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                //basePhyInsert = 2; //(replace) * 2 or (insert) * 2
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = (i == 0 ? 3 : 1); //3:(delete + insert) + delete
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
            } else {
                //push replace
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + 1));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "replace into " + tableName
            + "(a,b) select a,b+1 from " + sourceTableName + " where a=1";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = 4; //4:(delete + insert) * 2
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + topology.size() + basePhyInsert));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + topology.size() + basePhyInsert));
        } else if (finalTableStatus.isDeleteOnly()) {
            basePhyInsert = 3; //3:(delete + insert) + delete
            Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + topology.size() + basePhyInsert));
        } else {
            //select + replace
            Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + 1));
        }
    }

    @Test
    public void tablePkAndUkForReplace() {
        final String tableName = "test_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "replace into " + tableName
            + "(a,b,c) values(1+1-1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5+1-1),(6,6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int scan = 6;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + scan + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + scan));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert));
        }
        int scanForDuplicateCheck = 1;
        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                + "(a,b,c) values(1+1-1,1,1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 4 : 2); //4:(delete + insert) * 2
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                basePhyInsert = 2; //(replace) * 2
                Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = (i == 0 ? 3 : 1); //3:(delete + insert) + delete
                Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(1));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "replace into " + tableName
            + "(a,b,c) values(1,1,3)";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = 4; //4:(delete + insert) * 2
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(2));
        } else if (finalTableStatus.isDeleteOnly()) {
            basePhyInsert = 3; //3:(delete + insert) + delete
            Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(1));
        }
    }

    @Test
    public void tablePkAndUkForReplaceSelect() {
        final String tableName = "test_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);

        final String sourceTableName = "source_tb_with_pk_uk";
        sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + sourceTableName + " where 1=1";
        executeDml(sql);

        sql = "replace into " + sourceTableName
            + "(a,b,c) values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(6,6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false, MERGE_UNION=false)*/ ";
        if (isCache == null) {
            hintStr = " /*+TDDL:cmd_extra(MERGE_UNION=false)*/ ";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true,MERGE_UNION=false)*/ ";
        }
        executeDml(hintStr + sql);

        sql = "replace into " + tableName
            + "(a,b,c) select 3+a-3,b+1-1,c+2-2 from " + sourceTableName
            + " where a<1000 and b<>( 1+3333+21-1*2)";
        executeDml("trace " + hintStr + sql);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(2 * basePhyInsert + topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + topology.size() + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(2 * basePhyInsert + topology.size()));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + topology.size()));
        }
        int scanForDuplicateCheck = 1;
        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                + "(a,b,c) select a+1*2-2,1+b-1,c+2-2 from " + sourceTableName + " where a=( 1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 4 : 2); //4:(delete + insert) * 2
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(),
                    is(tbPartitions + scanForDuplicateCheck + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                basePhyInsert = 2; //(replace) * 2
                Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + basePhyInsert));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = (i == 0 ? 3 : 1); //3:(delete + insert) + delete
                Assert.assertThat(trace.toString(), trace.size(),
                    is(tbPartitions + scanForDuplicateCheck + basePhyInsert));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + 1));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "replace into " + tableName
            + "(a,b,c) select a+1-1,b,c+2 from " + sourceTableName + " where a=(select 1) and c<>( 34567)";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = 4; //4:(delete + insert) * 2
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(),
                is(topology.size() + scanForDuplicateCheck + basePhyInsert));
        } else if (finalTableStatus.isReadyToPublic()) {
            //select + push replace
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + 1 + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            basePhyInsert = 3; //3:(delete + insert) + delete
            Assert.assertThat(trace.toString(), trace.size(),
                is(topology.size() + scanForDuplicateCheck + basePhyInsert));
        } else {
            //select + push replace
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + 1));
        }
    }

    @Test
    public void brdTablePkNoUkForReplace() {
        final String tableName = "test_brc_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "replace into " + tableName
            + "(a,b) values(1,1), (2,2+1-1), (3,3), (4,4), (5,5),(6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 1; //minus metaDb and scaleout target db
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount));
        }
        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                + "(a,b) values(1,1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 2 * (physicalDbCount + 1) : physicalDbCount + 1); //(delete + insert) * 2
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = (i == 0 ? 2 * physicalDbCount + 1 : physicalDbCount); //(delete + insert) + delete
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "replace into " + tableName
            + "(a,b) values(1+1-1,2)";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = physicalDbCount * 2; //(delete + insert) * 2
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert + 2));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            basePhyInsert = 2 * physicalDbCount + 1; //(delete + insert) + delete
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
        } else if ((finalTableStatus.isPublic())) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount));
        }
    }

    @Test
    public void brdTablePkNoUkForReplaceSelect() {
        final String tableName = "test_brc_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);

        final String sourceTableName = "source_brc_tb_with_pk_no_uk";
        sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + sourceTableName + " where 1=1";
        executeDml(sql);

        sql = "replace into " + sourceTableName
            + "(a,b) values(1,1), (2,2), (3,3), (4,4), (5,5),(6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml(hintStr + sql);

        sql = "replace into " + tableName
            + "(a,b) select a+1-1,b+1-1 from " + sourceTableName + " where 1=1 and a<>1+23456";
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 1; //minus metaDb and scaleout target db
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 2 * topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount));
        }
        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                + "(a,b) select a,b from " + sourceTableName + " where 2=2 and a=1+1-1";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 2 * (physicalDbCount + 1) : physicalDbCount + 1); //(delete + insert) * 2
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = (i == 0 ? 2 * physicalDbCount + 1 : physicalDbCount); //(delete + insert) + delete
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "replace into " + tableName
            + "(a,b) select a,b+1 from " + sourceTableName + " where a=1 and 2=2";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = physicalDbCount * 2; //(delete + insert) * 2
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert + 2));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            basePhyInsert = 2 * physicalDbCount + 1; //(delete + insert) + delete
            Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount));
        }
    }

    @Test
    public void brdTablePkAndUkForReplace() {
        final String tableName = "test_brc_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "replace into " + tableName
            + "(a,b,c) values(1,1,1+1-1), (2,2,2), (3,3+1-1,3), (4,4,4), (5,5,5),(6,6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 1; //minus metaDb and scaleout target db
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount));
        }
        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                + "(a,b,c) values(1+1-1,1,1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 2 * (physicalDbCount + 1) : physicalDbCount + 1); //(delete + insert) * 2
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = (i == 0 ? 2 * physicalDbCount + 1 : physicalDbCount); //(delete + insert) + delete
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "replace into " + tableName
            + "(a,b,c) values(1,1+1-1,2)";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = physicalDbCount * 2; //(delete + insert) * 2
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert + 2));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            basePhyInsert = 2 * physicalDbCount + 1; //(delete + insert) + delete
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount));
        }
    }

    @Test
    public void brdTablePkAndUkForReplaceSelect() {
        final String tableName = "test_brc_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ delete from " + tableName + " where 1=1";
        executeDml(sql);

        final String sourceTableName = "source_brc_tb_with_pk_uk";
        sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ delete from " + sourceTableName + " where 1=1";
        executeDml(sql);

        sql = "replace into " + sourceTableName
            + "(a,b,c) values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(6,6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml("trace " + hintStr + sql);

        sql = "replace into " + tableName
            + "(a,b,c) select a+1-1,b+1-1,c+2-2 from " + sourceTableName + " where a<>1 or 1=1";
        executeDml("trace " + hintStr + sql);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 1; //minus metaDb and scaleout target db
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 2 * topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount));
        }
        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                + "(a,b,c) select a,b,c from " + sourceTableName + " where a=1 and 2+1=3";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 2 * (physicalDbCount + 1) : physicalDbCount + 1); //(delete + insert) * 2
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = (i == 0 ? 2 * physicalDbCount + 1 : physicalDbCount); //(delete + insert) + delete
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "replace into " + tableName
            + "(a,b,c) select a,b,c+1 from " + sourceTableName + " where a=1 and (2=1 or 2=2)";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = physicalDbCount * 2; //(delete + insert) * 2
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert + 2));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            basePhyInsert = 2 * physicalDbCount + 1; //(delete + insert) + delete
            Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount));
        }
    }

    @Test
    public void tablePkNoUkForDelete() {
        final String tableName = "test_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
        executeDml(sql);

        String hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ ";
        } else if (isCache.booleanValue()) {
            hint = " /*+TDDL:cmd_extra(ENABLE_COMPLEX_DML_CROSS_DB=true,PLAN_CACHE=true)*/ ";
        }
        String[] sqlList = new String[] {
            "delete from " + tableName + " where 1=1",
            "delete from " + tableName + " where a < 300",
            "delete from " + tableName + " where a = 1",
            "delete from " + tableName + " where b=1",
            "delete from " + tableName + " where a = 1 and b=1",
            "delete from " + tableName + " where 1=1 order by a limit 6",};
        //{6, 2, 1, 1, 1, 1, 1}
        List<List<String>> trace;
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        int dbCount = getDataSourceCount() - 1 - 1 - 1;//minus metadb+scaleoutdb + singledb
        int[] scans = new int[] {dbCount, dbCount, 1, dbCount, 1, dbCount};
        int[] writablePhysicalOperations =
            new int[] {6, 6, 1, 1, 1, 6};
        int[] readyToPublishPhysicalOperations =
            new int[] {
                dbCount + 6 + 1, dbCount + 6 + 1, 1 + 1 + 1, dbCount + 1 + 1, 1 + 1 + 1, dbCount + 6 + 1};
        int[] otherPhysicalOperations =
            new int[] {topology.size(), topology.size(), 1 * 3, dbCount, 1, dbCount + 6};
        int[] publishPhysicalOperations =
            new int[] {topology.size(), topology.size(), 1 * 3, dbCount + 1, 1, dbCount + 1 + 6};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                + "(a,b) values(1,1), (2,2), (3,3), (4,4), (5,5),(6,6)";
            executeDml(sql);
            sql = sqlList[i];
            int physicalOperation = writablePhysicalOperations[i];
            int otherPhysicalOperation = otherPhysicalOperations[i];
            int readyToPublishPhysicalOperation = readyToPublishPhysicalOperations[i];
            int scan = scans[i];
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);

            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation + 1));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(readyToPublishPhysicalOperation));
            } else if (finalTableStatus.isDeleteOnly()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation + 1));
            } else if ((finalTableStatus.isPublic())) {
                Assert.assertThat(trace.toString(), trace.size(), is(publishPhysicalOperations[i]));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(otherPhysicalOperation));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
            executeDml(sql);
        }
    }

    @Test
    public void tablePkAndUkForDelete() {
        final String tableName = "test_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
        executeDml(sql);
        String hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ ";
        } else if (isCache.booleanValue()) {
            hint = " /*+TDDL:cmd_extra(ENABLE_COMPLEX_DML_CROSS_DB=true,PLAN_CACHE=true)*/ ";
        }
        String[] sqlList = new String[] {
            "delete from " + tableName + " where 1=1",
            "delete from " + tableName + " where a < 300",
            "delete from " + tableName + " where a = 1",
            "delete from " + tableName + " where b=1",
            "delete from " + tableName + " where c=1",
            "delete from " + tableName + " where a = 1 and b=1",
            "delete from " + tableName + " where a = 1 and b=1 and c=1",
            "delete from " + tableName + " where 1=1 order by a limit 6",};
        //{6, 2, 1, 1, 1, 1, 1}
        List<List<String>> trace;
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        int dbCount = getDataSourceCount() - 1 - 1 - 1;//minus metadb+scaleoutdb + singledb
        int[] scans = new int[] {dbCount, dbCount, 1, dbCount, dbCount, 1, 1, dbCount};
        int[] writablePhysicalOperations =
            new int[] {6, 6, 1, 1, 1, 1, 1, 6};
        int[] readyToPublishPhysicalOperations =
            new int[] {
                dbCount + 6 + 1, dbCount + 6 + 1, 1 + 1 + 1, dbCount + 1 + 1, dbCount + 1 + 1,
                1 + 1 + 1, 1 + 1 + 1, dbCount + 6 + 1};
        int[] otherPhysicalOperations =
            new int[] {topology.size(), topology.size(), 1 * 3, dbCount, topology.size(), 1, 1, dbCount + 6};
        int[] publishPhysicalOperations =
            new int[] {topology.size(), topology.size(), 1 * 3, dbCount + 1, topology.size(), 1, 1, dbCount + 1 + 6};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                + "(a,b,c) values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(6,6,6)";
            executeDml(sql);
            sql = sqlList[i];
            int physicalOperation = writablePhysicalOperations[i];
            int otherPhysicalOperation = otherPhysicalOperations[i];
            int readyToPublishPhysicalOperation = readyToPublishPhysicalOperations[i];
            int scan = scans[i];
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);

            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation + 1));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(readyToPublishPhysicalOperation));
            } else if (finalTableStatus.isDeleteOnly()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation + 1));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(publishPhysicalOperations[i]));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(otherPhysicalOperation));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
            executeDml(sql);
        }
    }

    @Test
    public void brdTablePkNoUkForDelete() {
        final String tableName = "test_brc_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
        executeDml(sql);
        String hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ ";
        } else if (isCache.booleanValue()) {
            hint = " /*+TDDL:cmd_extra(ENABLE_COMPLEX_DML_CROSS_DB=true,PLAN_CACHE=true)*/ ";
        }
        String[] sqlList = new String[] {
            "delete from " + tableName + " where 1=1",
            "delete from " + tableName + " where a < 300",
            "delete from " + tableName + " where a = 2",
            "delete from " + tableName + " where b=1",
            "delete from " + tableName + " where a = 1 and b=1",
            "delete from " + tableName + " where 1=1 order by a limit 6",};
        //{6, 2, 1, 1, 1, 1, 1}
        List<List<String>> trace;
        int brdDbCount = getDataSourceCount() - 1 - 1;//minus metadb+scaleoutdb
        int[] scans = new int[] {1, 1, 1, 1, 1, 1, 1};
        int[] physicalOperations =
            new int[] {brdDbCount, brdDbCount, brdDbCount, brdDbCount, brdDbCount, brdDbCount, brdDbCount};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                + "(a,b) values(1,1), (2,2), (3,3), (4,4), (5,5),(6,6)";
            executeDml(sql);
            sql = sqlList[i];
            int physicalOperation = physicalOperations[i];
            int scan = scans[i];
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);

            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation + 1));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation + 1));
            } else if (finalTableStatus.isDeleteOnly()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation + 1));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(brdDbCount + 1));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(brdDbCount));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
            executeDml(sql);
        }
    }

    @Test
    public void brdTablePkAndUkForDelete() {
        final String tableName = "test_brc_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
        executeDml(sql);
        String hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ ";
        } else if (isCache.booleanValue()) {
            hint = " /*+TDDL:cmd_extra(ENABLE_COMPLEX_DML_CROSS_DB=true,PLAN_CACHE=true)*/ ";
        }
        String[] sqlList = new String[] {
            "delete from " + tableName + " where 1=1",
            "delete from " + tableName + " where a < 300",
            "delete from " + tableName + " where a = 2",
            "delete from " + tableName + " where b=1",
            "delete from " + tableName + " where c=1",
            "delete from " + tableName + " where a = 1 and b=1",
            "delete from " + tableName + " where a = 1 and b=1 and c=1",
            "delete from " + tableName + " where 1=1 order by a limit 6",};
        //{6, 2, 1, 1, 1, 1, 1}
        List<List<String>> trace;
        int brdDbCount = getDataSourceCount() - 1 - 1;//minus metadb+scaleoutdb
        int[] scans = new int[] {1, 1, 1, 1, 1, 1, 1, 1};
        int[] physicalOperations =
            new int[] {brdDbCount, brdDbCount, brdDbCount, brdDbCount, brdDbCount, brdDbCount, brdDbCount, brdDbCount};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                + "(a,b,c) values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(6,6,6)";
            executeDml(sql);
            sql = sqlList[i];
            int physicalOperation = physicalOperations[i];
            int scan = scans[i];
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);

            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation + 1));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation + 1));
            } else if (finalTableStatus.isDeleteOnly()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation + 1));
            } else if ((finalTableStatus.isPublic())) {
                Assert.assertThat(trace.toString(), trace.size(), is(brdDbCount + 1));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(brdDbCount));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
            executeDml(sql);
        }
    }

    @Test
    public void tablePkNoUkForUpdate() {
        final String tableName = "test_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
        executeDml(sql);
        String hint =
            "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE,PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/ ";
        } else if (isCache.booleanValue()) {
            hint =
                " /*+TDDL:cmd_extra(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE,PLAN_CACHE=true)*/ ";
        }

        String[] sqlList = new String[] {
            "update " + tableName + " set k=k+1",
            "update " + tableName + " set k=k+1 where a=1",
            "update " + tableName + " set k=k+1 where a=1 and b=1",
            "update " + tableName + " set k=k+1 where b=1",
            "update " + tableName + " set a=a-1 where a<100",//5
            "update " + tableName + " set b=b-1 where a<100",
            "update " + tableName + " set b=b-1,a=a-1 where b<100",
            "update " + tableName + " set a=a-1 where a=1",
            "update " + tableName + " set b=b-1 where a=1",
            "update " + tableName + " set b=b-1,a=a-1 where a=1",//10
            "update " + tableName + " set a=a-1 where a=1 and b=1",
            "update " + tableName + " set b=b-1 where a=1 and b=1",
            "update " + tableName + " set b=b-1,a=a-1 where a=1 and b=1",
            "update " + tableName + " set a=a-1 where b=1",
            "update " + tableName + " set b=b-1 where b=1",//15
            "update " + tableName + " set b=b-1,a=a-1 where b=1",
            "update " + tableName + " set k=k+1 order by a limit 6",
            "update " + tableName + " set a=a-1 where a<>3 order by a limit 6",
            "update " + tableName + " set k=k-1 where a=1 and b=1 order by a limit 6",};
        List<List<String>> trace;
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        int dbCount = getDataSourceCount() - 1 - 1 - 1;//minus metadb+scaleoutdb + singledb
        int[] scans =
            new int[] {
                dbCount, 1, 1, dbCount, dbCount,
                dbCount, dbCount, 1, 1, 1,
                1, 1, 1, dbCount, dbCount,
                dbCount, dbCount, dbCount, 1};
        int[] writablePhysicalOperations =
            new int[] {
                6 + 1, 1 + 1, 1 + 1, 1 + 1, 6 * 2 + 1 + 1,
                6 * 2 + 1 + 1, 6 * 2 + 1 + 1, 1 * 2 + 1, 1 * 2 + 2, 1 * 2 + 1,
                1 * 2 + 1, 1 * 2 + 2, 1 * 2 + 1, 1 * 2 + 1, 1 * 2 + 2,
                1 * 2 + 1, 6 + 1, 5 * 2 + 2, 1 + 1};
        int[] readyToPublishPhysicalOperations =
            new int[] {
                dbCount + 6 + 1, 1 + 1 + 1, 1 + 1 + 1, dbCount + 1 + 1, dbCount + 6 * 2 + 2,
                dbCount + 6 * 2 + 2, dbCount + 6 * 2 + 2, 1 + 1 * 2 + 1, 1 + 1 * 2 + 2, 1 + 1 * 2 + 1,
                1 + 1 * 2 + 1, 1 + 1 * 2 + 2, 1 + 1 * 2 + 1, dbCount + 1 * 2 + 1, dbCount + 1 * 2 + 2,
                dbCount + 1 * 2 + 1, dbCount + 6 + 1, dbCount + 5 * 2 + 2, 1 + 1 + 1};
        int[] otherPhysicalOperations =
            new int[] {
                topology.size(), 3, 1, dbCount, dbCount + 6 * 2,
                dbCount + 6 * 2, dbCount + 6 * 2, 1 + 1 * 2, 1 + 1 * 2, 1 + 1 * 2,
                1 + 1 * 2, 1 + 1 * 2, 1 + 1 * 2, dbCount + 1 * 2, dbCount + 1 * 2,
                dbCount + 1 * 2, dbCount + 6, dbCount + 5 * 2, 1};
        int[] publishPhysicalOperations =
            new int[] {
                topology.size(), 3, 1, dbCount + 1, dbCount + 1 + 6 * 2,
                dbCount + 1 + 6 * 2, dbCount + 1 + 6 * 2, 1 + 1 * 2, 1 + 1 * 2, 1 + 1 * 2,
                1 + 1 * 2, 1 + 1 * 2, 1 + 1 * 2, dbCount + 1 + 1 * 2, dbCount + 1 + 1 * 2,
                dbCount + 1 + 1 * 2, dbCount + 1 + 6, dbCount + 1 + 5 * 2, 1};
        int[] deleteOnlyPhysicalOperations =
            new int[] {
                6 + 1, 1 + 1, 1 + 1, 1 + 1, 6 * 2 + 1,
                6 * 2 + 1, 6 * 2 + 1, 1 * 2 + 1, 1 * 2 + 1, 1 * 2 + 1,
                1 * 2 + 1, 1 * 2 + 1, 1 * 2 + 1, 1 * 2 + 1, 1 * 2 + 1,
                1 * 2 + 1, 6 + 1, 5 * 2 + 1, 1 + 1};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                + "(a,b,k) values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(6,6,6)";
            executeDml(sql);
            sql = sqlList[i];
            int physicalOperation = writablePhysicalOperations[i];
            int otherPhysicalOperation = otherPhysicalOperations[i];
            int readyToPublishPhysicalOperation = readyToPublishPhysicalOperations[i];
            int deleteOnlyPhysicalOperation = deleteOnlyPhysicalOperations[i];
            int scan = scans[i];
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);

            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(readyToPublishPhysicalOperation));
            } else if (finalTableStatus.isDeleteOnly()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scan + deleteOnlyPhysicalOperation));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(publishPhysicalOperations[i]));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(otherPhysicalOperation));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
            executeDml(sql);
        }
    }

    @Test
    public void tablePkAndUkForUpdate() {
        final String tableName = "test_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
        executeDml(sql);
        String hint =
            "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE,PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/ ";
        } else if (isCache.booleanValue()) {
            hint =
                " /*+TDDL:cmd_extra(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE,PLAN_CACHE=true)*/ ";
        }
        String[] sqlList = new String[] {
            "update " + tableName + " set k=k+1",
            "update " + tableName + " set k=k+1 where a=1",
            "update " + tableName + " set k=k+1 where a=1 and b=1",
            "update " + tableName + " set k=k+1 where b=1",
            "update " + tableName + " set a=a-1 where a<100",//5
            "update " + tableName + " set b=b-1 where a<100",
            "update " + tableName + " set b=b-1,a=a-1 where b<100",
            "update " + tableName + " set a=a-1 where a=1",
            "update " + tableName + " set b=b-1 where a=1",
            "update " + tableName + " set b=b-1,a=a-1 where a=1",//10
            "update " + tableName + " set a=a-1 where a=1 and b=1",
            "update " + tableName + " set b=b-1 where a=1 and b=1",
            "update " + tableName + " set b=b-1,a=a-1 where a=1 and b=1",
            "update " + tableName + " set a=a-1 where b=1",
            "update " + tableName + " set b=b-1 where b=1",//15
            "update " + tableName + " set b=b-1,a=a-1 where b=1",
            "update " + tableName + " set k=k+1 order by a limit 6",
            "update " + tableName + " set a=a-1 where a<>3 order by a limit 6",
            "update " + tableName + " set k=k-1 where a=1 and b=1 order by a limit 6",
            "update " + tableName + " set c=c-1 where b=1",
            "update " + tableName + " set c=c-1 where a=1",
        };
        //{6, 2, 1, 1, 1, 1, 1}
        List<List<String>> trace;
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        int dbCount = getDataSourceCount() - 1 - 1 - 1;//minus metadb+scaleoutdb + singledb
        int[] scans =
            new int[] {
                dbCount, 1, 1, dbCount, dbCount,
                dbCount, dbCount, 1, 1, 1,
                1, 1, 1, dbCount, dbCount,
                dbCount, dbCount, dbCount, 1, dbCount,
                1};
        int[] writablePhysicalOperations =
            new int[] {
                6 + 1, 1 + 1, 1 + 1, 1 + 1, 6 * 2 + 1 + 1,
                6 * 2 + 1 + 1, 6 * 2 + 1 + 1, 1 * 2 + 1, 1 * 2 + 2, 1 * 2 + 1,
                1 * 2 + 1, 1 * 2 + 2, 1 * 2 + 1, 1 * 2 + 1, 1 * 2 + 2,
                1 * 2 + 1, 6 + 1, 5 * 2 + 2, 1 + 1, 1 + 1,
                1 + 1};
        int[] readyToPublishPhysicalOperations =
            new int[] {
                dbCount + 6 + 1, 1 + 1 + 1, 1 + 1 + 1, dbCount + 1 + 1, dbCount + 6 * 2 + 2,
                dbCount + 6 * 2 + 2, dbCount + 6 * 2 + 2, 1 + 1 * 2 + 1, 1 + 1 * 2 + 2, 1 + 1 * 2 + 1,
                1 + 1 * 2 + 1, 1 + 1 * 2 + 2, 1 + 1 * 2 + 1, dbCount + 1 * 2 + 1, dbCount + 1 * 2 + 2,
                dbCount + 1 * 2 + 1, dbCount + 6 + 1, dbCount + 5 * 2 + 2, 1 + 1 + 1, dbCount + 1 + 1,
                1 + 1 + 1};
        int[] otherPhysicalOperations =
            new int[] {
                topology.size(), 3, 1, dbCount, dbCount + 6 * 2,
                dbCount + 6 * 2, dbCount + 6 * 2, 1 + 1 * 2, 1 + 1 * 2, 1 + 1 * 2,
                1 + 1 * 2, 1 + 1 * 2, 1 + 1 * 2, dbCount + 1 * 2, dbCount + 1 * 2,
                dbCount + 1 * 2, dbCount + 6, dbCount + 5 * 2, 1, dbCount,
                3};
        int[] deleteOnlyPhysicalOperations =
            new int[] {
                6 + 1, 1 + 1, 1 + 1, 1 + 1, 6 * 2 + 1,
                6 * 2 + 1, 6 * 2 + 1, 1 * 2 + 1, 1 * 2 + 1, 1 * 2 + 1,
                1 * 2 + 1, 1 * 2 + 1, 1 * 2 + 1, 1 * 2 + 1, 1 * 2 + 1,
                1 * 2 + 1, 6 + 1, 5 * 2 + 1, 1 + 1, 1 + 1,
                1 + 1};
        int[] publishPhysicalOperations =
            new int[] {
                topology.size(), 3, 1, dbCount + 1, dbCount + 1 + 6 * 2,
                dbCount + 1 + 6 * 2, dbCount + 1 + 6 * 2, 1 + 1 * 2, 1 + 1 * 2, 1 + 1 * 2,
                1 + 1 * 2, 1 + 1 * 2, 1 + 1 * 2, dbCount + 1 + 1 * 2, dbCount + 1 + 1 * 2,
                dbCount + 1 + 1 * 2, dbCount + 1 + 6, dbCount + 1 + 5 * 2, 1, dbCount + 1,
                3};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                + "(a,b,c,k) values(1,1,1,1), (2,2,2,2), (3,3,3,3), (4,4,4,4), (5,5,5,5),(6,6,6,6)";
            executeDml(sql);
            sql = sqlList[i];
            int physicalOperation = writablePhysicalOperations[i];
            int otherPhysicalOperation = otherPhysicalOperations[i];
            int readyToPublishPhysicalOperation = readyToPublishPhysicalOperations[i];
            int deleteOnlyPhysicalOperation = deleteOnlyPhysicalOperations[i];
            int scan = scans[i];
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);

            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(readyToPublishPhysicalOperation));
            } else if (finalTableStatus.isDeleteOnly()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scan + deleteOnlyPhysicalOperation));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(publishPhysicalOperations[i]));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(otherPhysicalOperation));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
            executeDml(sql);
        }
    }

    @Test
    public void brdTablePkNoUkForUpdate() {
        final String tableName = "test_brc_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
        executeDml(sql);
        String hint =
            "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE,PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/ ";
        } else if (isCache.booleanValue()) {
            hint =
                " /*+TDDL:cmd_extra(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE,PLAN_CACHE=true)*/ ";
        }
        String[] sqlList = new String[] {
            "update " + tableName + " set k=k+1",
            "update " + tableName + " set k=k+1 where a=1",
            "update " + tableName + " set k=k+1 where a=1 and b=1",
            "update " + tableName + " set k=k+1 where b=1",
            "update " + tableName + " set a=a-1 where a<100",//5
            "update " + tableName + " set b=b-1 where a<100",
            "update " + tableName + " set b=b-1,a=a-1 where b<100",
            "update " + tableName + " set a=a-1 where a=1",
            "update " + tableName + " set b=b-1 where a=1",
            "update " + tableName + " set b=b-1,a=a-1 where a=1",//10
            "update " + tableName + " set a=a-1 where a=1 and b=1",
            "update " + tableName + " set b=b-1 where a=1 and b=1",
            "update " + tableName + " set b=b-1,a=a-1 where a=1 and b=1",
            "update " + tableName + " set a=a-1 where b=1",
            "update " + tableName + " set b=b-1 where b=1",//15
            "update " + tableName + " set b=b-1,a=a-1 where b=1",
            "update " + tableName + " set k=k+1 order by a limit 6",
            "update " + tableName + " set a=a+10 where a<>3 order by a limit 6",
            "update " + tableName + " set k=k-1 where a=1 and b=1 order by a limit 6",};
        //{6, 2, 1, 1, 1, 1, 1}
        List<List<String>> trace;
        int brdDbCount = getDataSourceCount() - 1 - 1;//minus metadb+scaleoutdb
        int scan = 1;
        int[] writablePhysicalOperations =
            new int[] {
                scan + brdDbCount + 1, scan + brdDbCount + 1, scan + brdDbCount + 1, scan + brdDbCount + 1,
                scan + brdDbCount * 2 + 2,
                scan + brdDbCount + 1, scan + brdDbCount * 2 + 2, scan + brdDbCount * 2 + 2,
                scan + brdDbCount + 1, scan + brdDbCount * 2 + 2,
                scan + brdDbCount * 2 + 2, scan + brdDbCount + 1, scan + brdDbCount * 2 + 2,
                scan + brdDbCount * 2 + 2, scan + brdDbCount + 1,
                scan + brdDbCount * 2 + 2, scan + brdDbCount + 1, scan + brdDbCount * 2 + 2, scan + brdDbCount + 1};
        int[] readyToPublishPhysicalOperations =
            new int[] {
                scan + brdDbCount + 1, scan + brdDbCount + 1, scan + brdDbCount + 1, scan + brdDbCount + 1,
                scan + brdDbCount * 2 + 2,
                scan + brdDbCount + 1, scan + brdDbCount * 2 + 2, scan + brdDbCount * 2 + 2,
                scan + brdDbCount + 1, scan + brdDbCount * 2 + 2,
                scan + brdDbCount * 2 + 2, scan + brdDbCount + 1, scan + brdDbCount * 2 + 2,
                scan + brdDbCount * 2 + 2, scan + brdDbCount + 1,
                scan + brdDbCount * 2 + 2, scan + brdDbCount + 1, scan + brdDbCount * 2 + 2, scan + brdDbCount + 1};
        int[] otherPhysicalOperations =
            new int[] {
                brdDbCount, brdDbCount, brdDbCount, brdDbCount, brdDbCount,
                brdDbCount, brdDbCount, brdDbCount, brdDbCount, brdDbCount,
                brdDbCount, brdDbCount, brdDbCount, brdDbCount, brdDbCount,
                brdDbCount, brdDbCount, brdDbCount, brdDbCount};
        int[] deleteOnlyPhysicalOperations =
            new int[] {
                scan + brdDbCount + 1, scan + brdDbCount + 1, scan + brdDbCount + 1, scan + brdDbCount + 1,
                scan + brdDbCount * 2 + 1,
                scan + brdDbCount + 1, scan + brdDbCount * 2 + 1, scan + brdDbCount * 2 + 1,
                scan + brdDbCount + 1, scan + brdDbCount * 2 + 1,
                scan + brdDbCount * 2 + 1, scan + brdDbCount + 1, scan + brdDbCount * 2 + 1,
                scan + brdDbCount * 2 + 1, scan + brdDbCount + 1,
                scan + brdDbCount * 2 + 1, scan + brdDbCount + 1, scan + brdDbCount * 2 + 1, scan + brdDbCount + 1};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                + "(a,b,k) values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(6,6,6)";
            executeDml(sql);
            sql = sqlList[i];
            int physicalOperation = brdDbCount;
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);

            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(writablePhysicalOperations[i]));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(readyToPublishPhysicalOperations[i]));
            } else if (finalTableStatus.isDeleteOnly()) {
                Assert.assertThat(trace.toString(), trace.size(), is(deleteOnlyPhysicalOperations[i]));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(otherPhysicalOperations[i] + 1));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(otherPhysicalOperations[i]));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
            executeDml(sql);
        }
    }

    @Test
    public void brdTablePkAndUkForUpdate() {
        final String tableName = "test_brc_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
        executeDml(sql);
        String hint =
            "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE,PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/ ";
        } else if (isCache.booleanValue()) {
            hint =
                " /*+TDDL:cmd_extra(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE,PLAN_CACHE=true)*/ ";
        }
        String[] sqlList = new String[] {
            "update " + tableName + " set k=k+1",
            "update " + tableName + " set k=k+1 where a=1",
            "update " + tableName + " set k=k+1 where a=1 and b=1",
            "update " + tableName + " set k=k+1 where b=1",
            "update " + tableName + " set a=a-1 where a<100",//5
            "update " + tableName + " set b=b-1 where a<100",
            "update " + tableName + " set b=b-1,a=a-1 where b<100",
            "update " + tableName + " set a=a-1 where a=1",
            "update " + tableName + " set b=b-1 where a=1",
            "update " + tableName + " set b=b-1,a=a-1 where a=1",//10
            "update " + tableName + " set a=a-1 where a=1 and b=1",
            "update " + tableName + " set b=b-1 where a=1 and b=1",
            "update " + tableName + " set b=b-1,a=a-1 where a=1 and b=1",
            "update " + tableName + " set a=a-1 where b=1",
            "update " + tableName + " set b=b-1 where b=1",//15
            "update " + tableName + " set b=b-1,a=a-1 where b=1",
            "update " + tableName + " set k=k+1 order by a limit 6",
            "update " + tableName + " set a=a+10 where a<>3 order by a limit 6",
            "update " + tableName + " set k=k-1 where a=1 and b=1 order by a limit 6",
            "update " + tableName + " set c=c-1 where b=1",//20
            "update " + tableName + " set c=c-1 where a=1",
        };
        List<List<String>> trace;
        int brdDbCount = getDataSourceCount() - 1 - 1;//minus metadb+scaleoutdb
        int scan = 1;
        int[] writablePhysicalOperations =
            new int[] {
                scan + brdDbCount + 1, scan + brdDbCount + 1, scan + brdDbCount + 1, scan + brdDbCount + 1,
                scan + brdDbCount * 2 + 2,
                scan + brdDbCount * 2 + 2, scan + brdDbCount * 2 + 2, scan + brdDbCount * 2 + 2,
                scan + brdDbCount * 2 + 2, scan + brdDbCount * 2 + 2,
                scan + brdDbCount * 2 + 2, scan + brdDbCount * 2 + 2, scan + brdDbCount * 2 + 2,
                scan + brdDbCount * 2 + 2, scan + brdDbCount * 2 + 2,
                scan + brdDbCount * 2 + 2, scan + brdDbCount + 1, scan + brdDbCount * 2 + 2, scan + brdDbCount + 1,
                scan + brdDbCount + 1,
                scan + brdDbCount + 1};
        int[] readyToPublishPhysicalOperations =
            new int[] {
                scan + brdDbCount + 1, scan + brdDbCount + 1, scan + brdDbCount + 1, scan + brdDbCount + 1,
                scan + brdDbCount * 2 + 2,
                scan + brdDbCount * 2 + 2, scan + brdDbCount * 2 + 2, scan + brdDbCount * 2 + 2,
                scan + brdDbCount * 2 + 2, scan + brdDbCount * 2 + 2,
                scan + brdDbCount * 2 + 2, scan + brdDbCount * 2 + 2, scan + brdDbCount * 2 + 2,
                scan + brdDbCount * 2 + 2, scan + brdDbCount * 2 + 2,
                scan + brdDbCount * 2 + 2, scan + brdDbCount + 1, scan + brdDbCount * 2 + 2, scan + brdDbCount + 1,
                scan + brdDbCount + 1,
                scan + brdDbCount + 1};
        int[] otherPhysicalOperations =
            new int[] {
                brdDbCount, brdDbCount, brdDbCount, brdDbCount, brdDbCount,
                brdDbCount, brdDbCount, brdDbCount, brdDbCount, brdDbCount,
                brdDbCount, brdDbCount, brdDbCount, brdDbCount, brdDbCount,
                brdDbCount, brdDbCount, brdDbCount, brdDbCount, brdDbCount,
                brdDbCount};
        int[] deleteOnlyPhysicalOperations =
            new int[] {
                scan + brdDbCount + 1, scan + brdDbCount + 1, scan + brdDbCount + 1, scan + brdDbCount + 1,
                scan + brdDbCount * 2 + 1,
                scan + brdDbCount * 2 + 1, scan + brdDbCount * 2 + 1, scan + brdDbCount * 2 + 1,
                scan + brdDbCount * 2 + 1, scan + brdDbCount * 2 + 1,
                scan + brdDbCount * 2 + 1, scan + brdDbCount * 2 + 1, scan + brdDbCount * 2 + 1,
                scan + brdDbCount * 2 + 1, scan + brdDbCount * 2 + 1,
                scan + brdDbCount * 2 + 1, scan + brdDbCount + 1, scan + brdDbCount * 2 + 1, scan + brdDbCount + 1,
                scan + brdDbCount + 1,
                scan + brdDbCount + 1};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                + "(a,b,c,k) values(1,1,1,1), (2,2,2,2), (3,3,3,3), (4,4,4,4), (5,5,5,5),(6,6,6,6)";
            executeDml(sql);
            sql = sqlList[i];
            int physicalOperation = brdDbCount;
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);
            System.out.println("i = " + String.valueOf(i));
            System.out.println("sql = " + sql);

            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(writablePhysicalOperations[i]));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(readyToPublishPhysicalOperations[i]));
            } else if (finalTableStatus.isDeleteOnly()) {
                Assert.assertThat(trace.toString(), trace.size(), is(deleteOnlyPhysicalOperations[i]));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(otherPhysicalOperations[i] + 1));
            } else {
                Assert.assertThat(trace.toString(), trace.size(), is(otherPhysicalOperations[i]));
            }
            sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
            executeDml(sql);
        }
    }

    @Test
    public void tablePkNoUkForUpsert() {
        final String tableName = "test_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);

        sql = "insert into " + tableName
            + "(a,b) values(1,1), (2,2), (3,3), (4-2+2,4), (5,5),(9+6-9,6) on duplicate key update b=b+1+3";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int shrdDbCount = getDataSourceCount() - 1 - 1 - 1;//minus metadb+scaleoutdb+singledb
        int basePhyInsert = 6;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + topology.size() + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + topology.size()));
        } else {
            //select_for_duplicate + insert
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
        }
        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                + "(a,b) values(1,1+1-1) on duplicate key update b=b+1+3";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i == 0 ? 4 : 2;
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else if (finalTableStatus.isDeleteOnly()) {
                // 3：2*delete + insert; 1: insert
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + (i == 0 ? 3 : 1)));
            } else {
                //select_for_duplicate + insert
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + (i == 0 ? 2 : 1)));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "insert into " + tableName
            + "(a,b) values(1+1-1,2) on duplicate key update a=a+" + String.valueOf(shrdDbCount) + "*20, b=b+2";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = 4; //4:(delete + insert) * 2
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
        } else if (finalTableStatus.isDeleteOnly()) {
            basePhyInsert = 3; //3:(delete + insert) + delete
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
        } else {
            //select_for_duplicate + delete + insert
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + 1 + 1));
        }
    }

    @Test
    public void tablePkNoUkForUpsertSelect() {
        final String tableName = "test_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);

        final String sourceTableName = "source_tb_with_pk_no_uk";
        sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + sourceTableName + " where 1=1";
        executeDml(sql);

        sql = "insert into " + sourceTableName
            + "(a,b) values(1,1), (2,2), (3,3), (4,4), (5,5),(6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false, MERGE_UNION=false)*/ ";
        if (isCache == null) {
            hintStr = " /*+TDDL:cmd_extra(MERGE_UNION=false)*/ ";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true,MERGE_UNION=false)*/ ";
        }
        executeDml(hintStr + sql);

        sql = "insert into " + tableName
            + "(a,b) select a+1-1,b+1-1 from " + sourceTableName + " on duplicate key update b=b+1+3";
        executeDml("trace " + hintStr + sql);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int shrdDbCount = getDataSourceCount() - 1 - 1 - 1;//minus metadb+scaleoutdb+singledb
        int basePhyInsert = 6;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + 2 * topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + 2 * topology.size() + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + 2 * topology.size()));
        } else {
            //select + select_for_duplicate + insert
            Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
        }
        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                + "(a,b) select a+1-1,b from " + sourceTableName + " where a=1+1-1 on duplicate key update b=b+1+3";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i == 0 ? 4 : 2;
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
            } else if (finalTableStatus.isDeleteOnly()) {
                // 3：2*delete + insert; 1: insert
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + (i == 0 ? 3 : 1)));
            } else {
                //0:select + select_for_duplicate + delete + insert
                //1:select + select_for_duplicate + insert
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + (i == 0 ? 2 : 1)));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "insert into " + tableName
            + "(a,b) select a,b+1 from " + sourceTableName + " where a=1 on duplicate key update a=a+" + String
            .valueOf(shrdDbCount) + "*20, b=b+2";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = 4; //4:(delete + insert) * 2
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + topology.size() + basePhyInsert));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + topology.size() + basePhyInsert));
        } else if (finalTableStatus.isDeleteOnly()) {
            basePhyInsert = 3; //3:(delete + insert) + delete
            Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + topology.size() + basePhyInsert));
        } else {
            //select + select_for_duplicate + delete + insert
            Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + topology.size() + 1 + 1));
        }
    }

    @Test
    public void tablePkAndUkForUpsert() {
        final String tableName = "test_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);

        sql = "insert into " + tableName
            + "(a,b,c) values(1,1,1), (2,2+1-1,2), (3,3,3), (4,4,4), (5,5+2-2,5),(6,6,6) on duplicate key update a=a+1";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml("trace " + hintStr + sql);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int scan = 6;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + scan + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + scan + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + scan));
        } else {
            //0:select_for_duplicate + insert
            Assert.assertThat(trace.toString(), trace.size(), is(scan + basePhyInsert));
        }
        int scanForDuplicateCheck = 1;
        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                + "(a,b,c) values(1,1+1-1,1) on duplicate key update b=b+1";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 4 : 2); //4:(delete + insert) * 2
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = (i == 0 ? 3 : 1); //3:(delete + insert) + delete
                Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));
            } else {
                //0:select_for_duplicate + delete + insert
                //1:select_for_duplicate + insert
                Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + (i == 0 ? 2 : 1)));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "insert into " + tableName
            + "(a,b,c) values(1+1-1,1,3) on duplicate key update b=b+1";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = 4; //4:(delete + insert) * 2
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));
        } else if (finalTableStatus.isDeleteOnly()) {
            basePhyInsert = 3; //3:(delete + insert) + delete
            Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));
        } else {
            //0:select_for_duplicate + delete + insert
            Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + 1 + 1));
        }
    }

    @Test
    public void tablePkAndUkForUpsertSelect() {
        final String tableName = "test_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);

        final String sourceTableName = "source_tb_with_pk_uk";
        sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + sourceTableName + " where 1=1";
        executeDml(sql);

        sql = "insert into " + sourceTableName
            + "(a,b,c) values(1,1,1), (2,2,2+1-1), (3,3,3), (4,4,4), (5,5,5),(6,6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false, MERGE_UNION=false)*/ ";
        if (isCache == null) {
            hintStr = " /*+TDDL:cmd_extra(MERGE_UNION=false)*/ ";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true,MERGE_UNION=false)*/ ";
        }
        executeDml(hintStr + sql);

        sql = "insert into " + tableName
            + "(a,b,c) select a+1-1,b,c from " + sourceTableName + " where a<>2+12324 on duplicate key update a=a+1";
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(2 * basePhyInsert + topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(2 * basePhyInsert + topology.size() + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(2 * basePhyInsert + topology.size()));
        } else {
            //select + select_for_duplicate + insert
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert + basePhyInsert));
        }
        // 1 is the second time to find the duplicate record
        int scanForDuplicateCheck = 1;
        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                + "(a,b,c) select a,b+2-2,c from " + sourceTableName
                + " where a=1 and 2=2 on duplicate key update b=b+2";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 4 : 2); //4:(delete + insert) * 2
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(),
                    is(tbPartitions + scanForDuplicateCheck + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(),
                    is(tbPartitions + scanForDuplicateCheck + basePhyInsert));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = (i == 0 ? 3 : 1); //3:(delete + insert) + delete
                Assert.assertThat(trace.toString(), trace.size(),
                    is(tbPartitions + scanForDuplicateCheck + basePhyInsert));
            } else {
                //0：select + select_for_duplicate + delete + insert
                //1：select + select_for_duplicate + insert
                Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + (i == 0 ? 3 : 2)));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "insert into " + tableName
            + "(a,b,c) select a,b,c+2 from " + sourceTableName + " where a=1 on duplicate key update b=b+20";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = 4; //4:(delete + insert) * 2
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + scanForDuplicateCheck + basePhyInsert));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + scanForDuplicateCheck + basePhyInsert));
        } else if (finalTableStatus.isDeleteOnly()) {
            basePhyInsert = 3; //3:(delete + insert) + delete
            Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + scanForDuplicateCheck + basePhyInsert));
        } else {
            //select + select_for_duplicate + delete + insert
            Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + 1 + 1 + 1));
        }
    }

    @Test
    public void brdTablePkNoUkForUpsert() {
        final String tableName = "test_brc_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "insert into " + tableName
            + "(a,b) values(1+1-1,1), (2,2+1-1), (3,3), (4,4), (5,5),(6,6) on duplicate key update b=b+20";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 1; //minus metaDb and scaleout target db
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            //push upsert
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size()));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else {
            //push upsert
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount));
        }
        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                + "(a,b) values(1+2-2,1) on duplicate key update b=b+20";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = physicalDbCount + 1;
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                //push upsert
                Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert));
            } else if (finalTableStatus.isDeleteOnly()) {
                Assert.assertThat(trace.toString(), trace.size(),
                    is(topology.size() + (i == 0 ? basePhyInsert : physicalDbCount)));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
            } else {
                //push upsert
                Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "insert into " + tableName
            + "(a,b) values(1,2+1-1) on duplicate key update a=a+20";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = physicalDbCount + 1;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount * 2 + 2));
        } else if (finalTableStatus.isReadyToPublic()) {
            //push upsert
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount * 2 + 1));
        } else if ((finalTableStatus.isPublic())) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount));
        }
    }

    @Test
    public void brdTablePkNoUkForUpsertSelect() {
        final String tableName = "test_brc_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);

        final String sourceTableName = "source_brc_tb_with_pk_no_uk";
        sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + sourceTableName + " where 1=1";
        executeDml(sql);

        sql = "insert into " + sourceTableName
            + "(a,b) values(1,1), (2,2), (3,3), (4,4), (5,5),(6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml(hintStr + sql);

        sql = "insert into " + tableName
            + "(a,b) select a,b+1-1 from " + sourceTableName
            + " where 1=1 and a<>1+23456 on duplicate key update b=b+20";
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 1; //minus metaDb and scaleout target db
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 2 * topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            //select + push upsert
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + physicalDbCount));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
        } else {
            //select + push upsert
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount));
        }
        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                + "(a,b) select a+1-1,b from " + sourceTableName
                + " where 2=2 and a=1+1-1 on duplicate key update b=b+20";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = physicalDbCount + 1;
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
            } else if (finalTableStatus.isReadyToPublic()) {
                //select + push upsert
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
            } else if (finalTableStatus.isDeleteOnly()) {
                basePhyInsert = (i == 0 ? basePhyInsert : physicalDbCount); //(delete + insert) + delete
                Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
            } else {
                //select + push upsert
                Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "insert into " + tableName
            + "(a,b) select a,b+1 from " + sourceTableName + " where a=1 and 2=2 on duplicate key update b=b+20";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = physicalDbCount + 1;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));
        } else if (finalTableStatus.isReadyToPublic()) {
            //select + pushdown upsert
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + physicalDbCount + 1));
        } else if ((finalTableStatus.isPublic())) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
        } else {
            //select + pushdown upsert
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount));
        }
    }

    @Test
    public void brdTablePkAndUkForUpsert() {
        final String tableName = "test_brc_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "insert into " + tableName
            + "(a,b,c) values(1,1+1-1,1), (2+1-1,2,2), (3,3,3), (4,4,4), (5,5,5),(6,6,6) on duplicate key update b=b+20";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 1; //minus metaDb and scaleout target db
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            //pushdown upsert
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size()));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 1));
        } else {
            // pushdown upsert
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount));
        }
        int scan = topology.size();
        int[] writablePhysicalOperations =
            new int[] {scan + physicalDbCount * 2 + 2, scan + physicalDbCount + 1};
        int[] readyToPublishPhysicalOperations =
            new int[] {physicalDbCount + 1, physicalDbCount + 1};
        int[] otherPhysicalOperations =
            new int[] {physicalDbCount, physicalDbCount};
        int[] deleteOnlyPhysicalOperations =
            new int[] {scan + physicalDbCount * 2 + 1, scan + physicalDbCount};
        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                + "(a,b,c) values(1,1+1-1,1) on duplicate key update b=b+20";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(writablePhysicalOperations[i]));
            } else if (finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(readyToPublishPhysicalOperations[i]));
            } else if (finalTableStatus.isDeleteOnly()) {
                Assert.assertThat(trace.toString(), trace.size(), is(deleteOnlyPhysicalOperations[i]));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(otherPhysicalOperations[i] + 1));
            } else {
                // pushdown upsert
                Assert.assertThat(trace.toString(), trace.size(), is(otherPhysicalOperations[i]));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "insert into " + tableName
            + "(a,b,c) values(1+1-1,1,2+1-1) on duplicate key update b=b+20";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        int i = 0;
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(writablePhysicalOperations[i]));
        } else if (finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(readyToPublishPhysicalOperations[i]));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(deleteOnlyPhysicalOperations[i]));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(otherPhysicalOperations[i] + 1));
        } else {
            // pushdown upsert
            Assert.assertThat(trace.toString(), trace.size(), is(otherPhysicalOperations[i]));
        }
    }

    @Test
    public void brdTablePkAndUkForUpsertSelect() {
        final String tableName = "test_brc_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ delete from " + tableName + " where 1=1";
        executeDml(sql);

        final String sourceTableName = "source_brc_tb_with_pk_uk";
        sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ delete from " + sourceTableName + " where 1=1";
        executeDml(sql);

        sql = "insert into " + sourceTableName
            + "(a,b,c) values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(6,6,6)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hintStr = "";
        } else if (isCache.booleanValue()) {
            hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=true)*/ ";
        }
        executeDml("trace " + hintStr + sql);

        sql = "insert into " + tableName
            + "(a,b,c) select a,b,c+1-1 from " + sourceTableName + " where a<>1 or 1=1 on duplicate key update b=b+20";
        executeDml("trace " + hintStr + sql);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 1; //minus metaDb and scaleout target db
        if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 2 * topology.size() + 1));
        } else if (finalTableStatus.isReadyToPublic()) {
            //select + push upsert
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 1));
        } else if (finalTableStatus.isDeleteOnly()) {
            Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 2 * topology.size()));
        } else if (finalTableStatus.isPublic()) {
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount + 1));
        } else {
            //select + push upsert
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + physicalDbCount));
        }
        int scan = topology.size();
        int[] writablePhysicalOperations =
            new int[] {scan + 1 + physicalDbCount * 2 + 2, scan + 1 + physicalDbCount + 1};
        int[] readyToPublishPhysicalOperations =
            new int[] {scan + physicalDbCount + 1, scan + physicalDbCount + 1};
        int[] otherPhysicalOperations =
            new int[] {1 + physicalDbCount, 1 + physicalDbCount};
        int[] deleteOnlyPhysicalOperations =
            new int[] {scan + 1 + physicalDbCount * 2 + 1, scan + 1 + physicalDbCount};
        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                + "(a,b,c) select a+1-1,b,c from " + sourceTableName
                + " where a=1 and 2+1=3 on duplicate key update b=b+20";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);
            if (finalTableStatus.isWritable() && !finalTableStatus.isReadyToPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(writablePhysicalOperations[i]));
            } else if (finalTableStatus.isReadyToPublic()) {
                //select + push upsert
                Assert.assertThat(trace.toString(), trace.size(), is(readyToPublishPhysicalOperations[i]));
            } else if (finalTableStatus.isDeleteOnly()) {
                Assert.assertThat(trace.toString(), trace.size(), is(deleteOnlyPhysicalOperations[i]));
            } else if (finalTableStatus.isPublic()) {
                Assert.assertThat(trace.toString(), trace.size(), is(otherPhysicalOperations[i] + 1));
            } else {
                //select + push upsert
                Assert.assertThat(trace.toString(), trace.size(), is(otherPhysicalOperations[i]));
            }
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
    }
}