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
import org.apache.calcite.util.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.jcip.annotations.NotThreadSafe;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;

/**
 * @author luoyanxin
 */
@NotThreadSafe
public class ScaleOutPlanWithMultiGroupMoveTest extends ScaleOutBaseTest {

    private static List<String> moveTableStatus =
            Stream.of(
                    ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY.toString(),
                    ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY.toString(),
                    ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC.toString()).collect(Collectors.toList());
    final static int tbPartitions = 3;
    static boolean firstIn = true;

    public ScaleOutPlanWithMultiGroupMoveTest() {
        super("ScaleOutPlanMultiMoveTest", "polardbx_meta_db_polardbx",
                moveTableStatus);
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

    @Test
    public void insertIgnorePushDownOnNonScaleOutGroup() {

        Optional<String> group01 = sourceGroupKeys.stream().filter(o -> o.contains("000001")).findFirst();
        if (!group01.isPresent()) {
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
                "  insert ignore into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123460+4*3, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";

        String hintStr = "";

        executeDml("trace " + hintStr + sql2);
        List<List<String>> trace2 = getTrace(tddlConnection);

        executeDml("trace " + hintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsertSqlCnt = 1;
        int scanForDuplicateCheck = 1;
        Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsertSqlCnt + 1));
        Assert.assertThat(trace2.toString(), trace2.size(), is(basePhyInsertSqlCnt));

    }

    @Test
    public void replacePushDownOnNonScaleOutGroup() {

        Optional<String> group01 = sourceGroupKeys.stream().filter(o -> o.contains("000001")).findFirst();
        if (!group01.isPresent()) {
            return;
        }
        final String tableName = "mdb_mtb_mk1";

        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        String sql2 = "";
        executeDml(sql);

        // shard on scaleout-group
        sql =
                "replace into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123460, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";
        // shard on non-scaleout-group, should be pushdown
        sql2 =
                "replace into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123460+4*3, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";

        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";

        executeDml("trace " + hintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);

        executeDml("trace " + hintStr + sql2);
        List<List<String>> trace2 = getTrace(tddlConnection);

        int basePhyInsertSqlCnt = 1;
        int scanForDuplicateCheck = 1;
        Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsertSqlCnt + 1));
        Assert.assertThat(trace2.toString(), trace2.size(), is(basePhyInsertSqlCnt));

    }

    @Test
    public void updatePushDownOnNonScaleOutGroup() {

        Optional<String> group01 = sourceGroupKeys.stream().filter(o -> o.contains("000001")).findFirst();
        if (!group01.isPresent()) {
            return;
        }
        final String tableName = "mdb_mtb_mk1";

        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        String sql2 = "";
        executeDml(sql);
        String insert1 =
                "  insert ignore into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123456+4, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";
        executeDml(insert1);
        String insert2 =
                "  insert ignore into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123460+4*3, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";
        executeDml(insert2);

        // shard on scaleout-group
        sql = "update mdb_mtb_mk1 set varchar_test='2' where pk=123456+4";
        // shard on non-scaleout-group, should be pushdown
        sql2 = "update mdb_mtb_mk1 set varchar_test='3' where pk=123456";

        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";

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
        Optional<String> group01 = sourceGroupKeys.stream().filter(o -> o.contains("000001")).findFirst();
        if (!group01.isPresent()) {
            return;
        }
        final String tableName = "mdb_mtb_mk1";

        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        String sql2 = "";
        executeDml(sql);
        String insert1 =
                "  insert ignore into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123456+4, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";
        executeDml(insert1);
        String insert2 =
                "  insert ignore into `mdb_mtb_mk1` (pk, integer_test, varchar_test, datetime_test, timestamp_test) values (123460+4*3, 1, '1000', '2020-12-12 12:12:12', '2021-12-12 12:12:12');";
        executeDml(insert2);

        // shard on scaleout-group
        sql = "delete from  mdb_mtb_mk1 where pk=123456+4";
        // shard on non-scaleout-group, should be pushdown
        sql2 = "delete from mdb_mtb_mk1 where pk=123456";

        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";

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
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false,DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ";

        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        //finalTableStatus.size() - 1(delete_only) = multiWrite
        Assert.assertThat(trace.toString(), trace.size(),
                is(basePhyInsert * 2 + finalTableStatus.size() - 1));

        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                    + "(a,b) values(0,0),(1,1),(2,2),(3,3)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i * 1 + i * 2 + i * 2 + 1;
            Assert.assertThat(trace.toString(), trace.size(), is(4 + basePhyInsert));

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
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false, MERGE_UNION=false,DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ";

        executeDml(hintStr + sql);
        sql = "insert ignore into " + tableName + "(a,b) select a,b+100 from " + sourceTableName;
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert * 2 + topology.size() + 0 + 1 + 1));
        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                    + "(a,b) select a,( 1+2-2) from " + sourceTableName + " where a = 1+1-1";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = 2;
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + 1 + basePhyInsert));
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

        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int scanCount = 6;
        Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + scanCount + 0 + 1 + 1));
        int scanForDuplicateCheck = 1;
        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                    + "(a,b,c) values(1,1,1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i * 2;
            Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));
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
        Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + scan + topology.size() + 0 + 1 + 1));

        int scanForDuplicateCheck = 1;
        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                    + "(a,b,c) select a,b+1-1,c from " + sourceTableName + " where a=(select 1) and b<>2+100";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i * 2;
            Assert.assertThat(trace.toString(), trace.size(),
                    is(scanForDuplicateCheck + topology.size() + basePhyInsert));
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

        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 3; //minus metaDb and scaleout target db
        Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 0 + 1 + 1));

        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                    + "(a,b) values(1+1-1,1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i > 0 ? i * physicalDbCount + 0 + 1 + 1 : 0;
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));
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

        executeDml(hintStr + sql);
        sql = "insert ignore into " + tableName
                + "(a,b) select a+1-1,b+20 from " + sourceTableName + " where a<> ( 1000)";
        executeDml("trace " + hintStr + sql);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 3; //minus metaDb and scaleout target db
        Assert.assertThat(trace.toString(), trace.size(),
                is(1/*scan*/ + physicalDbCount/*select*/ + topology.size()/*insert*/ + 0 + 1 + 1));

        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                    + "(a,b) select a+1-1,b+1 from " + sourceTableName + " where a=( 1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i > 0 ? i * physicalDbCount + 0 + 1 + 1 : 0;
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + 1 + basePhyInsert));

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

        executeDml("trace " + hintStr + sql);
        int physicalDbCount = getDataSourceCount() - 1 - 3; //minus metaDb and scaleout target db
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 0 + 1 + 1));

        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                    + "(a,b,c) values(1,1,1),(2,2,2),(6,6,6)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i > 0 ? i * physicalDbCount + 0 + 1 + 1 : 0;
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));

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

        executeDml(hintStr + sql);

        sql = "insert ignore into " + tableName
                + "(a,b,c) select a+1-1,b+1-1,c from " + sourceTableName + " where a<>2000 and b<1+2+2000";
        executeDml("trace " + hintStr + sql);
        int physicalDbCount = getDataSourceCount() - 1 - 3; //minus metaDb and scaleout target db
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        Assert.assertThat(trace.toString(), trace.size(), is(1 + physicalDbCount + topology.size() + 0 + 1 + 1));

        for (int i = 0; i < 2; i++) {
            sql = "insert ignore into " + tableName
                    + "(a,b,c) select a+1-1,b,c+1-1 from " + sourceTableName
                    + " where (a=1 or a=2 or a=0) and b<>1+9999+32";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i > 0 ? i * physicalDbCount + 0 + 1 + 1 : 0;
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert + 1));

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
                + "(a,b) values(1+1-1,1), (2,2), (3,3), (4,4), (5,5),(0,0+8-8)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false,DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ";

        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert * 2 + 0 + 1 + 1));

        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                    + "(a,b) values(1+1-1,1),(0+1-1,0),(2+1-1,2)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 4 + 4 + (2 + 1) : 3 + 0 + 1 + 1); //4:(delete + insert) * 2
            Assert.assertThat(trace.toString(), trace.size(), is(3 + basePhyInsert));

            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "replace into " + tableName
                + "(a,b) values(5,5)";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        Assert.assertThat(trace.toString(), trace.size(), is(1 + 1));
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
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false, MERGE_UNION=false, DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ";

        executeDml(hintStr + sql);

        sql = "replace into " + tableName
                + "(a,b) select a,b+20 from " + sourceTableName + " where b<>( 200+321)";
        executeDml("trace " + hintStr + sql);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert * 2 + topology.size() + 0 + 1 + 1));

        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                    + "(a,b) select a+1-1,b from " + sourceTableName + " where a=1+1+2-3 and b<>( 1234+3456)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = 2; //4:(delete + insert) * 2
            Assert.assertThat(trace.toString(), trace.size(), is(1 + basePhyInsert + topology.size()));

            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "replace into " + tableName
                + "(a,b) select a,b+1 from " + sourceTableName + " where a=1";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = 2; //2:(insert) * 2
        Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + 1 + basePhyInsert));

    }

    @Test
    public void tablePkAndUkForReplace() {
        final String tableName = "test_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "replace into " + tableName
                + "(a,b,c) values(1+1-1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5+1-1),(0,0,0)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";

        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int scan = 6;
        Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + scan + 0 + 1 + 1));

        int scanForDuplicateCheck = 1;
        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                    + "(a,b,c) values(1+1-1,1,1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 4 : 2); //4:(delete + insert) * 2
            Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));

            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "replace into " + tableName
                + "(a,b,c) values(5,5,3)";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        //pushdown
        Assert.assertThat(trace.toString(), trace.size(), is(1));

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
                + "(a,b,c) values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(0,0,0)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false, MERGE_UNION=false)*/ ";

        executeDml(hintStr + sql);

        sql = "replace into " + tableName
                + "(a,b,c) select 3+a-3,b+1-1,c+2-2 from " + sourceTableName
                + " where a<1000 and b<>( 1+3333+21-1*2)";
        executeDml("trace " + hintStr + sql);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        Assert.assertThat(trace.toString(), trace.size(), is(2 * basePhyInsert + topology.size() + 0 + 1 + 1));

        int scanForDuplicateCheck = 1;
        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                    + "(a,b,c) select a+1*2-2,1+b-1,c+2-2 from " + sourceTableName + " where a=( 1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 4 : 2); //4:(delete + insert) * 2
            Assert.assertThat(trace.toString(), trace.size(),
                    is(tbPartitions + scanForDuplicateCheck + basePhyInsert));

            sql = "replace into " + tableName
                    + "(a,b,c) select a+1*2-2,1+b-1,c+2-2 from " + sourceTableName + " where a=( 0)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 3 : 1); //4:(delete + insert) * 2
            Assert.assertThat(trace.toString(), trace.size(),
                    is(tbPartitions + scanForDuplicateCheck + basePhyInsert));
            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "replace into " + tableName
                + "(a,b,c) select a+1-1,b,c+2 from " + sourceTableName + " where a=(select 0) and c<>( 34567)";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = 3; //3:(delete + insert)  + delete
        Assert.assertThat(trace.toString(), trace.size(),
                is(topology.size() + scanForDuplicateCheck + basePhyInsert));
    }

    @Test
    public void brdTablePkNoUkForReplace() {
        final String tableName = "test_brc_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "replace into " + tableName
                + "(a,b) values(1,1), (2,2+1-1), (3,3), (4,4), (5,5),(0,0)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";

        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 3; //minus metaDb and scaleout target db
        Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 0 + 1 + 1));

        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                    + "(a,b) values(1,1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 2 * physicalDbCount + (0 + 1) + (1 + 1) + (1 + 1) :
                    physicalDbCount + 0 + 1 + 1); //(delete + insert) * 2
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));

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
        Assert.assertThat(trace.toString(), trace.size(),
                is(topology.size() + basePhyInsert + (0 + 1) + (1 + 1) + (1 + 1)));

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
                + "(a,b) values(1,1), (2,2), (3,3), (4,4), (5,5),(0,0)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";

        executeDml(hintStr + sql);

        sql = "replace into " + tableName
                + "(a,b) select a+1-1,b+1-1 from " + sourceTableName + " where 1=1 and a<>1+23456";
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 3; //minus metaDb and scaleout target db
        Assert.assertThat(trace.toString(), trace.size(),
                is(topology.size()/*select from source*/ + topology.size()/*select from target*/ + physicalDbCount + 0 + 1
                        + 1));

        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                    + "(a,b) select a,b from " + sourceTableName + " where 2=2 and a=0";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert =
                    (i == 0 ? 2 * physicalDbCount + (0 + 1) + (1 + 1) + (1 + 1) :
                            physicalDbCount + 0 + 1 + 1); //delete + insert* 2
            Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));

            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "replace into " + tableName
                + "(a,b) select a,b from " + sourceTableName + " where 2=2 and a=1";
        executeDml("trace " + hintStr + sql);

        sql = "replace into " + tableName
                + "(a,b) select a,b+1 from " + sourceTableName + " where a=1 and 2=2";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = physicalDbCount * 2; //(delete + insert) * 2
        Assert.assertThat(trace.toString(), trace.size(),
                is(2 * topology.size() + basePhyInsert + (0 + 1) + (1 + 1) + (1 + 1)));

    }

    @Test
    public void brdTablePkAndUkForReplace() {
        final String tableName = "test_brc_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "replace into " + tableName
                + "(a,b,c) values(1,1,1+1-1), (2,2,2), (3,3+1-1,3), (4,4,4), (5,5,5),(0,0,0)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";

        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 3; //minus metaDb and scaleout target db
        Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 0 + 1 + 1));

        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                    + "(a,b,c) values(1+1-1,1,1)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 2 * physicalDbCount + (1 + 0) + (1 + 1) + (1 + 1) :
                    physicalDbCount + 0 + 1 + 1); //(delete + insert) * 2
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));

            sql = "replace into " + tableName
                    + "(a,b,c) values(0,0,0)";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 2 * physicalDbCount + (1 + 0) + (1 + 1) + (1 + 1) :
                    physicalDbCount + 0 + 1 + 1); //delete*2 + insert
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));

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
        Assert.assertThat(trace.toString(), trace.size(),
                is(topology.size() + basePhyInsert + (1 + 0) + (1 + 1) + (1 + 1)));

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
                + "(a,b,c) values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(0,0,0)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";

        executeDml("trace " + hintStr + sql);

        sql = "replace into " + tableName
                + "(a,b,c) select a+1-1,b+1-1,c+2-2 from " + sourceTableName + " where a<>1 or 1=1";
        executeDml("trace " + hintStr + sql);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 3; //minus metaDb and scaleout target db
        Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 2 * topology.size() + 0 + 1 + 1));

        for (int i = 0; i < 2; i++) {
            sql = "replace into " + tableName
                    + "(a,b,c) select a,b,c from " + sourceTableName + " where a=1 and 2+1=3";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 2 * physicalDbCount + (0 + 1) + (1 + 1) + (1 + 1) :
                    physicalDbCount + 0 + 1 + 1); //(delete + insert) * 2
            Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));

            sql = "replace into " + tableName
                    + "(a,b,c) select a,b,c from " + sourceTableName + " where a=0 and 2+1=3";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 2 * physicalDbCount + (0 + 1) + (1 + 1) + (1 + 1) :
                    physicalDbCount + 0 + 1 + 1); //delete*2 + insert
            Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));

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
        Assert.assertThat(trace.toString(), trace.size(),
                is(2 * topology.size() + basePhyInsert + (0 + 1) + (1 + 1) + (1 + 1)));

    }

    @Test
    public void tablePkNoUkForDelete() {
        final String tableName = "test_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
        executeDml(sql);

        String hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,PLAN_CACHE=false)*/ ";

        String[] sqlList = new String[]{
                "delete from " + tableName + " where 1=1",
                "delete from " + tableName + " where a < 300",
                "delete from " + tableName + " where a = 1",
                "delete from " + tableName + " where b=1",
                "delete from " + tableName + " where a = 1 and b=1",
                "delete from " + tableName + " where 1=1 order by a limit 6",};
        //{6, 2, 1, 1, 1, 1, 1}
        List<List<String>> trace;
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        int dbCount = getDataSourceCount() - 1 - 1 - 3;//minus metadb+scaleoutdb + singledb
        int[] scans = new int[]{dbCount, dbCount, 1, dbCount, 1, dbCount};
        int[] writablePhysicalOperations =
                new int[]{6 + 1 + 1 + 1, 6 + 1 + 1 + 1, 1 + 0 + 1 + 0, 1 + 0 + 1 + 0, 1 + 0 + 1 + 0, 6 + 1 + 1 + 1};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                    + "(a,b) values(1,1), (2,2), (3,3), (4,4), (5,5),(0,0)";
            executeDml(sql);
            sql = sqlList[i];
            int physicalOperation = writablePhysicalOperations[i];
            int scan = scans[i];
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);

            Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation));

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

        String[] sqlList = new String[]{
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
        int dbCount = getDataSourceCount() - 1 - 1 - 3;//minus metadb+scaleoutdb + singledb
        int[] scans = new int[]{dbCount, dbCount, 1, dbCount, dbCount, 1, 1, dbCount};
        int[] writablePhysicalOperations =
                new int[]{
                        6 + 1 + 1 + 1, 6 + 1 + 1 + 1, 1 + 0 + 1 + 0, 1 + 0 + 1 + 0, 1 + 0 + 1 + 0, 1 + 0 + 1 + 0, 1 + 0 + 1 + 0,
                        6 + 1 + 1 + 1};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                    + "(a,b,c) values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(0,0,0)";
            executeDml(sql);
            sql = sqlList[i];
            int physicalOperation = writablePhysicalOperations[i];
            int scan = scans[i];
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);
            Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation));

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

        String[] sqlList = new String[]{
                "delete from " + tableName + " where 1=1",
                "delete from " + tableName + " where a < 300",
                "delete from " + tableName + " where a = 2",
                "delete from " + tableName + " where b=1",
                "delete from " + tableName + " where a = 1 and b=1",
                "delete from " + tableName + " where 1=1 order by a limit 6",};
        //{6, 2, 1, 1, 1, 1, 1}
        List<List<String>> trace;
        int brdDbCount = getDataSourceCount() - 1 - 3;//minus metadb+scaleoutdb
        int[] scans = new int[]{1, 1, 1, 1, 1, 1, 1};
        int[] physicalOperations =
                new int[]{
                        brdDbCount + 1 + 1 + 1, brdDbCount + 1 + 1 + 1, brdDbCount + 1 + 1 + 1, brdDbCount + 1 + 1 + 1,
                        brdDbCount + 1 + 1 + 1, brdDbCount + 1 + 1 + 1, brdDbCount + 1 + 1 + 1};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                    + "(a,b) values(1,1), (2,2), (3,3), (4,4), (5,5),(6,6)";
            executeDml(sql);
            sql = sqlList[i];
            int physicalOperation = physicalOperations[i];
            int scan = scans[i];
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);

            Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation));

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

        String[] sqlList = new String[]{
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
        int brdDbCount = getDataSourceCount() - 1 - 3;//minus metadb+scaleoutdb
        int[] scans = new int[]{1, 1, 1, 1, 1, 1, 1, 1};
        int[] physicalOperations =
                new int[]{
                        brdDbCount + 1 + 1 + 1, brdDbCount + 1 + 1 + 1, brdDbCount + 1 + 1 + 1, brdDbCount + 1 + 1 + 1,
                        brdDbCount + 1 + 1 + 1, brdDbCount + 1 + 1 + 1, brdDbCount + 1 + 1 + 1, brdDbCount + 1 + 1 + 1};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                    + "(a,b,c) values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(6,6,6)";
            executeDml(sql);
            sql = sqlList[i];
            int physicalOperation = physicalOperations[i];
            int scan = scans[i];
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);
            Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation));

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

        String[] sqlList = new String[]{
                "update " + tableName + " set k=k+1",
                "update " + tableName + " set k=k+1 where a=1",
                "update " + tableName + " set k=k+1 where a=1 and b=1",
                "update " + tableName + " set k=k+1 where b=1",
                "update " + tableName + " set a=a+1 where a<100",//5
                "update " + tableName + " set b=b-1 where a<100",
                "update " + tableName + " set b=b-1,a=a+1 where b<100",
                "update " + tableName + " set a=a-1 where a=1",
                "update " + tableName + " set b=b-1 where a=1",
                "update " + tableName + " set b=b-1,a=a+1 where a=1",//10
                "update " + tableName + " set a=a+1 where a=1 and b=1",
                "update " + tableName + " set b=b-1 where a=1 and b=1",
                "update " + tableName + " set b=b-1,a=a+1 where a=1 and b=1",
                "update " + tableName + " set a=a+1 where b=1",
                "update " + tableName + " set b=b-1 where b=1",//15
                "update " + tableName + " set b=b-1,a=a+1 where b=1",
                "update " + tableName + " set k=k+1 order by a limit 6",
                "update " + tableName + " set a=a+1 where a<>3 order by a limit 6",
                "update " + tableName + " set k=k-1 where a=1 and b=1 order by a limit 6",};
        List<List<String>> trace;
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        int dbCount = getDataSourceCount() - 1 - 1 - 3;//minus metadb+scaleoutdb + singledb
        int[] scans =
                new int[]{
                        dbCount, 1, 1, dbCount, dbCount,
                        dbCount, dbCount, 1, 1, 1,
                        1, 1, 1, dbCount, dbCount,
                        dbCount, dbCount, dbCount, 1};
        //0 delete 0 + insert 1
        //1 delete 1 + insert 2
        //2 delete 2 + insert 3
        int[] writablePhysicalOperations =
                new int[]{
                        6 + 1 + 1 + 1, 1 + 1, 1 + 1, 1 + 1, 6 * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        6 * 2 + (1 + 0) + (1 + 1) + (1 + 1), 6 * 2 + (1 + 0) + (1 + 1) + (1 + 1), 1 * 2 + 1, 1 * 2 + 2,
                        1 * 2 + 1 * 2,
                        1 * 2 + 1 * 2, 1 * 2 + 2, 1 * 2 + 1 * 2, 1 * 2 + 1 * 2, 1 * 2 + 2,
                        1 * 2 + 1 * 2, 6 + 1 + 1 + 1, 5 * 2 + (1 + 0) + (1 + 1) + (1 + 1), 1 + 1};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                    + "(a,b,k) values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(0,0,0)";
            executeDml(sql);
            sql = sqlList[i];
            int physicalOperation = writablePhysicalOperations[i];
            int scan = scans[i];
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);
            Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation));

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

        String[] sqlList = new String[]{
                "update " + tableName + " set k=k+1",
                "update " + tableName + " set k=k+1 where a=1",
                "update " + tableName + " set k=k+1 where a=1 and b=1",
                "update " + tableName + " set k=k+1 where b=1",
                "update " + tableName + " set a=a+1 where a<100",//5
                "update " + tableName + " set b=b-1 where a<100",
                "update " + tableName + " set b=b-1,a=a+1 where b<100",
                "update " + tableName + " set a=a+1 where a=1",
                "update " + tableName + " set b=b-1 where a=1",
                "update " + tableName + " set b=b-1,a=a+1 where a=1",//10
                "update " + tableName + " set a=a+1 where a=1 and b=1",
                "update " + tableName + " set b=b-1 where a=1 and b=1",
                "update " + tableName + " set b=b-1,a=a+1 where a=1 and b=1",
                "update " + tableName + " set a=a+1 where b=1",
                "update " + tableName + " set b=b-1 where b=1",//15
                "update " + tableName + " set b=b-1,a=a+1 where b=1",
                "update " + tableName + " set k=k+1 order by a limit 6",
                "update " + tableName + " set a=a+1 where a<>3 order by a limit 6",
                "update " + tableName + " set k=k-1 where a=1 and b=1 order by a limit 6",
                "update " + tableName + " set c=c-1 where b=1",
                "update " + tableName + " set c=c-1 where a=1",
        };
        //{6, 2, 1, 1, 1, 1, 1}
        List<List<String>> trace;
        int dbCount = getDataSourceCount() - 1 - 1 - 3;//minus metadb+scaleoutdb + singledb
        int[] scans =
                new int[]{
                        dbCount, 1, 1, dbCount, dbCount,
                        dbCount, dbCount, 1, 1, 1,
                        1, 1, 1, dbCount, dbCount,
                        dbCount, dbCount, dbCount, 1, dbCount,
                        1};
        int[] writablePhysicalOperations =
                new int[]{
                        6 + 1 + 1 + 1, 1 + 1, 1 + 1, 1 + 1, 6 * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        6 * 2 + (1 + 0) + (1 + 1) + (1 + 1), 6 * 2 + (1 + 0) + (1 + 1) + (1 + 1), 1 * 2 + 2, 1 * 2 + 2,
                        1 * 2 + 2,
                        1 * 2 + 2, 1 * 2 + 2, 1 * 2 + 2, 1 * 2 + 2, 1 * 2 + 2,
                        1 * 2 + 2, 6 + 1 + 1 + 1, 5 * 2 + (1 + 0) + (1 + 1) + (1 + 1), 1 + 1, 1 + 1,
                        1 + 1};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                    + "(a,b,c,k) values(1,1,1,1), (2,2,2,2), (3,3,3,3), (4,4,4,4), (5,5,5,5),(0,0,0,0)";
            executeDml(sql);
            sql = sqlList[i];
            int physicalOperation = writablePhysicalOperations[i];
            int scan = scans[i];
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);

            Assert.assertThat(trace.toString(), trace.size(), is(scan + physicalOperation));

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

        String[] sqlList = new String[]{
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
        int brdDbCount = getDataSourceCount() - 1 - 3;//minus metadb+scaleoutdb
        int scan = 1;
        int[] writablePhysicalOperations =
                new int[]{
                        scan + brdDbCount + 1 + 1 + 1, scan + brdDbCount + 1 + 1 + 1, scan + brdDbCount + 1 + 1 + 1,
                        scan + brdDbCount + 1 + 1 + 1,
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount + 1 + 1 + 1, scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount + 1 + 1 + 1, scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1), scan + brdDbCount + 1 + 1 + 1,
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1), scan + brdDbCount + 1 + 1 + 1,
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1), scan + brdDbCount + 1 + 1 + 1,
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1), scan + brdDbCount + 1 + 1 + 1};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                    + "(a,b,k) values(1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5),(6,6,6)";
            executeDml(sql);
            sql = sqlList[i];
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);
            Assert.assertThat(trace.toString(), trace.size(), is(writablePhysicalOperations[i]));

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

        String[] sqlList = new String[]{
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
        int brdDbCount = getDataSourceCount() - 1 - 3;//minus metadb+scaleoutdb
        int scan = 1;
        int[] writablePhysicalOperations =
                new int[]{
                        scan + brdDbCount + 3, scan + brdDbCount + 3, scan + brdDbCount + 3, scan + brdDbCount + 3,
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1),
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1), scan + brdDbCount + 3,
                        scan + brdDbCount * 2 + (1 + 0) + (1 + 1) + (1 + 1), scan + brdDbCount + 3,
                        scan + brdDbCount + 3,
                        scan + brdDbCount + 3};

        for (int i = 0; i < sqlList.length; i++) {
            sql = "insert into " + tableName
                    + "(a,b,c,k) values(1,1,1,1), (2,2,2,2), (3,3,3,3), (4,4,4,4), (5,5,5,5),(6,6,6,6)";
            executeDml(sql);
            sql = sqlList[i];
            executeDml("trace " + hint + sql);
            trace = getTrace(tddlConnection);
            System.out.println("i = " + String.valueOf(i));
            System.out.println("sql = " + sql);

            Assert.assertThat(trace.toString(), trace.size(), is(writablePhysicalOperations[i]));

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
                + "(a,b) values(1,1), (2,2), (3,3), (4-2+2,4), (5,5),(9+0-9,0) on duplicate key update b=b+1+3";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";

        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int shrdDbCount = getDataSourceCount() - 1 - 1 - 3;//minus metadb+scaleoutdb+singledb
        int basePhyInsert = 6;

        Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert * 2 + 0 + 1 + 1));

        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                    + "(a,b) values(1,1+1-1) on duplicate key update b=b+1+3";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i == 0 ? 4 : 2;
            Assert.assertThat(trace.toString(), trace.size(), is(1 + basePhyInsert));

            sql = "insert into " + tableName
                    + "(a,b) values(0,0+1-1) on duplicate key update b=b+1+3";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i == 0 ? 3 : 1;
            Assert.assertThat(trace.toString(), trace.size(), is(1 + basePhyInsert));

            sql = "insert into " + tableName
                    + "(a,b) values(2,2+1-1) on duplicate key update b=b+1+3";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i == 0 ? 4 : 2;
            Assert.assertThat(trace.toString(), trace.size(), is(1 + basePhyInsert));

            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "insert into " + tableName
                + "(a,b) values(1+1-1,2) on duplicate key update a=a+" + String.valueOf(shrdDbCount) + "*20, b=b+2";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = 2; //2:(insert) * 2
        Assert.assertThat(trace.toString(), trace.size(), is(1 + basePhyInsert));

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
                + "(a,b) values(1,1), (2,2), (3,3), (4,4), (5,5),(0,0)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false, MERGE_UNION=false, DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN=false)*/ ";

        executeDml(hintStr + sql);

        sql = "insert into " + tableName
                + "(a,b) select a+1-1,b+1-1 from " + sourceTableName + " on duplicate key update b=b+1+3";
        executeDml("trace " + hintStr + sql);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int shrdDbCount = getDataSourceCount() - 1 - 1 - 3;//minus metadb+scaleoutdb+singledb
        int basePhyInsert = 6;
        Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert * 2 + topology.size() + 0 + 1 + 1));

        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                    + "(a,b) select a+1-1,b from " + sourceTableName + " where a=1+1-1 on duplicate key update b=b+1+3";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i == 0 ? 4 : 2;
            Assert.assertThat(trace.toString(), trace.size(), is(1 + topology.size() + basePhyInsert));

            sql = "insert into " + tableName
                    + "(a,b) select a+1-1,b from " + sourceTableName + " where a=0+1-1 on duplicate key update b=b+1+3";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i == 0 ? 3 : 1;
            Assert.assertThat(trace.toString(), trace.size(), is(1 + topology.size() + basePhyInsert));

            sql = "insert into " + tableName
                    + "(a,b) select a+1-1,b from " + sourceTableName + " where a=2+1-1 on duplicate key update b=b+1+3";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i == 0 ? 4 : 2;
            Assert.assertThat(trace.toString(), trace.size(), is(1 + topology.size() + basePhyInsert));

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

        basePhyInsert = 2; //2:(insert) * 2
        Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + 1 + basePhyInsert));

    }

    @Test
    public void tablePkAndUkForUpsert() {
        final String tableName = "test_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);

        sql = "insert into " + tableName
                + "(a,b,c) values(1,1,1), (2,2+1-1,2), (3,3,3), (4,4,4), (5,5+2-2,5),(0,0,0) on duplicate key update a=a+1";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";

        executeDml("trace " + hintStr + sql);

        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int scan = 6;
        Assert.assertThat(trace.toString(), trace.size(), is(basePhyInsert + scan + 0 + 1 + 1));

        int scanForDuplicateCheck = 1;
        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                    + "(a,b,c) values(1,1+1-1,1) on duplicate key update b=b+1";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 4 : 2); //4:(delete + insert) * 2
            Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));

            sql = "insert into " + tableName
                    + "(a,b,c) values(0,0+1-1,1) on duplicate key update b=b+1";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 3 : 1);
            Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));

            sql = "insert into " + tableName
                    + "(a,b,c) values(2,2+1-1,1) on duplicate key update b=b+1";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 4 : 2); //4:(delete + insert) * 2
            Assert.assertThat(trace.toString(), trace.size(), is(scanForDuplicateCheck + basePhyInsert));

            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
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
                + "(a,b,c) values(1,1,1), (2,2,2+1-1), (3,3,3), (4,4,4), (5,5,5),(0,0,0)";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false, MERGE_UNION=false)*/ ";

        executeDml(hintStr + sql);

        sql = "insert into " + tableName
                + "(a,b,c) select a+1-1,b,c from " + sourceTableName + " where a<>2+12324 on duplicate key update a=a+1";
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        Assert.assertThat(trace.toString(), trace.size(), is(2 * basePhyInsert + topology.size() + 0 + 1 + 1));

        // 1 is the second time to find the duplicate record
        int scanForDuplicateCheck = 1;
        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                    + "(a,b,c) select a,b+2-2,c from " + sourceTableName
                    + " where a=1 and 2=2 on duplicate key update b=b+2";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 4 : 2); //4:(delete + insert) * 2
            Assert.assertThat(trace.toString(), trace.size(),
                    is(tbPartitions + scanForDuplicateCheck + basePhyInsert));

            sql = "insert into " + tableName
                    + "(a,b,c) select a,b+2-2,c from " + sourceTableName
                    + " where a=0 and 2=2 on duplicate key update b=b+2";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 3 : 1); //4:(delete + insert) * 2
            Assert.assertThat(trace.toString(), trace.size(),
                    is(tbPartitions + scanForDuplicateCheck + basePhyInsert));

            sql = "insert into " + tableName
                    + "(a,b,c) select a,b+2-2,c from " + sourceTableName
                    + " where a=2 and 2=2 on duplicate key update b=b+2";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = (i == 0 ? 4 : 2); //4:(delete + insert) * 2
            Assert.assertThat(trace.toString(), trace.size(),
                    is(tbPartitions + scanForDuplicateCheck + basePhyInsert));

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
        Assert.assertThat(trace.toString(), trace.size(), is(tbPartitions + scanForDuplicateCheck + basePhyInsert));

    }

    @Test
    public void brdTablePkNoUkForUpsert() {
        final String tableName = "test_brc_tb_with_pk_no_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "insert into " + tableName
                + "(a,b) values(1+1-1,1), (2,2+1-1), (3,3), (4,4), (5,5),(0,0) on duplicate key update b=b+20";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";

        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 3; //minus metaDb and scaleout target db
        Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 0 + 1 + 1));

        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                    + "(a,b) values(1+2-2,1) on duplicate key update b=b+20";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i == 0 ? physicalDbCount + 3 : physicalDbCount + 0 + 1 + 1;
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));

            sql = "insert into " + tableName
                    + "(a,b) values(0+2-2,0) on duplicate key update b=b+20";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i == 0 ? physicalDbCount + 3 : physicalDbCount + 0 + 1 + 1;
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));

            sql = "insert into " + tableName
                    + "(a,b) values(2+2-2,1) on duplicate key update b=b+20";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i == 0 ? physicalDbCount + 3 : physicalDbCount + 0 + 1 + 1;
            Assert.assertThat(trace.toString(), trace.size(), is(topology.size() + basePhyInsert));

            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "insert into " + tableName
                + "(a,b) values(1,2+1-1) on duplicate key update a=a+20";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        Assert.assertThat(trace.toString(), trace.size(),
                is(topology.size() + physicalDbCount * 2 + (0 + 1) + (1 + 1) + (1 + 1)));

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

        executeDml(hintStr + sql);

        sql = "insert into " + tableName
                + "(a,b) select a,b+1-1 from " + sourceTableName
                + " where 1=1 and a<>1+23456 on duplicate key update b=b+20";
        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 3; //minus metaDb and scaleout target db
        Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 2 * topology.size() + 0 + 1 + 1));

        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                    + "(a,b) select a+1-1,b from " + sourceTableName
                    + " where 2=2 and a=1+1-1 on duplicate key update b=b+20";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            basePhyInsert = i == 0 ? physicalDbCount + 3 : physicalDbCount + 2;
            Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));

            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
        sql = "insert into " + tableName
                + "(a,b) select a,b+1 from " + sourceTableName + " where a=1 and 2=2 on duplicate key update b=b+20";
        executeDml("trace " + hintStr + sql);
        trace = getTrace(tddlConnection);

        basePhyInsert = physicalDbCount + 1 + 1 + 1;
        Assert.assertThat(trace.toString(), trace.size(), is(2 * topology.size() + basePhyInsert));

    }

    @Test
    public void brdTablePkAndUkForUpsert() {
        final String tableName = "test_brc_tb_with_pk_uk";
        String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ delete from " + tableName + " where 1=1";
        executeDml(sql);
        sql = "insert into " + tableName
                + "(a,b,c) values(1,1+1-1,1), (2+1-1,2,2), (3,3,3), (4,4,4), (5,5,5),(6,6,6) on duplicate key update b=b+20";
        String hintStr = " /*+TDDL:cmd_extra(PLAN_CACHE=false)*/ ";

        executeDml("trace " + hintStr + sql);
        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int physicalDbCount = getDataSourceCount() - 1 - 3; //minus metaDb and scaleout target db
        Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + topology.size() + 0 + 1 + 1));

        int scan = topology.size();
        int[] writablePhysicalOperations =
                new int[]{scan + physicalDbCount * 2 + (0 + 1) + (1 + 1) + (1 + 1), scan + physicalDbCount + 0 + 1 + 1};

        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                    + "(a,b,c) values(1,1+1-1,1) on duplicate key update b=b+20";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);

            Assert.assertThat(trace.toString(), trace.size(), is(writablePhysicalOperations[i]));

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
        Assert.assertThat(trace.toString(), trace.size(), is(writablePhysicalOperations[i]));

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

        executeDml("trace " + hintStr + sql);

        sql = "insert into " + tableName
                + "(a,b,c) select a,b,c+1-1 from " + sourceTableName + " where a<>1 or 1=1 on duplicate key update b=b+20";
        executeDml("trace " + hintStr + sql);

        final List<Pair<String, String>> topology = JdbcUtil.getTopology(tddlConnection, tableName);
        List<List<String>> trace = getTrace(tddlConnection);

        int basePhyInsert = 6;
        int physicalDbCount = getDataSourceCount() - 1 - 3; //minus metaDb and scaleout target db
        Assert.assertThat(trace.toString(), trace.size(), is(physicalDbCount + 2 * topology.size() + 0 + 1 + 1));

        int scan = topology.size();
        int[] writablePhysicalOperations =
                new int[]{
                        scan + 1 + physicalDbCount * 2 + (0 + 1) + (1 + 1) + (1 + 1), scan + 1 + physicalDbCount + 0 + 1 + 1};

        for (int i = 0; i < 2; i++) {
            sql = "insert into " + tableName
                    + "(a,b,c) select a+1-1,b,c from " + sourceTableName
                    + " where a=1 and 2+1=3 on duplicate key update b=b+20";
            executeDml("trace " + hintStr + sql);
            trace = getTrace(tddlConnection);
            Assert.assertThat(trace.toString(), trace.size(), is(writablePhysicalOperations[i]));

            if (i == 0) {
                sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/ delete from " + tableName + " where 1=1";
                executeDml(sql);
            }
        }
    }
}