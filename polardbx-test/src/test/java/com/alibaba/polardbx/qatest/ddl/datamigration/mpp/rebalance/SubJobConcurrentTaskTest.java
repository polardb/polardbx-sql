package com.alibaba.polardbx.qatest.ddl.datamigration.mpp.rebalance;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestBase;
import com.alibaba.polardbx.qatest.ddl.datamigration.mpp.pkrange.PkTest;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

@NotThreadSafe
@RunWith(Parameterized.class)
public class SubJobConcurrentTaskTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(PkTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public SubJobConcurrentTaskTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}});
    }

    @Before
    public void init() {
        this.tableName = schemaPrefix + randomTableName("empty_table", 4);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testSubJobConcurrentTaskSimple() throws SQLException, InterruptedException {
        String schemaName = "sub_job_concurrent_test";
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database  " + schemaName + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t1(a int, b int) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t2(a bigint, b int) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t3(a varchar(32), b int) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t4(a varchar(16), b int) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s1(a varchar(16), b int) single");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s2(a varchar(16), b int) single");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table b1(a varchar(16), b int) broadcast");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table b2(a varchar(16), b int) broadcast");
        int waitTime = 4000;// 100s
        int minConcurrent = 2;
        int maxConcurrent = 3;
        String rebalanceDdl =
            "/*+TDDL:cmd_extra(REBALANCE_MAX_UNIT_PARTITION_COUNT=2,REBALANCE_TASK_PARALISM=3)*/rebalance database shuffle_data_dist=1;";
        concurrentTaskSubJobTest(tddlConnection, schemaName, minConcurrent, maxConcurrent, rebalanceDdl, waitTime);
    }

    @Test
    public void testSubJobConcurrentTaskWithGsi1() throws SQLException, InterruptedException {
        String schemaName = "sub_job_concurrent_test";

        // tablegroup1 t1, g_i_b1
        // tablegroup2 t2, g_i_b4
        // tablegroup3 t3
        // tablegroup4 t4, g_i_b2
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database  " + schemaName + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t1(a int, b int) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t2(a bigint, b varchar(16)) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t3(a varchar(32), b int) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t4(a varchar(16), b bigint) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table t1 add global index g_i_b1(b) partition by hash(b) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table t2 add global index g_i_b2(b) partition by hash(b) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table t4 add global index g_i_b4(b) partition by hash(b) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s1(a varchar(16), b int) single");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s2(a varchar(16), b int) single");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table b1(a varchar(16), b int) broadcast");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table b2(a varchar(16), b int) broadcast");
        int waitTime = 4000;// 100s
        int minConcurrent = 3;
        int maxConcurrent = 3;
        String rebalanceDdl =
            String.format(
                "/*+TDDL:cmd_extra(REBALANCE_MAX_UNIT_PARTITION_COUNT=2,REBALANCE_TASK_PARALISM=%d)*/rebalance database shuffle_data_dist=1;",
                maxConcurrent);
        concurrentTaskSubJobTest(tddlConnection, schemaName, minConcurrent, maxConcurrent, rebalanceDdl, waitTime);
    }

    @Test
    public void testSubJobConcurrentTaskWithGsiCaseTest1() throws SQLException, InterruptedException {
        String schemaName = "sub_job_concurrent_test";

        // tablegroup1 t1, g_i_b1
        // tablegroup2 t2, g_i_b4
        // tablegroup3 t3
        // tablegroup4 t4, g_i_b2
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database  " + schemaName + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t1(a int, b int) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table CaseMixedT1(a int, b int) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t2(a bigint, b varchar(16)) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table CaseMixedT2(a bigint, b varchar(16)) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t3(a varchar(32), b int) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create tablegroup tgMixedCase11;");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table t3 set tablegroup = tgMixedCase11;");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t4(a varchar(16), b bigint) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table t1 add global index g_I_B1(b) partition by hash(b) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table t2 add global index g_i_b2(b) partition by hash(b) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table t4 add global index g_I_B4(b) partition by hash(b) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s1(a varchar(16), b int) single");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s2(a varchar(16), b int) single");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table b1(a varchar(16), b int) broadcast");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table b2(a varchar(16), b int) broadcast");
        int waitTime = 4000;// 100s
        int minConcurrent = 3;
        int maxConcurrent = 3;
        String rebalanceDdl =
            String.format(
                "/*+TDDL:cmd_extra(REBALANCE_MAX_UNIT_PARTITION_COUNT=2,REBALANCE_TASK_PARALISM=%d)*/rebalance database shuffle_data_dist=1;",
                maxConcurrent);
        concurrentTaskSubJobTest(tddlConnection, schemaName, minConcurrent, maxConcurrent, rebalanceDdl, waitTime);
    }

    @Test
    public void testSubJobConcurrentTaskWithGsi2() throws SQLException, InterruptedException {
        String schemaName = "sub_job_concurrent_test";

        // tablegroup1 t1, g_i_b1
        // tablegroup2 t2, g_i_b2, g_i_c1
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database  " + schemaName + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t1(a int, b int, c varchar(16)) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t2(a varchar(16), b varchar(16)) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table t1 add global index g_i_b1(b) partition by hash(b) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table t1 add global index g_i_c1(c) partition by hash(c) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table t2 add global index g_i_b2(b) partition by hash(b) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s1(a varchar(16), b int) single");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s2(a varchar(16), b int) single");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s_b1(a varchar(16), b int) single locality = 'balance_single_table=on'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s_b2(a varchar(16), b int) single locality = 'balance_single_table=on'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s_b3(a varchar(16), b int) single locality = 'balance_single_table=on'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s_b4(a varchar(16), b int) single locality = 'balance_single_table=on'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table b1(a varchar(16), b int) broadcast");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table b2(a varchar(16), b int) broadcast");
        int waitTime = 4000;// 100s
        int minConcurrent = 1;
        int maxConcurrent = 1;
        String rebalanceDdl =
            String.format(
                "/*+TDDL:cmd_extra(REBALANCE_MAX_UNIT_PARTITION_COUNT=2,REBALANCE_TASK_PARALISM=%d)*/rebalance database shuffle_data_dist=1;",
                maxConcurrent);
        concurrentTaskSubJobTest(tddlConnection, schemaName, minConcurrent, maxConcurrent, rebalanceDdl, waitTime);
    }

    @Test
    public void testSubJobConcurrentTaskDrainNodeWithGsi1() throws SQLException, InterruptedException {
        String schemaName = "sub_job_concurrent_test";

        // tablegroup1 t1, g_i_b1
        // tablegroup2 t2, g_i_b4
        // tablegroup3 t3
        // tablegroup4 t4, g_i_b2
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database  " + schemaName + " mode = auto");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t1(a int, b int) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t2(a bigint, b varchar(16)) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t3(a varchar(32), b int) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table t4(a varchar(16), b bigint) partition by hash(a) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table t1 add global index g_i_b1(b) partition by hash(b) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table t2 add global index g_i_b2(b) partition by hash(b) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table t4 add global index g_i_b4(b) partition by hash(b) partitions 16 ");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s1(a varchar(16), b int) single");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s2(a varchar(16), b int) single");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s_b1(a varchar(16), b int) single locality = 'balance_single_table=on'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s_b2(a varchar(16), b int) single locality = 'balance_single_table=on'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s_b3(a varchar(16), b int) single locality = 'balance_single_table=on'");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table s_b4(a varchar(16), b int) single locality = 'balance_single_table=on'");
//        JdbcUtil.executeUpdateSuccess(tddlConnection,
//            "create table b1(a varchar(16), b int) broadcast");
//        JdbcUtil.executeUpdateSuccess(tddlConnection,
//            "create table b2(a varchar(16), b int) broadcast");
        int waitTime = 4000;// 100s
        int minConcurrent = 3;
        int maxConcurrent = 3;
        String undeletableDn = LocalityTestBase.chooseDatanode(tddlConnection, true);
        List<String> dns = LocalityTestBase.getDatanodes(tddlConnection);
        dns.remove(undeletableDn);
        String dn = dns.get(0);
        String rebalanceDdl = String.format(
            "/*+TDDL:cmd_extra(REBALANCE_MAX_UNIT_PARTITION_COUNT=2,REBALANCE_TASK_PARALISM=%d)*/rebalance database drain_node = '%s'",
            maxConcurrent, dn);
        concurrentTaskSubJobTest(tddlConnection, schemaName, minConcurrent, maxConcurrent, rebalanceDdl, waitTime);
    }

    public static void concurrentTaskSubJobTest(Connection tddlConnection, String schemaName, int minExpectedConcurrent,
                                                int maxExpectedConcurrent, String rebalanceDdl, int waitTime)
        throws SQLException, InterruptedException {
        Map<String, List<String>>
            primaryTableToGsiMap = DdlStateCheckUtil.getPrimaryTableToGsiMap(tddlConnection, schemaName);
        Map<String, String>
            tableToTableGroupMap = DdlStateCheckUtil.getTableToTableGroupMap(tddlConnection, schemaName);
        logger.info("the primary table to gsi is " + JSON.toJSONString(primaryTableToGsiMap));
        logger.info("the table to table group map is " + JSON.toJSONString(tableToTableGroupMap));

        Map<String, Set<String>> unionSet = new HashMap<>();
        for (String primaryTable : primaryTableToGsiMap.keySet()) {
            List<String> gsis = primaryTableToGsiMap.get(primaryTable);
            String primaryTableGroup = tableToTableGroupMap.get(primaryTable);

            Set<String> gsiTableGroups =
                gsis.stream().map(o -> tableToTableGroupMap.get(o)).collect(Collectors.toSet());
            for (String gsiTableGroup : gsiTableGroups) {
                if (!primaryTableGroup.equals(gsiTableGroup)) {
                    unionSet.computeIfAbsent(gsiTableGroup, k -> new HashSet<>()).add(primaryTableGroup);
                    unionSet.get(gsiTableGroup).addAll(gsiTableGroups);
                    unionSet.get(gsiTableGroup).remove(gsiTableGroup);
                    unionSet.computeIfAbsent(primaryTableGroup, k -> new HashSet<>()).add(gsiTableGroup);
                    unionSet.get(primaryTableGroup).addAll(gsiTableGroups);
                    unionSet.get(primaryTableGroup).remove(primaryTableGroup);
                }
            }

        }
        logger.info(" related tablegroup map: " + JSON.toJSONString(unionSet));
        String regex = "tg\\w*\\d+";
        // 创建Pattern对象
        Pattern pattern = Pattern.compile(regex);
        // 创建Matcher对象
        int maxRunningSubJobCount = 0;
        Boolean jobFinished = false;
        Set<Set<String>> illegalConcurrentTableGroupSet = new HashSet<>();
        Set<Set<String>> legalConcurrentTableGroupSet = new HashSet<>();
        List<Set<String>> legalConcurrentTableGroupList = new ArrayList<>();
        JdbcUtil.executeUpdateSuccess(tddlConnection, rebalanceDdl);
        Long jobId = DdlStateCheckUtil.getDdlJobIdFromPattern(tddlConnection, rebalanceDdl);
        String querySql =
            "select task_name, task_info, task_state, execution_time, node_ip from information_schema.ddl_scheduler where task_name = \"SubJobTask\" and task_state = \"ACTIVE\" and task_info like \"%alter tablegroup%\" and job_id = "
                + jobId;
        for (int i = 0; i < waitTime; i++) {
            ResultSet resultSet = JdbcUtil.executeQuery(querySql,
                tddlConnection);
            Set<String> tgNames = new HashSet<>();
            while (resultSet.next()) {
                String task_info = resultSet.getString("task_info");
                Matcher matcher = pattern.matcher(task_info);
                if (matcher.find()) {
                    if (!tgNames.add(matcher.group(0))) {
                        illegalConcurrentTableGroupSet.add(tgNames);
                        logger.warn(" illegal tablegroup concurrent: " + matcher.group(0));
                    }
                }
            }

            if (!checkIfConcurrentLegally(tgNames, unionSet)) {
                if (illegalConcurrentTableGroupSet.add(tgNames)) {
                    logger.warn(" illegal concurrent tablegroup: " + tgNames);
                }
            } else {
                if (!legalConcurrentTableGroupSet.contains(tgNames)) {
                    legalConcurrentTableGroupSet.add(tgNames);
                    legalConcurrentTableGroupList.add(tgNames);
                    logger.info(" concurrent tablegroup: " + tgNames);
                }
            }
            maxRunningSubJobCount = Math.max(maxRunningSubJobCount, tgNames.size());
            Thread.sleep(50);
            jobFinished = DdlStateCheckUtil.checkIfCompleteFully(tddlConnection, jobId, "randomasyoulike");
            if (jobFinished) {
                break;
            }
        }
        logger.info(" concurrent tablegroup stream: " + JSON.toJSONString(legalConcurrentTableGroupList));
        if (maxRunningSubJobCount < minExpectedConcurrent || maxRunningSubJobCount > maxExpectedConcurrent
            || !illegalConcurrentTableGroupSet.isEmpty()) {
            String errMsg =
                " not ok , no suitable concurrent subjob here, expected " + String.format("[%d, %d], but [%d]",
                    minExpectedConcurrent, maxExpectedConcurrent, maxRunningSubJobCount)
                    + " and the illegal tg set is " + illegalConcurrentTableGroupSet;
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, errMsg);
        }
        if (!jobFinished) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " not ok , rebalance job not finsihed!");
        }
    }

    static boolean checkIfConcurrentLegally(Set<String> tgNames, Map<String, Set<String>> unionSet) {
        TreeSet<String> tgSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        tgSet.addAll(tgNames);
        Boolean ok = true;
        for (int i = 0; i < tgNames.size(); ++i) {
            String tgName = tgSet.pollFirst();
            List<String> illegalTgNames =
                tgSet.stream().filter(o -> unionSet.get(tgName) != null && unionSet.get(tgName).contains(o))
                    .collect(Collectors.toList());
            if (!illegalTgNames.isEmpty()) {
                logger.warn(String.format(" for tg %s, illegal tg name: %s ", tgName, illegalTgNames));
                ok = false;
            }
        }
        return ok;

    }

}