package com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.LogicalTableGsiPkRangeBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.AlterGsiAddLocalIndexTask;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.qatest.ddl.auto.dal.CheckTableTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import org.apache.commons.logging.Log;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DdlStateCheckUtil {

    private final static Logger LOG = LoggerFactory.getLogger(DdlStateCheckUtil.class);

    public static Boolean killPhysicalDdlRandomly(Connection tddlConnection, Long jobId, String mytable, String state) {
        String sql = "show physical ddl";
        Boolean killSuccess = false;
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql));
        List<List<Object>> runningPhyDdl =
            results.stream().filter(o -> o.get(3).toString().equalsIgnoreCase(state)).
                filter(o -> o.get(1).toString().contains(mytable)).collect(Collectors.toList());
        LOG.info("get related physical ddl status: " + runningPhyDdl);
        Random random = new Random();
        if (!runningPhyDdl.isEmpty()) {
            int index = random.nextInt(runningPhyDdl.size());
            String phyDbName = runningPhyDdl.get(index).get(0).toString();
            String phyProcess = runningPhyDdl.get(index).get(4).toString();
            LOG.info("choose " + String.valueOf(index) + " " + phyDbName + ":" + runningPhyDdl.get(index).get(1));
            sql = String.format("show physical processlist where db='%s' and id = %s", phyDbName, phyProcess);
            List<List<Object>> queryPhysicalResults =
                JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql));
            LOG.info("get query result: " + queryPhysicalResults);
            if (!queryPhysicalResults.isEmpty()) {
                sql = String.format("kill \"%s-%s-%s\"", queryPhysicalResults.get(0).get(0),
                    queryPhysicalResults.get(0).get(1), queryPhysicalResults.get(0).get(2));
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
                killSuccess = true;
            }
        }
        return killSuccess;
    }

    public static Map<String, Map<String, Integer>> getPhyDdlStatus(String schemaName, String tableName,
                                                                    Connection tddlConnection, Long jobId) {
        String sql = "show physical ddl status";
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql));
        String fullTableName = String.format("%s.%s", schemaName, tableName);
        Map<String, Map<String, Integer>> phyDdlStatus = new HashMap<>();
        for (List<Object> result : results) {
            if (result.get(0).toString().equalsIgnoreCase(fullTableName)) {
                phyDdlStatus.put(result.get(1).toString(), parseStatusText(result.get(3).toString()));
            }
        }
        return phyDdlStatus;
    }

    public static Map<String, Integer> parseStatusText(String statusText) {
        Map<String, Integer> statusMap = new HashMap<>();
        String[] expectedStatuses = {"WAIT_RUNNING", "RUNNING", "REACHED_BARRIER"};

        // 初始化状态计数为0
        for (String status : expectedStatuses) {
            statusMap.put(status, 0);
        }

        // 匹配状态和数量
        Pattern pattern = Pattern.compile("(\\w+) Count:(\\d+)");
        Matcher matcher = pattern.matcher(statusText);

        while (matcher.find()) {
            String status = matcher.group(1);
            int count = Integer.parseInt(matcher.group(2));
            if (statusMap.containsKey(status)) {
                statusMap.put(status, count);
            }
        }
        return statusMap;
    }

    public static Boolean waitTillDdlDone(Connection tddlConnection, Long jobId, String tableName)
        throws InterruptedException {
        return waitTillDdlDone(tddlConnection, jobId, tableName, -1, false);
    }

    public static Map<Long, Long> collectTimeOfBackfillIdExecuteTime(Connection connection, Long jobId) {
        return new HashMap<>();
    }

    public static Boolean checkIfContinueValid(Connection connection, Long jobId, List<String> errMsg) {
        String fetchExecutionTimeSql =
            String.format("select task_id, backfill_ids, extra from metadb.backfill_sample_rows where job_id = %d",
                jobId);
        List<List<Object>> results =
            JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(connection, fetchExecutionTimeSql));
        List<String> msg = new ArrayList<>();
        Boolean checkOk = true;
        for (List<Object> result : results) {
            Long taskId = Long.valueOf(result.get(0).toString());
            String fetchSuccessRowCountSql =
                String.format("select job_id, success_row_count from metadb.backfill_objects where task_id = %d",
                    taskId);
            List<List<Object>> successRowCount =
                JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(connection, fetchSuccessRowCountSql));
            Map<Long, Long> successRowCountMap = new HashMap<>();
            for (List<Object> row : successRowCount) {
                successRowCountMap.put(Long.valueOf(row.get(0).toString()), Long.valueOf(row.get(1).toString()));
            }
            String backfillIds = (String) result.get(1);
            String executionTime = (String) result.get(2);
//            if (!checkIfContinueValid(taskId, backfillIds, executionTime, successRowCountMap, msg)) {
//                checkOk = false;
//            }
        }
        errMsg.add(StringUtil.join(",", msg).toString());
        return checkOk;
    }

    public static Map<Long, String> parseExecutionTime(String executionTime) {
        String executionTimeMapString = DdlHelper.decompress(executionTime);
        Map<Long, String> executionTimeMap =
            JSON.parseObject(executionTimeMapString, new HashMap<Long, String>().getClass());
        return executionTimeMap;
    }

    public static Long fromExecutionTime(String executionTime) {
        String newExecutionTime = executionTime.replace(":i", "");
        Long result = Long.parseLong(newExecutionTime);
        if (!newExecutionTime.equalsIgnoreCase(executionTime)) {
            result = result * -1;
        }
        return result;
    }

    public static Boolean checkIfContinueValid(Long taskId, String backfillIds, String executionTime,
                                               Map<Long, Long> successRowCountMap,
                                               List<String> errMsg) {
        final int EXCEPTION_COUNT = 10;
        String[] executionTimeSeqs = executionTime.split("\n");
        Boolean result = true;
        if (executionTimeSeqs.length >= 2) {
            List<Long> backfillIdList =
                Arrays.stream(backfillIds.split(",")).map(o -> Long.valueOf(o)).collect(Collectors.toList());
            String lastExecutionTime = executionTimeSeqs[executionTimeSeqs.length - 2];
            String thisExecutionTime = executionTimeSeqs[executionTimeSeqs.length - 1];
            Map<Long, String> lastExecutionTimeMap = parseExecutionTime(lastExecutionTime);
            Map<Long, String> thisExecutionTimeMap = parseExecutionTime(thisExecutionTime);
            int pausedBackfillIdIndex = 0;
            Long backfillId;
            Long beforeTotalTime = 0L;
            Long afterTotalTime = 0L;
            Long beforeTime = 0L;
            Long afterTime = 0L;
            int flag = 0;
            for (; pausedBackfillIdIndex < backfillIdList.size(); pausedBackfillIdIndex++) {
                backfillId = backfillIdList.get(pausedBackfillIdIndex);
                if (lastExecutionTimeMap.containsKey(backfillId) && thisExecutionTimeMap.containsKey(backfillId)) {
                    beforeTime = fromExecutionTime(lastExecutionTimeMap.get(backfillId).toString());
                    afterTime = fromExecutionTime(thisExecutionTimeMap.get(backfillId).toString());
                    beforeTotalTime += beforeTime;
                    afterTotalTime += Math.abs(afterTime);
                    if (afterTime >= 0) {
                        errMsg.add(
                            String.format(" the execution time not passed idempotent test %d, after:before = %d:%d",
                                taskId,
                                afterTime, beforeTime));
                        flag++;
                    }
                }
                if (!lastExecutionTimeMap.containsKey(backfillId)) {
                    break;
                }
            }
            if (flag == 1) {
                errMsg.remove(errMsg.size() - 1);
                flag = 0;
            } else if (flag > 1) {
                result = false;
            }
            if (afterTotalTime > beforeTotalTime * 0.7) {
                errMsg.add(String.format(" the execution time too long for task %d, after:before = %d:%d", taskId,
                    afterTotalTime, beforeTotalTime));
                result = false;
            }
            Long totalBatchExecutionTime = 0L;
            Long totalBatchRowCount = 0L;
            int beginBackfillIdIndex = pausedBackfillIdIndex;
            int totalBackfillIdNum = 0;
            for (; beginBackfillIdIndex < backfillIdList.size(); beginBackfillIdIndex++) {
                backfillId = backfillIdList.get(beginBackfillIdIndex);
                if (thisExecutionTimeMap.containsKey(backfillId)) {
                    totalBatchExecutionTime += fromExecutionTime(thisExecutionTimeMap.get(backfillId).toString());
                    totalBatchRowCount += Long.valueOf(successRowCountMap.get(backfillId).toString());
                    totalBackfillIdNum++;
                }
                if (!thisExecutionTimeMap.containsKey(backfillId)) {
                    break;
                }
            }
            long avgBatchExecutionTime = totalBatchExecutionTime / totalBackfillIdNum;
            long avgBatchRowCount = totalBatchRowCount / totalBackfillIdNum;
            if (thisExecutionTimeMap.containsKey(pausedBackfillIdIndex)) {
                backfillId = backfillIdList.get(pausedBackfillIdIndex);
                if (thisExecutionTimeMap.containsKey(backfillId)) {
                    if (fromExecutionTime(thisExecutionTimeMap.get(backfillId))
                        > avgBatchExecutionTime * EXCEPTION_COUNT
                        || successRowCountMap.get(backfillId) > avgBatchRowCount * EXCEPTION_COUNT) {
                        errMsg.add(String.format(
                            " the execution time too long for task %d, backfill batch %d, time batch:avg=%s:%d, rows batch:avg=%d:%d",
                            taskId,
                            backfillId, thisExecutionTimeMap.get(backfillId), avgBatchExecutionTime,
                            successRowCountMap.get(backfillId), avgBatchRowCount));
                        result = false;
                    }
                }
            }
//            for(; beginBackfillIdIndex < pausedBackfillIdIndex; beginBackfillIdIndex++){
//                backfillId = backfillIdList.get(beginBackfillIdIndex);
//                if(thisExecutionTimeMap.containsKey(backfillId)){
//                    if(thisExecutionTimeMap.get(backfillId) > avgBatchExecutionTime * 10){
//                        errMsg.add(String.format(" the execution time too long for task %d, backfill batch %d", taskId, backfillId));
//                        return false;
//                    }
//                }
//            }
        }
        return result;
    }

    public static Boolean checkIfBackfillObjectValid(Connection connection, Long jobId, List<String> errMsg) {
        return true;
    }

    public static Boolean waitTillDdlDone(Connection tddlConnection, Long jobId, String tableName,
                                          int expectedLocalIndexConcurrency, Boolean checkMppTaskAllocation)
        throws InterruptedException {
        return waitTillDdlDone(null, tddlConnection, jobId, tableName, expectedLocalIndexConcurrency, "alter table",
            checkMppTaskAllocation);
    }

    public static Boolean waitTillDdlDone(Log logger, Connection tddlConnection, Long jobId, String tableName,
                                          int expectedLocalIndexConcurrency, String pattern,
                                          Boolean checkMppTaskAllocation)
        throws InterruptedException {
        Boolean waitDone = false;
        String showDdlSql = String.format("select state from metadb.ddl_engine where job_id =  %d", jobId);
        String showPhysicalDdlSql = "show physical processlist where info like '%" + pattern + "%'";
        String showDdlEngineStatusSql = String.format(
            "select node_ip from information_schema.ddl_scheduler where job_id = %d and task_name = \"%s\" and task_state = \"ACTIVE\""
            , jobId, LogicalTableGsiPkRangeBackfillTask.class.getName());
        String showActiveTaskSql = String.format(
            "select task_name, task_id, task_state, execution_time from information_schema.ddl_scheduler where job_id = %d and task_state = \"ACTIVE\""
            , jobId);
        String showFullDdlSql = "show full ddl";
        int i = 0;
        int finalConcurrency = 0;
        if (logger != null) {
            logger.info(String.format(" loop start "));
        }
        while (i < 10000) {
            List<List<Object>> results =
                JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, showDdlSql));
            if (results.isEmpty()) {
                waitDone = true;
                break;
            }
            if (i % 1000 == 0 && logger != null) {
                results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, showFullDdlSql));
                logger.info(String.format(" loop %d, show full ddl is  %s", i, results.toString()));
                results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, showActiveTaskSql));
                logger.info(String.format(" loop %d, show ddl active task is  %s", i, results.toString()));
            }
            results = new ArrayList<>();
            if (expectedLocalIndexConcurrency > 0) {
                try {
                    ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, showPhysicalDdlSql);
                    while (resultSet.next()) {
                        Object physicalDb = resultSet.getObject("db");
                        Object sql = resultSet.getObject("info");
                        results.add(Lists.newArrayList(physicalDb, sql));
                    }
                } catch (SQLException | NullPointerException e) {
                    if (logger != null) {
                        logger.warn(e.getMessage());
                    }
                    i++;
                    continue;
                }
                Map<String, Integer> physicalConcurrency = new HashMap<>();
                for (List<Object> result : results) {
                    String phyDbName = result.get(0).toString();
                    physicalConcurrency.put(phyDbName, physicalConcurrency.getOrDefault(phyDbName, 0) + 1);
                }
                if (!physicalConcurrency.isEmpty() && logger != null) {
                    logger.info(String.format(" loop %d, the physical concurrency is %s", i, physicalConcurrency));
                }
                if (!physicalConcurrency.isEmpty()) {
                    int maxConcurreny =
                        physicalConcurrency.values().stream().max(Comparator.comparingInt(o -> o)).get();
                    finalConcurrency = Math.max(maxConcurreny, finalConcurrency);
                }
            }
            if (checkMppTaskAllocation) {
                results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, showDdlEngineStatusSql));
                Map<String, Long> elementCounts = results.stream().map(o -> o.get(0).toString())
                    .collect(Collectors.groupingBy(o -> o, Collectors.counting()));
                int totalNum = results.size();
                if (elementCounts.size() > 0) {
                    int avgNum = totalNum / elementCounts.size();
                    for (String nodeIp : elementCounts.keySet()) {
                        if (elementCounts.get(nodeIp) > avgNum + 1 || elementCounts.get(nodeIp) < avgNum - 1) {
                            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                                String.format(
                                    " check mpp task allocation error, expected %d active count, while get %d",
                                    avgNum, elementCounts.get(nodeIp)));
                        }
                    }
                }

            }
            Thread.sleep(50);
            i++;
        }
        if (logger != null) {
            logger.info(String.format(" loop done for %d times ", i));
        }
        if (finalConcurrency < expectedLocalIndexConcurrency) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                " expected concurrency:" + expectedLocalIndexConcurrency + " but got " + finalConcurrency);
        }
        return waitDone;
    }

    public static Boolean waitTillDdlRunning(Connection tddlConnection, Long jobId, String tableName)
        throws InterruptedException {
        Boolean waitRunning = false;
        String sql = String.format("select state from metadb.ddl_engine where job_id =  %d", jobId);
        int i = 0;
        while (i < 1000) {
            List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql));
            if (results.isEmpty()) {
                waitRunning = false;
                break;
            } else if (results.get(0).get(0).toString().equalsIgnoreCase("RUNNING")) {
                waitRunning = true;
                break;
            }
            Thread.sleep(1 * 1000);
            i++;
        }
        return waitRunning;
    }

    public static Boolean waitTillCommit(Connection tddlConnection, Long jobId, String tableName)
        throws InterruptedException {
        Boolean waitCommit = false;
        String sql = String.format(
            "select state from metadb.ddl_engine_task where job_id =  %d and name = \"CommitTwoPhaseDdlTask\"", jobId);
        int i = 0;
        while (i < 1000) {
            List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql));
            if (results.stream().allMatch(o -> o.get(0).toString().equalsIgnoreCase("DIRTY"))) {
                waitCommit = true;
                break;
            }
            Thread.sleep(2 * 1000);
            i++;
        }
        return waitCommit;
    }

    public static Boolean waitTillPrepare(Connection tddlConnection, Long jobId, String tableName)
        throws InterruptedException {
        Boolean waitCommit = false;
        String sql = String.format(
            "select state from metadb.ddl_engine_task where job_id =  %d and name = \"PrepareTwoPhaseDdlTask\"", jobId);
        int i = 0;
        while (i < 1000) {
            List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql));
            if (results.stream().allMatch(o -> o.get(0).toString().equalsIgnoreCase("DIRTY"))) {
                waitCommit = true;
                break;
            }
            Thread.sleep(2 * 1000);
            i++;
        }
        return waitCommit;
    }

    public static Boolean waitTillImportTableSpaceDone(Connection tddlConnection, Long jobId, String tableName)
        throws InterruptedException {
        Boolean waitCommit = false;
        String sql = String.format(
            "select state from metadb.ddl_engine_task where job_id =  %d and name = \"ImportTableSpaceDdlNormalTask\"",
            jobId);
        int i = 0;
        while (i < 1000) {
            List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql));
            if (results.isEmpty() || results.stream().allMatch(o -> o.get(0).toString().equalsIgnoreCase("SUCCESS"))) {
                waitCommit = true;
                break;
            }
            Thread.sleep(2 * 1000);
            i++;
        }
        return waitCommit;
    }

    public static Boolean waitTillLogicalBackfillDone(Connection tddlConnection, Long jobId, String tableName)
        throws InterruptedException {
        Boolean waitCommit = false;
        String sql = String.format(
            "select state from metadb.ddl_engine_task where job_id =  %d and name = \"AlterTableGroupBackFillTask\"",
            jobId);
        int i = 0;
        while (i < 1000) {
            List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql));
            if (results.isEmpty() || results.stream().allMatch(o -> o.get(0).toString().equalsIgnoreCase("SUCCESS"))) {
                waitCommit = true;
                break;
            }
            Thread.sleep(2 * 1000);
            i++;
        }
        return waitCommit;
    }

    public static Boolean checkTableStatus(Connection tddlConnection, Long jobId, String tableName) {
        String sql = "check table " + tableName;
        Boolean checkTableOk = true;
        List<List<Object>> results = null;
        try {
            ResultSet resultSet = JdbcUtil.executeQueryWithoutAssert(sql, tddlConnection);
            results = JdbcUtil.getAllResult(resultSet);
        } catch (Exception e) {
            checkTableOk = false;
        }
        if (checkTableOk) {
            checkTableOk = results.stream().allMatch(o -> o.get(3).toString().equalsIgnoreCase("OK"));
        }
        if (!checkTableOk) {
            LOG.info("check table bad results: " + results);
        }
        return checkTableOk;
    }

    public static Boolean checkPhyDdlStatus(String schemaName, Connection tddlConnection, Long jobId, int sleepTime,
                                            String tableName)
        throws InterruptedException {
        Boolean checkResult = true;
        Map<String, Map<String, Integer>> lastFetchedPhyDdlStatus =
            getPhyDdlStatus(schemaName, tableName, tddlConnection, jobId);
        Thread.sleep(sleepTime * 1000L);
        Map<String, Map<String, Integer>> phyDdlStatus = getPhyDdlStatus(schemaName, tableName, tddlConnection, jobId);
        for (String phyDb : phyDdlStatus.keySet()) {
            int waitRunningCount = lastFetchedPhyDdlStatus.get(phyDb).get("WAIT_RUNNING");
            if (waitRunningCount != phyDdlStatus.get(phyDb).get("WAIT_RUNNING")) {
                checkResult = false;
            }
        }
        if (checkResult) {
            checkResult = checkTableStatus(tddlConnection, jobId, tableName);
        }
        return checkResult;

    }

    public static String convertString(String originalDdl) {
        return originalDdl.replace("'", "\\'");
    }

    public static Long getRootDdlJobIdFromPattern(Connection tddlConnection, String originalDdl)
        throws InterruptedException {
        String sql1 =
            "select job_id from metadb.ddl_engine where ddl_stmt like '%" + convertString(originalDdl)
                + "%' order by id desc limit 1";
        String sql2 = "select name from metadb.ddl_engine_task where job_id = ";
        List<List<Object>> results1 = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql1));
        int time = 0;
        Long jobId = -1L;
        while (results1.isEmpty() && time > 2000) {
            Thread.sleep(100);
            results1 = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql1));
            time++;
        }
        if (results1.isEmpty()) {
            return -1L;
        } else {
            jobId = Long.parseLong(results1.get(0).get(0).toString());
        }
        LOG.info(String.format("fetch ddl job_id %d, %s", jobId, originalDdl));
        return jobId;
    }

    public static Long getDdlJobIdFromPattern(Connection tddlConnection, String originalDdl)
        throws InterruptedException {
        String sql1 =
            "select job_id from metadb.ddl_engine where ddl_stmt like '%" + convertString(originalDdl)
                + "%' order by id desc limit 1";
        String sql2 = "select name from metadb.ddl_engine_task where job_id = ";
        List<List<Object>> results1 = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql1));
        int time = 0;
        Long jobId = -1L;
        while (results1.isEmpty() && time > 2000) {
            Thread.sleep(100);
            results1 = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql1));
            time++;
        }
        if (results1.isEmpty()) {
            return -1L;
        } else {
            jobId = Long.parseLong(results1.get(0).get(0).toString());
        }
        LOG.info(String.format("fetch ddl job_id %d, %s", jobId, originalDdl));
        List<List<Object>> results2 = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql2 + jobId));
        // for subJob
        while (results2.size() < 3) {
            Thread.sleep(50);
            jobId = Long.valueOf(
                JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql1)).get(0).get(0)
                    .toString());
            results2 = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql2 + jobId));
        }
        return jobId;
    }

    // partition_name => dn_id
    public static Map<String, List<String>> getTableTopology(Connection connection, String tableName)
        throws SQLException {
        String fetchTopology = String.format("show topology %s", tableName);
        ResultSet resultSet2 = JdbcUtil.executeQuery(fetchTopology, connection);
        Map<String, List<String>> tableTopology = new HashMap<>();
        while (resultSet2.next()) {
            String partitionName = resultSet2.getString("PARTITION_NAME");
            String storageInst = resultSet2.getString("DN_ID");
            String groupName = resultSet2.getString("GROUP_NAME");
            String physicalTableName = resultSet2.getString("TABLE_NAME");
            List<String> topology = new ArrayList<>();
            topology.add(storageInst);
            topology.add(groupName);
            topology.add(physicalTableName);
            tableTopology.put(partitionName, topology);
        }
//            +----+-----------------+---------------+----------------+-------------+---------------------------------+
//            | ID | GROUP_NAME      | TABLE_NAME    | PARTITION_NAME| PARENT_PARTITION_NAME | PHY_DB_NAME | DN_ID                           |
//            +----+-----------------+---------------+----------------+-------------+---------------------------------+
//            | 0  | D1_P00002_GROUP | t1_cLVA_00001 | p2           | p2sp1  | d1_p00002   | pxc-xdb-s-pxchzrwy270yxoiww3934 |
        return tableTopology;
    }

    public static List<String> getStorageList(Connection connection) throws SQLException {
        String showStorage = "show storage";
        ResultSet resultSet2 = JdbcUtil.executeQuery(showStorage, connection);
        List<String> storageInsts = new ArrayList<>();
        List<String> undeletableStorages = new ArrayList<>();
        while (resultSet2.next()) {
            String storageInst = resultSet2.getString("STORAGE_INST_ID");
            String instKind = resultSet2.getString("INST_KIND");
            String deletable = resultSet2.getString("DELETABLE");
            if (instKind.equalsIgnoreCase("MASTER")) {
                storageInsts.add(storageInst);
                if (deletable.equalsIgnoreCase("FALSE")) {
                    undeletableStorages.add(storageInst);
                }
            }
        }
        storageInsts.removeAll(undeletableStorages);
        for (String storageInst : undeletableStorages) {
            storageInsts.add(0, storageInst);
        }
        return storageInsts;
    }

    public static void pauseDdl(Connection tddlConnection, Long jobId) {
        String sql = String.format("pause ddl %d", jobId);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public static void continueDdl(Connection tddlConnection, Long jobId) throws InterruptedException {
        String sql = String.format("select state from metadb.ddl_engine where job_id = %d", jobId);
        String ddlState =
            JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql)).get(0).get(0).toString();
        long time = 0;
        while (!ddlState.equalsIgnoreCase("PAUSED") || time > 15L) {
            ddlState =
                JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql)).get(0).get(0).toString();
            time++;
            Thread.sleep(500L);
        }
        sql = String.format("continue ddl %d", jobId);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public static void continueDdlAsync(Connection tddlConnection, Long jobId) throws InterruptedException {
        String sql = String.format("select state from metadb.ddl_engine where job_id = %d", jobId);
        String ddlState =
            JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql)).get(0).get(0).toString();
        long time = 0;
        while (!ddlState.equalsIgnoreCase("PAUSED") || time > 15L) {
            ddlState =
                JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql)).get(0).get(0).toString();
            time++;
            Thread.sleep(500L);
        }
        sql = String.format("/*+TDDL:cmd_extra(PURE_ASYNC_DDL_MODE=true)*/continue ddl %d", jobId);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public static void checkIfReportErrorByChecker(Connection tddlConnection, Long jobId, List<String> phyDbs,
                                                   List<String> phyTables, List<List<String>> pks, int expectedErrorNum)
        throws InterruptedException {
        String phyDbStr = phyDbs.stream().map(o -> "'" + o + "'").collect(Collectors.joining(","));
        String phyTableStr = phyTables.stream().map(o -> "'" + o + "'").collect(Collectors.joining(","));
        String sql = String.format(
            "select physical_db, physical_table, primary_key, details from metadb.checker_reports where job_id = %d and extra = 'FastCheckerReporter.'",
            jobId, phyDbStr, phyTableStr);
        Map<Pair<String, String>, List<String>> errors = new HashMap<>();
        JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql)).stream()
            .forEach(
                o -> errors.computeIfAbsent(Pair.of(o.get(0).toString(), o.get(1).toString()), k -> new ArrayList<>())
                    .add(o.get(2).toString() + ";" + o.get(3).toString()));
        List<Pair<String, String>> topologys = new ArrayList<>();
        int errorReportNum = 0;
        for (int i = 0; i < phyDbs.size(); i++) {
            String phyDb = phyDbs.get(i);
            String phyTable = phyTables.get(i);
            topologys.add(Pair.of(phyDb, phyTable));
            List<String> pk = pks.get(i);
            List<String> error = errors.get(Pair.of(phyDb, phyTable));
            if (error != null && checkErrorExists(error, pk)) {
                errorReportNum += 1;
            }
        }
        Assert.assertTrue(errorReportNum == expectedErrorNum,
            " expected error, but get ### " + expectedErrorNum + " ### " + errorReportNum + " ### " + JSON.toJSONString(topologys) + " ### " + JSON.toJSONString(pks)
                + " ### "
                + JSON.toJSONString(errors));
    }

    public void testAfterAll(){
        List<Pair<String, String>> topologys = new ArrayList<>();
        List<List<String>> pks = new ArrayList<>();
        Map<Pair<String, String>, List<String>> errors = new HashMap<>();
        int expectedErrorNum = 2;

        String fullErrorString = "";
        String expectedErrorNumString = fullErrorString.split(" ### ")[1];
        String topologyString = fullErrorString.split(" ### ")[3];
        String pkString = fullErrorString.split(" ### ")[4];
        String errorString = fullErrorString.split(" ### ")[5];
        expectedErrorNum = Integer.parseInt(expectedErrorNumString);
        pks = JSON.parseObject(pkString, new TypeReference<List<List<String>>>() {});
        errors = JSON.parseObject(errorString, new TypeReference<Map<Pair<String, String>, List<String>>>() {});
        topologys = JSON.parseObject(topologyString, new TypeReference<List<Pair<String, String>>>(){});

        int errorReportNum = 0;
        for (int i = 0; i < topologys.size(); i++) {
            String phyDb = topologys.get(i).getKey();
            String phyTable = topologys.get(i).getValue();
            List<String> pk = pks.get(i);
            List<String> error = errors.get(Pair.of(phyDb, phyTable));
            if (error != null && checkErrorExists(error, pk)) {
                errorReportNum += 1;
            }
        }
        Assert.assertTrue(errorReportNum == expectedErrorNum,
            " expected error: " + expectedErrorNum + ", " + JSON.toJSONString(topologys) + ", " + JSON.toJSONString(pks)
                + " but get"
                + JSON.toJSONString(errors));
    }
    public static boolean checkErrorExists(List<String> error, List<String> pk) {
        Boolean batchErrorFound = false;
        int pkErrorFound = 0;
        for (String err : error) {
            if (err != null && err.contains("fastchecker found unconsistency, source batch hash")) {
                batchErrorFound = true;
            } else {
                String pkError = err.split(";")[0];
                for (String pkStr : pk) {
                    if (pkError.equalsIgnoreCase(pkStr)) {
                        pkErrorFound++;
                    }
                }
            }
        }
        return (batchErrorFound && (pkErrorFound >= pk.size()));
    }

    public static void tryRollbackDdl(Connection tddlConnection, Long jobId) {
        String sql = String.format("rollback ddl %d", jobId);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public static void alterTableViaJdbc(Connection tddlConnection, String schemaName, String mytable,
                                         String sql) throws SQLException {
        if (sql.contains("rebalance")) {
            JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        } else {
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
    }

    public static void dropTableViaJdbc(Connection tddlConnection, String schemaName, String mytable)
        throws SQLException {
        String sql = "drop table if exists " + schemaName + "." + mytable;
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public static Boolean checkIfExecuteTwoPhaseDdl(Connection tddlConnection, Long jobId) {
        String fetchTaskSql = String.format("select name from metadb.ddl_engine_task_archive where job_id = %d", jobId);
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchTaskSql));
        Boolean containsTwoPhaseDdlTasks = results.stream().anyMatch(o -> o.toString().contains("TwoPhase"));
        return containsTwoPhaseDdlTasks;
    }

    public static Boolean checkIfExecuteByPkRangeAndBuildLocalIndexLater(Connection tddlConnection, Long jobId,
                                                                         Boolean pkRange, Boolean localIndexLater,
                                                                         long expectedPkRangeNum, List<String> errMsg) {
        String fetchTaskSql =
            String.format("select name from metadb.ddl_engine_task_archive where root_job_id = %d or job_id = %d",
                jobId, jobId);
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchTaskSql));
        List<String> names = results.stream().map(o -> o.get(0).toString()).collect(Collectors.toList());
        long pkRangeNum =
            names.stream().filter(o -> LogicalTableGsiPkRangeBackfillTask.class.getName().contains(o.toString()))
                .count();
        Boolean containsPkRange = (pkRangeNum >= 1);
        Boolean enoughPkRange = (pkRangeNum >= expectedPkRangeNum);
        Boolean containsLocalIndexLater =
            names.stream().anyMatch(o -> AlterGsiAddLocalIndexTask.class.getName().contains(o.toString()));
        Boolean result = (pkRange == containsPkRange) && (localIndexLater == containsLocalIndexLater) && enoughPkRange;
        if (!result) {
            String msg = String.format("pk range num is %d, local index later is %s", pkRangeNum, localIndexLater);
            errMsg.add(msg);
        }
        return result;
    }

    public static Boolean checkIfBackfillSampleRowsArchived(Connection tddlConnection, Long jobId, Boolean skipCheck)
        throws InterruptedException {
        if (skipCheck) {
            return true;
        }
        int failedTime = 0;
        Boolean result;
        do {
            result = true;
            String fetchTaskSql =
                String.format("select job_id from metadb.backfill_sample_rows where job_id = %d", jobId,
                    jobId);
            List<List<Object>> results =
                JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchTaskSql));
            List<String> jobIds = results.stream().map(o -> o.get(0).toString()).collect(Collectors.toList());
            if (!jobIds.isEmpty()) {
                result = false;
            }
            fetchTaskSql =
                String.format(
                    "select job_id from metadb.backfill_sample_rows_archive where job_id = %d", jobId,
                    jobId);
            results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchTaskSql));
            jobIds = results.stream().map(o -> o.get(0).toString()).collect(Collectors.toList());
            if (jobIds.isEmpty()) {
                result = false;
            }
            if (!result) {
                Thread.sleep(1000);
                failedTime++;
            }
        } while (!result && failedTime < 15);
        return result;
    }

    public static Boolean checkIfCompleteFully(Connection tddlConnection, Long jobId, String tableName) {
        String fetchJobSql = String.format("select state from metadb.ddl_engine_archive where job_id = %d", jobId);
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchJobSql));
        Boolean jobCompleted = false;
        if (results.size() > 0) {
            String state = results.get(0).get(0).toString();
            jobCompleted = state.equalsIgnoreCase("COMPLETED");
        }
        String fetchPhyDdl = String.format("show physical ddl");
        results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchPhyDdl));
        Boolean physicalDdlFinish = results.isEmpty();
        String showPhyDdlProcess =
            String.format("show physical processlist where info like \"%%alter table%%%s%%\"", tableName);
        results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, showPhyDdlProcess));
        if (physicalDdlFinish) {
            physicalDdlFinish = results.isEmpty();
        }
        return jobCompleted && physicalDdlFinish;
    }

    public static Boolean checkIfTerminate(Connection tddlConnection, Long jobId) {
        String fetchJobSql = String.format("select state from metadb.ddl_engine where job_id = %d", jobId);
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchJobSql));
        List<String> terminateStates = Lists.newArrayList("COMPLETED", "ROLLBACK_COMPLETED");
        if (results.isEmpty()) {// || terminateStates.contains(results.get(0).get(0).toString())) {
            return true;
        } else {
            return false;
        }
    }

    public static Boolean checkIfCompleteSuccessful(Connection tddlConnection, Long jobId) {
        String fetchJobSql = String.format("select state from metadb.ddl_engine_archive where job_id = %d", jobId);
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchJobSql));
        if (results.isEmpty()) {
            return false;
        }
        String state = results.get(0).get(0).toString();
        Boolean jobCompleted = state.equalsIgnoreCase("COMPLETED");
        return jobCompleted;
    }

    public static Boolean checkIfPauseSuccessful(Connection tddlConnection, Long jobId) {
        Boolean jobPaused = false;
        List<List<Object>> results =
            JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, " show ddl " + jobId));
        // state
        String state = results.get(0).get(5).toString();
        // current_phy_ddl_progress
        String ddlProcess = results.get(0).get(7).toString();
        if (state.equalsIgnoreCase("PAUSED") && !ddlProcess.equalsIgnoreCase("100%") && !ddlProcess.equalsIgnoreCase(
            "0%")) {
            jobPaused = true;
        }
        return jobPaused;
    }

    public static Map<String, List<String>> getPrimaryTableToGsiMap(Connection tddlConnection, String schemaName) {
        String fetchJobSql = String.format(
            "select table_name, index_name from metadb.indexes where table_schema = \"%s\" and index_type is null and seq_in_index = 1;",
            schemaName);
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchJobSql));
        Map<String, List<String>> primaryTableToGsiMap = new HashMap<>();
        for (List<Object> result : results) {
            primaryTableToGsiMap.computeIfAbsent(result.get(0).toString(), k -> new ArrayList<>())
                .add(result.get(1).toString());
        }
        String fetchTableNameSql = String.format(
            "select table_name from metadb.table_partitions where table_schema = \"%s\" and part_level = 0 and tbl_type in (0, 2, 3);",
            schemaName);
        results =
            JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchTableNameSql));
        for (List<Object> result : results) {
            primaryTableToGsiMap.computeIfAbsent(result.get(0).toString(), k -> new ArrayList<>());
        }

        return primaryTableToGsiMap;
    }

    public static Map<String, List<String>> getTableGroupToTableMap(Connection tddlConnection, String schemaName) {
        String fetchTableNameAndGroupIdSql = String.format(
            "select table_name, group_id from metadb.table_partitions where table_schema = \"%s\" and part_level = 0;",
            schemaName);
        List<List<Object>> results =
            JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchTableNameAndGroupIdSql));
        Map<Integer, List<String>> groupIdToTableMap = new HashMap<>();
        for (List<Object> result : results) {
            groupIdToTableMap.computeIfAbsent(Integer.parseInt(result.get(1).toString()), k -> new ArrayList<>())
                .add(result.get(0).toString());
        }
        String fetchGroupIdAndTableGroupSql =
            String.format("select tg_name, id from metadb.table_group where schema_name = \"%s\"", schemaName);
        results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchGroupIdAndTableGroupSql));
        Map<Integer, String> groupIdToTableGroupMap = new HashMap<>();
        for (List<Object> result : results) {
            groupIdToTableGroupMap.put(Integer.parseInt(result.get(1).toString()), result.get(0).toString());
        }
        Map<String, List<String>> tableGroupToTableMap = new HashMap<>();
        for (Integer groupId : groupIdToTableMap.keySet()) {
            tableGroupToTableMap.put(groupIdToTableGroupMap.get(groupId), groupIdToTableMap.get(groupId));
        }
        return tableGroupToTableMap;
    }

    public static Map<String, String> getTableToTableGroupMap(Connection tddlConnection, String schemaName) {
        Map<String, List<String>> tableGroupToTableMap = getTableGroupToTableMap(tddlConnection, schemaName);
        Map<String, String> tableToTableGroupMap = new HashMap<>();
        for (String tableGroup : tableGroupToTableMap.keySet()) {
            for (String table : tableGroupToTableMap.get(tableGroup)) {
                tableToTableGroupMap.put(table, tableGroup);
            }
        }
        return tableToTableGroupMap;
    }

    public static void waitPlanIdOrSchemaNameFinish(Connection tddlConnection, String schemaName)
        throws InterruptedException {
        final String querySql =
            String.format("select count(1),max(plan_id) from metadb.ddl_plan where table_schema = '%s' and state != 'SUCCESS'",
                schemaName);
        boolean waitOk = false;
        for (int i = 0; i < 1000; i++) {
            int count = Integer.parseInt(
                JdbcUtil.getAllResult(JdbcUtil.executeQuery(querySql, tddlConnection)).get(0).get(0).toString());
            if (count > 0) {
                Thread.sleep(1000);
            } else {
                waitOk = true;
                break;
            }
        }

        if (!waitOk) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "we have failed to execute rebalance plan for " + schemaName);
        }

    }

    public static Map<String, String> convertUnionSetToSetMap(Map<String, String> unionSet) {
        for (String tableGroup : unionSet.keySet()) {
            String finalPrimaryTableGroup = tableGroup;
            while (unionSet.containsKey(finalPrimaryTableGroup) && !unionSet.get(finalPrimaryTableGroup)
                .equalsIgnoreCase(finalPrimaryTableGroup)) {
                finalPrimaryTableGroup = unionSet.get(finalPrimaryTableGroup);
            }
            String nextTableGroup = tableGroup;
            while (unionSet.containsKey(nextTableGroup) && !unionSet.get(nextTableGroup)
                .equals(finalPrimaryTableGroup)) {
                String tempTableGroup = unionSet.get(nextTableGroup);
                unionSet.put(nextTableGroup, finalPrimaryTableGroup);
                nextTableGroup = tempTableGroup;
            }
        }

        return unionSet;
    }

    public static Boolean compareForLocalIndex(Connection tddlConnection, String schemaName, Long jobId,
                                               String originalTable,
                                               String createTableStmt, String addGsiStmt, String gsiName, int partNum,
                                               List<String> errMsg) throws SQLException {
        String compareOriginalTable = originalTable + "_compare";
        String compareGsiName = gsiName + "_compare";
        if (!StringUtils.isEmpty(createTableStmt)) {
            String createCompareTable = String.format(createTableStmt, compareOriginalTable, partNum);
            alterTableViaJdbc(tddlConnection, schemaName, originalTable, createCompareTable);
        }
        String addGsiForCompareTable = String.format(addGsiStmt, compareOriginalTable, compareGsiName, partNum);
        String hint =
            "/*+TDDL:CMD_EXTRA(PURE_ASYNC_DDL_MODE=false,GSI_BACKFILL_BY_PK_RANGE=false,GSI_BUILD_LOCAL_INDEX_LATER=false)*/";
        alterTableViaJdbc(tddlConnection, schemaName, originalTable, hint + addGsiForCompareTable);
        String gsiCreateTableSql1 = getGsiCreateTable(tddlConnection, schemaName, originalTable, gsiName);
        String gsiCreateTableSql2 = getGsiCreateTable(tddlConnection, schemaName, compareOriginalTable, compareGsiName);
        SQLStatementParser mySqlCreateTableParser =
            SQLParserUtils.createSQLStatementParser(gsiCreateTableSql1, DbType.mysql);
        MySqlCreateTableStatement gsiCreateTableStmt = (MySqlCreateTableStatement) mySqlCreateTableParser.parseCreate();
        gsiCreateTableStmt.setTableName(gsiName);
        SQLStatementParser mySqlCreateTableParser2 =
            SQLParserUtils.createSQLStatementParser(gsiCreateTableSql2, DbType.mysql);
        MySqlCreateTableStatement gsiCreateTableStmt2 =
            (MySqlCreateTableStatement) mySqlCreateTableParser2.parseCreate();
        gsiCreateTableStmt2.setTableName(gsiName);
        String s1 = gsiCreateTableStmt.toString();
        String s2 = gsiCreateTableStmt2.toString();
        if (s1.equalsIgnoreCase(s2)) {
            return true;
        } else {
            String msg = String.format("the original is %s, the reference is %s", s1, s2);
            errMsg.add(msg);
            return false;
        }
    }

    public static Boolean compareForLocalIndex(String stmt1, String stmt2) {
        Assert.assertTrue(!stmt1.equals(stmt2));
        SQLStatementParser stmt1Parser =
            SQLParserUtils.createSQLStatementParser(stmt1, DbType.mysql);
        MySqlCreateTableStatement createStmt1 =
            (MySqlCreateTableStatement) stmt1Parser.parseCreate();
        SQLStatementParser stmt2Parser =
            SQLParserUtils.createSQLStatementParser(stmt2, DbType.mysql);
        MySqlCreateTableStatement createStmt2 =
            (MySqlCreateTableStatement) stmt2Parser.parseCreate();
        createStmt1.setTableName(createStmt2.getTableName());
        String s1 = createStmt1.toString();
        String s2 = createStmt2.toString();
        return s1.equalsIgnoreCase(s2);

    }

    public static String getGsiCreateTable(Connection tddlConnection, String schemaName, String originalTable,
                                           String gsiName) {
        Pair<Integer, String> fullObjectName =
            CheckTableTest.getFullObjectName(tddlConnection, originalTable, gsiName, 0);
        Integer groupIndex = fullObjectName.getKey();
        String gsiFullName = fullObjectName.getValue();
        String showCreateTableSql = String.format("/*+TDDL:node(%d)*/ show create table `%s`", groupIndex, gsiFullName);
        ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, showCreateTableSql);
        String createTableSql = JdbcUtil.getAllResult(resultSet).get(0).get(1).toString();
        return createTableSql;
    }

    public static String getGsiCreateTableStmt(Connection tddlConnection, String schemaName, String originalTable,
                                               String gsiName) throws SQLException {
        Pair<Integer, String> fullObjectName = CheckTableTest.getFullGsiName(tddlConnection, originalTable, gsiName, 0);
        Integer groupIndex = fullObjectName.getKey();
        String gsiFullName = fullObjectName.getValue();
        String showCreateTableSql = String.format("/*+TDDL:node(%d)*/ show create table `%s`", groupIndex, gsiFullName);
        ResultSet resultSet = JdbcUtil.executeQuerySuccess(tddlConnection, showCreateTableSql);
        String createTableSql = JdbcUtil.getAllResult(resultSet).get(0).get(1).toString();
        return createTableSql;
    }

    public static Boolean checkIfRollbackFully(Connection tddlConnection, Long jobId, String tableName) {
        String fetchJobSql = String.format("select state from metadb.ddl_engine_archive where job_id = %d", jobId);
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchJobSql));
        String state = results.get(0).get(0).toString();
        Boolean jobRollback = state.equalsIgnoreCase("ROLLBACK_COMPLETED");
        String fetchPhyDdl = String.format("show physical ddl");
        results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchPhyDdl));
        Boolean physicalDdlFinish = results.isEmpty();
        String showPhyDdlProcess =
            String.format("show physical processlist where info like \"%%alter table%%%s%%\"", tableName);
        results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, showPhyDdlProcess));
        if (physicalDdlFinish) {
            physicalDdlFinish = results.isEmpty();
        }
        return jobRollback && physicalDdlFinish;
    }

    public static Boolean checkIfExecuteTwoPhaseDdl(Connection tddlConnection, String originalDdl) {
        String sql = "select job_id from metadb.ddl_engine_archive where ddl_stmt like '" + originalDdl
            + "' order by gmt_created desc limit 1";
        Long jobId = Long.valueOf(
            JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql)).get(0).get(0).toString());
        String fetchTaskSql = String.format("select name from metadb.ddl_engine_task_archive where job_id = %d", jobId);
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchTaskSql));
        Boolean containsTwoPhaseDdlTasks = results.stream().anyMatch(o -> o.toString().contains("TwoPhase"));
        return containsTwoPhaseDdlTasks;
    }

    public static Boolean compareShowDdlResult(List<List<Object>> showDdlResultBefore,
                                               List<List<Object>> showDdlResultLater) {
        // JOB_ID
        // OBJECT_SCHEMA
        // OBJECT_NAME
        // ENGINE
        // DDL_TYPE
        // STATE
        // TOTAL_BACKFILL_PROGRESS
        // CURRENT_PHY_DDL_PROGRESS
        // PROGRESS
        // FASTCHECKER_TASK_NUM
        // FASTCHECKER_TASK_FINISHED
        // START_TIME
        // END_TIME
        // ELAPSED_TIME(MS)
        // PHY_PROCESS
        // CANCELABLE
        int[] expectedTheSameColumns = new int[] {
            0, //JOB_ID
            1, //OBJECT_SCHEMA
            2, //OBJECT_NAME
            3, //ENGINE
            4, //DDL_TYPE
            11 //START_TIME
        };
        if (showDdlResultLater.isEmpty() || showDdlResultBefore.isEmpty()) {
            return true;
        }
        String msg =
            String.format("show ddl and show full ddl result diffs: %s, %s", showDdlResultBefore, showDdlResultLater);
        int minSize = Math.min(showDdlResultBefore.size(), showDdlResultLater.size());
        for (int i = 0; i < minSize; i++) {

            List<Object> row = showDdlResultBefore.get(i);
            List<Object> afterRow = showDdlResultLater.get(i);
            for (int column : expectedTheSameColumns) {
                if (!row.get(column).equals(afterRow.get(column))) {
                    Assert.assertTrue(false, String.format("row %d, column %s diff ", i, column) + msg);
                }

            }
            int progress = fetchProgress(row.get(8).toString());
            int afterProgress = fetchProgress(afterRow.get(8).toString());
            Assert.assertTrue(progress <= afterProgress || afterProgress <= progress * 0.5,
                String.format("row %d, progress diff ", i) + msg);
        }
        return true;
    }

    public static int fetchProgress(String progess) {
        return Integer.parseInt(progess.substring(0, progess.length() - 1));
    }

    public static Boolean checkIfBackfillSampleRowsDelete(Connection tddlConnection, String schemaName)
        throws InterruptedException {
        Thread.sleep(2000);
        String fetchTaskSql =
            String.format("select job_id from metadb.backfill_sample_rows where schema_name = '%s'", schemaName);
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchTaskSql));
        List<String> jobIds = results.stream().map(o -> o.get(0).toString()).collect(Collectors.toList());
        Boolean result = true;
        if (!jobIds.isEmpty()) {
            result = false;
        }
        fetchTaskSql =
            String.format(
                "select job_id from metadb.backfill_sample_rows_archive where schema_name = '%s'", schemaName);
        results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchTaskSql));
        jobIds = results.stream().map(o -> o.get(0).toString()).collect(Collectors.toList());
        if (!jobIds.isEmpty()) {
            result = false;
        }
        return result;
    }

    public static void dropDbAndCheckArchived(String schemaName, Connection tddlConnection)
        throws SQLException, InterruptedException {
        alterTableViaJdbc(tddlConnection, null, null, "use polardbx");
        alterTableViaJdbc(tddlConnection, null, null, "drop database " + schemaName);
        if (!checkIfBackfillSampleRowsDelete(tddlConnection, schemaName)) {
            throw new RuntimeException("now we drop database and expect all the sample rows meta clean but not!");

        }

    }

    public static List<String> generateRandomMatchCreateTableAndGsiSql(int tableNums, int maxGsiNum) {
        int columNum = maxGsiNum + 1;
        int maxVarcharLength = Math.min(tableNums, 255);
        Random random = new Random(maxVarcharLength);

        List<String> result = new ArrayList<>();
        for (int i = 0; i < tableNums; i++) {
            String tableName = "t" + i;
            List<String> columnDefs = new ArrayList<>();
            for (int j = 0; j < columNum; j++) {
                String columnDef = " c" + j + String.format(" varchar(%d) ", random.nextInt(maxVarcharLength) + 1);
                columnDefs.add(columnDef);
            }
            String createTableStmt =
                String.format(" create table %s (%s) partition by hash(c0) partitions 16", tableName,
                    StringUtil.join(",", columnDefs));
            result.add(createTableStmt);
            int gsiNum = random.nextInt(maxGsiNum) + 1;
            for (int k = 0; k < gsiNum; k++) {
                String gsiColumn = "c" + random.nextInt(columNum);
                String gsiStmt = String.format(
                    " alter table %s add global index g_i_%s_%s_%d(%s) partition by hash(%s) partitions 16", tableName,
                    tableName, gsiColumn, k, gsiColumn, gsiColumn);
                result.add(gsiStmt);
            }
        }
        for (int i = 0; i < 2; i++) {
            String singleTableName = "s" + i;
            String createSingleTableStmt = String.format(" create table %s (c0 int, c1 int) single", singleTableName);
            result.add(createSingleTableStmt);

            String broadcastTableName = "b" + i;
            String createBroadcastTableStmt =
                String.format(" create table %s (c0 int, c1 int) broadcast", broadcastTableName);
            result.add(createBroadcastTableStmt);
        }

        for (int i = 0; i < 8; i++) {
            String singleTableName = "s_b" + i;
            String createSingleTableStmt =
                String.format(" create table %s (c0 int, c1 int) single locality='balance_single_table=on'",
                    singleTableName);
            result.add(createSingleTableStmt);

        }
        return result;
    }
}
