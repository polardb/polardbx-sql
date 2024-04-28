package com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.ConnectionWrap;
import com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestCaseUtils.LocalitySingleTaskCaseTask;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.DataLoader;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.UniformDistributionDataGenerator;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.util.JdbcUtil.useDb;

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
        Boolean waitDone = false;
        String sql = String.format("select state from metadb.ddl_engine where job_id =  %d", jobId);
        int i = 0;
        while (i < 1000) {
            List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql));
            if (results.isEmpty()) {
                waitDone = true;
                break;
            }
            Thread.sleep(2 * 1000);
            i++;
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

    public static Boolean checkTableStatus(Connection tddlConnection, Long jobId, String tableName) {
        String sql = "check table " + tableName;
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql));
        Boolean checkTableOk = results.stream().allMatch(o -> o.get(3).toString().equalsIgnoreCase("OK"));
        if (!checkTableOk) {
            LOG.info("check table results: " + results);
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

    public static Long getDdlJobIdFromPattern(Connection tddlConnection, String originalDdl) {
        String sql = "select job_id from metadb.ddl_engine where ddl_stmt like '%" + originalDdl + "%' limit 1";
        Long jobId = Long.valueOf(
            JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, sql)).get(0).get(0).toString());
        return jobId;
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

    public static void tryRollbackDdl(Connection tddlConnection, Long jobId) {
        String sql = String.format("rollback ddl %d", jobId);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public static void alterTableViaTwoPhaseDdl(Connection tddlConnection, String schemaName, String mytable,
                                                String sql) throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    public static Boolean checkIfExecuteTwoPhaseDdl(Connection tddlConnection, Long jobId) {
        String fetchTaskSql = String.format("select name from metadb.ddl_engine_task_archive where job_id = %d", jobId);
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchTaskSql));
        Boolean containsTwoPhaseDdlTasks = results.stream().anyMatch(o -> o.toString().contains("TwoPhase"));
        return containsTwoPhaseDdlTasks;
    }

    public static Boolean checkIfCompleteFully(Connection tddlConnection, Long jobId, String tableName) {
        String fetchJobSql = String.format("select state from metadb.ddl_engine_archive where job_id = %d", jobId);
        List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuerySuccess(tddlConnection, fetchJobSql));
        String state = results.get(0).get(0).toString();
        Boolean jobCompleted = state.equalsIgnoreCase("COMPLETED");
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
}
