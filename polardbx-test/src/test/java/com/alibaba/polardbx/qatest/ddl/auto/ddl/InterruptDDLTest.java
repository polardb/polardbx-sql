package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class InterruptDDLTest extends DDLBaseNewDBTestCase {

    protected static final String TEST_TABLE = "test_ddl_interruption";

    protected static final String HINT_PURE_MODE = "/*+TDDL:CMD_EXTRA(PURE_ASYNC_DDL_MODE=TRUE)*/";
    protected static final String HINT_NODE_PUSHED = "/!TDDL:NODE=%s*/";

    protected static final String CREATE_TABLE =
        "create table %s (c1 int not null primary key, c2 varchar(100), c3 int, c4 int) partition by key(c1) partitions %s";
    protected static final String DROP_TABLE_IF_EXISTS = "drop table if exists %s";

    protected static final String SET_FAIL_POINT = "set @%s='%s'";
    protected static final String SET_FP_CLEAR = "set @FP_CLEAR=true";

    protected static volatile boolean stillAlive = false;
    protected static volatile boolean timeToDie = false;

    @Before
    public void init() throws SQLException {
        if (!stillAlive) {
            dropTable();
            createTable();
            insertRows();
            stillAlive = true;
        }
    }

    @After
    public void destroy() {
        if (timeToDie) {
            dropTable();
        }
    }

    protected JobInfo executeDDL(String sql) throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, HINT_PURE_MODE + sql);
        JobInfo job = fetchCurrentJob();
        waitUntilJobCompletedOrPaused();
        return job;
    }

    protected void injectDDLTimeout(int ms) throws SQLException {
        String sql = ms > 0 ? String.format(SET_FAIL_POINT, "FP_PHYSICAL_DDL_TIMEOUT", ms) : SET_FP_CLEAR;
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.executeUpdate();
        }
    }

    protected void injectDDLException(String failPointKey, boolean enabled) throws SQLException {
        String sql = enabled ? String.format(SET_FAIL_POINT, failPointKey, "TRUE") : SET_FP_CLEAR;
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.executeUpdate();
        }
    }

    protected void continueUntilComplete(JobInfo job) throws SQLException {
        continueDDL(job);
        waitUntilJobCompletedOrPaused();
    }

    protected void continueRollbackUntilComplete(JobInfo job) throws SQLException {
        continueDDLWithError(job, "The DDL job has been cancelled or interrupted");
        waitUntilJobCompletedOrPaused();
    }

    protected void cancelUntilComplete(JobInfo job) throws SQLException {
        cancelDDL(job);
        waitUntilJobCompletedOrPaused();
    }

    protected void waitUntilJobCompletedOrPaused() throws SQLException {
        JobInfo job;
        do {
            waitForSeconds(1);
            job = fetchCurrentJob();
        } while (job != null &&
            !TStringUtil.equalsIgnoreCase(job.parentJob.state, "PAUSED") &&
            !TStringUtil.equalsIgnoreCase(job.parentJob.state, "ROLLBACK_PAUSED"));
    }

    protected void checkTableConsistency(boolean tableExpectedToExist) throws SQLException {
        checkTable(tableExpectedToExist);
        checkTableMetaData(tableExpectedToExist);
    }

    protected void checkColumnConsistency(String expectedColumnInfo, boolean columnExpectedToExist)
        throws SQLException {
        checkTable(true);
        checkColumnMetaData(expectedColumnInfo, columnExpectedToExist);
    }

    protected String fetchPhyDDLDoneInfo(String taskPrefix) throws SQLException {
        String phyDDLDoneInfo = null;
        String sql = String.format("select extra from ddl_engine_task where name like '%sTablePhyDdlTask'", taskPrefix);
        try (Connection metaDbConn = getMetaConnection();
            PreparedStatement ps = metaDbConn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                phyDDLDoneInfo = rs.getString("extra");
                if (TStringUtil.isNotEmpty(phyDDLDoneInfo) &&
                    TStringUtil.containsIgnoreCase(phyDDLDoneInfo, TEST_TABLE)) {
                    return phyDDLDoneInfo;
                }
            }
        }
        return phyDDLDoneInfo;
    }

    protected String getActualTraceId(JobInfo job) {
        String traceId;
        if (job.subJobs.isEmpty()) {
            traceId = job.parentJob.traceId;
        } else {
            traceId = job.subJobs.get(0).traceId;
        }
        return traceId;
    }

    protected void killPhysicalProcess() throws SQLException {
        JobInfo job = fetchCurrentJob();

        while (job != null &&
            !TStringUtil.equalsIgnoreCase(job.parentJob.state, "PAUSED") &&
            !TStringUtil.equalsIgnoreCase(job.parentJob.state, "ROLLBACK_PAUSED")) {

            String traceId = getActualTraceId(job);

            String processId = fetchProcessId(true, traceId);
            while (TStringUtil.isNotEmpty(processId)) {
                killProcess(true, processId);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
                processId = fetchProcessId(true, traceId);
            }

            job = fetchCurrentJob();
        }
    }

    protected void killLogicalProcess(String commandPrefix) throws SQLException {
        String keyword = String.format("%s table %s", commandPrefix, TEST_TABLE);
        String processId = fetchProcessId(false, keyword);
        if (TStringUtil.isNotEmpty(processId)) {
            killProcess(false, processId);
            waitForSeconds(1);
        }
    }

    protected void killProcess(boolean physical, String processId) throws SQLException {
        String sql = String.format(physical ? "kill '%s'" : "kill %s", processId);
        Set<String> errorsIgnored = new HashSet<>();
        errorsIgnored.add("Unknown thread id");
        JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, sql, errorsIgnored);
    }

    protected void checkPhyProcess(JobInfo job) throws SQLException {
        String traceId = getActualTraceId(job);
        String processId = fetchProcessId(true, traceId);
        if (TStringUtil.isNotEmpty(processId)) {
            Assert.fail(
                String.format("There is still physical process %s running for Job %s", processId, job.parentJob.jobId));
        }
    }

    protected String fetchProcessId(boolean physical, String keyword) throws SQLException {
        String sql = String.format("show full %sprocesslist where command != 'sleep'", physical ? "physical_" : "");
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String info = rs.getString("Info");
                if (TStringUtil.containsIgnoreCase(info, keyword)) {
                    String processId = rs.getString("Id");
                    if (physical) {
                        String group = rs.getString("Group");
                        String atom = rs.getString("Atom");
                        processId = group + "-" + atom + "-" + processId;
                    }
                    return processId;
                }
            }
        }
        return null;
    }

    protected void cancelDDL(JobInfo job) {
        String sql = String.format("cancel ddl %s", job.parentJob.jobId);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    protected void cancelDDLFailed(JobInfo job, String errMsg) {
        String sql = String.format("cancel ddl %s", job.parentJob.jobId);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, errMsg);
    }

    protected void pauseDDL(JobInfo job) {
        String sql = String.format("pause ddl %s", job.parentJob.jobId);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        waitForSeconds(1);
    }

    protected void continueDDL(JobInfo job) {
        String sql =
            String.format("/*+TDDL:cmd_extra(CHECK_RESPONSE_IN_MEM = false)*/continue ddl %s", job.parentJob.jobId);
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    protected void continueDDLWithError(JobInfo job, String errMsg) {
        String sql = String.format("continue ddl %s", job.parentJob.jobId);
        JdbcUtil.executeFaied(tddlConnection, sql, errMsg);
    }

    protected void checkJobPaused() throws SQLException {
        checkJobState("PAUSED");
    }

    protected void checkJobRollbackPaused() throws SQLException {
        checkJobState("ROLLBACK_PAUSED");
    }

    protected void checkJobState(String expected) throws SQLException {
        JobInfo job = fetchCurrentJob();
        if (job == null) {
            Assert.fail("Not found any job");
        }
        if (!TStringUtil.equalsIgnoreCase(job.parentJob.state, expected)) {
            Assert.fail(String.format("Job %s has wrong state %s", job.parentJob.jobId, job.parentJob.state));
        }
    }

    protected void checkJobGone() throws SQLException {
        JobInfo job = fetchCurrentJob();
        if (job != null) {
            Assert.fail(String.format("Job %s is still there in %s", job.parentJob.jobId, job.parentJob.state));
        }
    }

    protected JobInfo fetchCurrentJob() throws SQLException {
        JobInfo job = null;
        JobEntry parentJob = null;
        List<JobEntry> subJobs = new ArrayList<>();

        String sql = "show full ddl";
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                JobEntry currentJob = new JobEntry();

                String tableName = rs.getString("OBJECT_NAME");

                if (!TStringUtil.equalsIgnoreCase(tableName, TEST_TABLE)) {
                    continue;
                }

                currentJob.jobId = rs.getLong("JOB_ID");
                currentJob.state = rs.getString("STATE");
                currentJob.traceId = rs.getString("TRACE_ID");
                currentJob.responseNode = rs.getString("RESPONSE_NODE");

                if (currentJob.responseNode.contains("subjob")) {
                    subJobs.add(currentJob);
                } else if (parentJob == null) {
                    parentJob = currentJob;
                } else {
                    Assert.fail("Unexpected: found multiple parent jobs");
                }
            }
        }

        if (parentJob != null) {
            job = new JobInfo();
            job.parentJob = parentJob;
            job.subJobs = subJobs;
        }

        return job;
    }

    protected static class JobInfo {
        JobEntry parentJob;
        List<JobEntry> subJobs;
    }

    protected static class JobEntry {
        long jobId;
        String state;
        String traceId;
        String responseNode;
    }

    protected void checkTable(boolean tableExpectedToExist) {
        List<Pair<String, String>> results = new ArrayList<>();
        String sql = String.format("check table %s", TEST_TABLE);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String msgType = rs.getString("MSG_TYPE");
                String msgText = rs.getString("MSG_TEXT");
                if (!TStringUtil.equalsIgnoreCase(msgType, "status") || !TStringUtil.equalsIgnoreCase(msgText, "OK")) {
                    results.add(Pair.of(msgType, msgText));
                }
            }
        } catch (SQLException e) {
            String errMsg = String.format("Table '%s' doesn't exist", TEST_TABLE);
            if (tableExpectedToExist || !TStringUtil.containsIgnoreCase(e.getMessage(), errMsg)) {
                Assert.fail("Failed to check table: " + e.getMessage());
            }
        }
        if (GeneralUtil.isNotEmpty(results)) {
            fail(results);
        }
    }

    protected void fail(List<Pair<String, String>> checkResults) {
        StringBuilder buf = new StringBuilder();

        buf.append("\nUnexpected: Check Table is not OK\n");

        for (Pair<String, String> result : checkResults) {
            buf.append("MSG_TYPE: ").append(result.getKey()).append(", ");
            buf.append("MSG_TEXT: ").append(result.getValue()).append("\n");
        }

        Assert.fail(buf.toString());
    }

    protected void checkTableMetaData(boolean tableExpectedToExist) throws SQLException {
        int countTables = countTablesFromMeta();
        List<Pair<String, String>> columnsMeta = fetchColumnsFromMeta();
        if (tableExpectedToExist) {
            if (countTables != 1 || columnsMeta.isEmpty()) {
                Assert.fail(String.format("No table and column meta found: %s, %s", countTables, columnsMeta.size()));
            }
            checkColumnMetaData();
        } else {
            if (countTables != 0 || !columnsMeta.isEmpty()) {
                Assert.fail(String.format("Found table and column meta: %s, %s", countTables, columnsMeta.size()));
            }
        }
    }

    protected void checkColumnMetaData() throws SQLException {
        checkColumnMetaData(false, null, false);
    }

    protected void checkColumnMetaData(String expectedColumnInfo, boolean columnExpectedToExist) throws SQLException {
        checkColumnMetaData(true, expectedColumnInfo, columnExpectedToExist);
    }

    private void checkColumnMetaData(boolean checkExpectedColumn, String expectedColumnInfo,
                                     boolean columnExpectedToExist) throws SQLException {
        List<Pair<String, String>> columnsMeta = fetchColumnsFromMeta();
        List<Pair<String, String>> columnsPhyTab = fetchColumnsFromPhyTable();

        if (columnsMeta.size() != columnsPhyTab.size()) {
            fail(columnsMeta, columnsPhyTab);
        }

        boolean foundColumn = false;

        for (int i = 0; i < columnsMeta.size(); i++) {
            Pair<String, String> columnMeta = columnsMeta.get(i);
            Pair<String, String> columnPhyTab = columnsPhyTab.get(i);
            if (!TStringUtil.equalsIgnoreCase(columnMeta.getKey(), columnPhyTab.getKey()) ||
                !TStringUtil.equalsIgnoreCase(columnMeta.getValue(), columnPhyTab.getValue())) {
                fail(columnsMeta, columnsPhyTab);
            }
            if (checkExpectedColumn &&
                TStringUtil.equalsIgnoreCase(columnMeta.getKey() + " " + columnMeta.getValue(), expectedColumnInfo)) {
                foundColumn = true;
            }
        }

        if (checkExpectedColumn && foundColumn && !columnExpectedToExist) {
            fail(expectedColumnInfo, false);
        }

        if (checkExpectedColumn && !foundColumn && columnExpectedToExist) {
            fail(expectedColumnInfo, true);
        }
    }

    protected void fail(String expectedColumnInfo, boolean expectedToExist) {
        StringBuilder buf = new StringBuilder();

        buf.append("\nUnexpected: The column '").append(expectedColumnInfo).append("' ");
        if (expectedToExist) {
            buf.append("doesn't exist\n");
        } else {
            buf.append("still exists\n");
        }

        Assert.fail(buf.toString());
    }

    protected void fail(List<Pair<String, String>> columnsMeta, List<Pair<String, String>> columnsPhyTab) {
        StringBuilder buf = new StringBuilder();

        buf.append("\nUnexpected: The columns in MetaDB aren't consistent with from Physical Tables\n");

        buf.append("\nColumns from MetaDB:\n");
        for (Pair<String, String> columnMeta : columnsMeta) {
            buf.append(columnMeta.getKey()).append(", ").append(columnMeta.getValue()).append("\n");
        }

        buf.append("\nColumns from PhyTable:\n");
        for (Pair<String, String> columnPhyTab : columnsPhyTab) {
            buf.append(columnPhyTab.getKey()).append(", ").append(columnPhyTab.getValue()).append("\n");
        }

        Assert.fail(buf.toString());
    }

    protected int countTablesFromMeta() throws SQLException {
        int countTables = 0;
        String sql =
            String.format("select count(*) from tables where table_schema='%s' and table_name='%s'", tddlDatabase1,
                TEST_TABLE);
        try (Connection metaDbConn = getMetaConnection();
            PreparedStatement ps = metaDbConn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                countTables = rs.getInt(1);
            } else {
                Assert.fail("Unexpected: no result for counting from tables");
            }
        }
        return countTables;
    }

    protected List<Pair<String, String>> fetchColumnsFromMeta() throws SQLException {
        List<Pair<String, String>> columnsMeta = new ArrayList<>();
        String sql = String.format(
            "select column_name, column_type from columns where table_schema='%s' and table_name='%s' order by ordinal_position",
            tddlDatabase1, TEST_TABLE);
        try (Connection metaDbConn = getMetaConnection();
            PreparedStatement ps = metaDbConn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                columnsMeta.add(Pair.of(rs.getString(1), rs.getString(2)));
            }
        }
        return columnsMeta;
    }

    protected List<Pair<String, String>> fetchColumnsFromPhyTable() throws SQLException {
        List<Pair<String, String>> columnsPhyTab = new ArrayList<>();
        Pair<String, String> phyTable = fetchFirstPhyTable();
        String sql = String.format(HINT_NODE_PUSHED + "desc %s", phyTable.getKey(), phyTable.getValue());
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                columnsPhyTab.add(Pair.of(rs.getString(1), rs.getString(2)));
            }
        }
        return columnsPhyTab;
    }

    protected Pair<String, String> fetchFirstPhyTable() throws SQLException {
        return showTopology().get(0);
    }

    protected List<Pair<String, String>> showTopology() throws SQLException {
        List<Pair<String, String>> groupPhyTables = new ArrayList<>();
        String sql = String.format("show topology from %s", TEST_TABLE);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String groupName = rs.getString("GROUP_NAME");
                String phyTableName = rs.getString("TABLE_NAME");

                int lastIndex = TStringUtil.lastIndexOf(groupName, "_");
                String groupNo = TStringUtil.substring(groupName, lastIndex - 1, lastIndex);

                groupPhyTables.add(Pair.of(groupNo, phyTableName));
            }
        }
        return groupPhyTables;
    }

    protected boolean isCurrentConnOnLeader() throws SQLException {
        String sql = "inspect rule version";

        String lastSource = null, isLeader = null;

        try (PreparedStatement ps = tddlConnection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                lastSource = rs.getString("SOURCE");
                isLeader = rs.getString("LEADER");
            }
        }

        if (TStringUtil.isNotEmpty(lastSource) && TStringUtil.isNotEmpty(isLeader)) {
            String[] sourceParts = lastSource.split(":");
            if (sourceParts.length == 3) {
                boolean hostOK = TStringUtil.equals("127.0.0.1", ConnectionManager.getInstance().getPolardbxAddress())
                    || TStringUtil.equals(sourceParts[1], ConnectionManager.getInstance().getPolardbxAddress());
                boolean portOK = TStringUtil.equals(sourceParts[2], ConnectionManager.getInstance().getPolardbxPort());
                boolean flagOK = TStringUtil.equals(isLeader, "Y");
                return hostOK && portOK && flagOK;
            }
        }

        return false;
    }

    protected void createTable() {
        String sql = String.format(CREATE_TABLE, TEST_TABLE, 4);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    protected void insertRows() throws SQLException {
        // Insert one record into each physical table
        String insertSql = HINT_NODE_PUSHED + "insert into %s values(%s,'%s',%s,%s)";
        List<Pair<String, String>> groupPhyTables = showTopology();
        for (int i = 0; i < groupPhyTables.size(); i++) {
            Pair<String, String> groupPhyTable = groupPhyTables.get(i);
            int value = i + 1;
            String sql =
                String.format(insertSql, groupPhyTable.getKey(), groupPhyTable.getValue(), value, value, value, value);
            try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
                ps.executeUpdate();
            }
        }
    }

    protected void dropTable() {
        String sql = String.format(DROP_TABLE_IF_EXISTS, TEST_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    protected void waitForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException ignored) {
        }
    }

    public boolean usingNewPartDb() {
        return true;
    }

}
