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

package com.alibaba.polardbx.qatest.ddl.auto.dag;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.newengine.dag.TaskScheduler;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.failpoint.base.TestNamedDdlTask;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.base.Splitter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * NOTE THAT the test cases inherited from this class must be run for New DDL Engine in the PolarDB-X mode.
 */
@Ignore
public class BaseDdlEngineTestCase extends DDLBaseNewDBTestCase {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BaseDdlEngineTestCase.class);

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    /**
     * Check if a table is consistent after it is created or changed.
     */
    protected boolean isTableConsistent(String tableName) {
        boolean hasTable = hasTable(tableName);
        boolean isStatusOK = isStatusOK(tableName);
        boolean ruleConsistent = areTableRulesConsistent();
        boolean hasDdlJob = hasDdlJob(tableName);

        StringBuilder buf = new StringBuilder();
        buf.append("Table=").append(tableName).append(": ");
        buf.append("hasTable=").append(hasTable).append(", ");
        buf.append("isStatusOK=").append(isStatusOK).append(", ");
        buf.append("ruleConsistent=").append(ruleConsistent).append(", ");
        buf.append("hasDdlJob=").append(hasDdlJob);
        LOGGER.warn(buf.toString());

        return hasTable && isStatusOK && ruleConsistent && !hasDdlJob;
    }

    /**
     * Check if all resources related to a table have been cleaned up and nothing left after it is dropped.
     */
    protected boolean isTableCleanedUp(String tableName) {
        waiting(1000);
        boolean hasTable = hasTable(tableName);
        boolean hasPhyTable = hasPhyTable(tableName);
        boolean ruleConsistent = areTableRulesConsistent();
        boolean hasDdlJob = hasDdlJob(tableName);

        StringBuilder buf = new StringBuilder();
        buf.append("Table=").append(tableName).append(": ");
        buf.append("hasTable=").append(hasTable).append(", ");
        buf.append("hasPhyTable=").append(hasPhyTable).append(", ");
        buf.append("ruleConsistent=").append(ruleConsistent).append(", ");
        buf.append("hasDdlJob=").append(hasDdlJob);
        LOGGER.warn(buf.toString());

        return !hasTable && !hasPhyTable && ruleConsistent && !hasDdlJob;
    }

    protected boolean isStatusOK(String tableName) {
        String result = checkTable(tableName);
        boolean isOK = TStringUtil.equalsIgnoreCase(result, "OK");
        if (!isOK) {
            LOGGER.warn("table=" + tableName + ", check_table=" + result);
        }
        return isOK;
    }

    protected String checkTable(String tableName) {
        String sql = "check table " + tableName;
        Connection conn = getServerConnection();
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getString("MSG_TEXT");
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            Assert.fail("Failed to check table " + tableName + ": " + e.getMessage());
        }
        return null;
    }

    protected boolean areTableRulesConsistent() {
        Map<String, Pair<String, String>> tableRules = inspectRuleVersion();

        Pair<String, String> tableInfo = tableRules.get("META_DB");
        String[] countParts = TStringUtil.split(tableInfo.getKey(), "/");
        int countVisible = Integer.valueOf(countParts[0]);
        int countInvisible = Integer.valueOf(countParts[1]);
        if (countInvisible != 0) {
            return false;
        }

        for (String source : tableRules.keySet()) {
            if (!TStringUtil.equalsIgnoreCase(source, "META_DB")) {
                tableInfo = tableRules.get(source);
                int tableCount = Integer.valueOf(tableInfo.getKey());
                if (countVisible != tableCount) {
                    return false;
                }
                String contentDiff = tableInfo.getValue();
                if (TStringUtil.isNotEmpty(contentDiff)) {
                    return false;
                }
            }
        }

        return true;
    }

    protected Map<String, Pair<String, String>> inspectRuleVersion() {
        Map<String, Pair<String, String>> tableRules = new HashMap<>();
        String sql = "inspect rule version";
        Connection conn = getServerConnection();
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String source = rs.getString("SOURCE");
                String tableCount = rs.getString("TABLE_COUNT");
                String contentDiff = rs.getString("CONTENT_DIFF");
                Pair<String, String> tableInfo = new Pair<>(tableCount, contentDiff);
                tableRules.put(source, tableInfo);
            }
        } catch (SQLException e) {
            log(e);
            Assert.fail("Failed to inspect rule version: " + e.getMessage());
        }
        return tableRules;
    }

    protected boolean hasDdlJob(String objectName) {
        return showDdl().contains(objectName);
    }

    protected Set<String> showDdl() {
        return showOrInspect("show ddl", "OBJECT_NAME");
    }

    protected Set<String> showTopology(String tableName) {
        return showOrInspect("show topology from " + tableName, "TABLE_NAME");
    }

    protected boolean hasPhyTable(String tableName) {
        return showPhyTables(tableName).size() > 0;
    }

    protected Set<String> showPhyTables(String tableName) {
        return showOrInspect("/*TDDL:SCAN*/show tables like '" + tableName + "%'", null);
    }

    protected boolean hasTable(String tableName) {
        return showTables().contains(tableName) &&
            TStringUtil.containsIgnoreCase(showCreateTable(tableName), tableName) &&
            queryInfoSchema(tableName, null, false).size() > 0;
    }

    protected boolean hasColumn(String tableName, String columnName) {
        return showTables().contains(tableName) &&
            TStringUtil.containsIgnoreCase(showCreateTable(tableName), columnName) &&
            queryInfoSchema(tableName, columnName, true).size() > 0;
    }

    protected boolean hasIndex(String tableName, String indexName) {
        return showTables().contains(tableName) &&
            TStringUtil.containsIgnoreCase(showCreateTable(tableName), indexName) &&
            queryInfoSchema(tableName, indexName, false).size() > 0;
    }

    protected Set<String> showTables() {
        return showOrInspect("show tables", null);
    }

    protected String showCreateTable(String tableName) {
        Iterator<String> iterator = showOrInspect("show create table " + tableName, "Create Table").iterator();
        return iterator.hasNext() ? iterator.next() : null;
    }

    protected Set<String> queryInfoSchema(String tableName, String objectName, boolean isColumn) {
        String query = "select * from information_schema.%s where table_name='%s'";
        if (TStringUtil.isEmpty(objectName)) {
            return showOrInspect(String.format(query, "tables", tableName), "table_name");
        } else if (isColumn) {
            return showOrInspect(String.format(query + " and column_name='%s'", "columns", tableName, objectName),
                "column_name");
        } else {
            return showOrInspect(String.format(query + " and index_name='%s'", "statistics", tableName, objectName),
                "index_name");
        }
    }

    protected Set<String> showOrInspect(String sql, String resultColumn) {
        Set<String> objects = new HashSet<>();
        Connection conn = getServerConnection();
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String objectName;
                if (TStringUtil.isEmpty(resultColumn)) {
                    objectName = rs.getString(1);
                } else {
                    objectName = rs.getString(resultColumn);
                }
                objects.add(objectName);
            }
        } catch (SQLException e) {
            log(e);
            Assert.fail("Failed to " + sql + ": " + e.getMessage());
        }
        return objects;
    }

    protected Connection getServerConnection() {
        return tddlConnection;
    }

    protected void waiting(long duration) {
        try {
            Thread.sleep(duration);
        } catch (Exception e) {
        }
    }

    private void log(Exception e) {
        LOGGER.error(e.getMessage(), e);
    }

    protected void showDDLGraph(String ddl) {
        JdbcUtil.executeSuccess(tddlConnection, "trace " + ddl);
        List<List<String>> trace = new ArrayList<>();
        trace = getTrace(tddlConnection);
        String graphViz = getGraphViz(trace);
        System.out.println("---------------------------------------------");
        System.out.println(ddl);
        System.out.println(graphViz);
    }

    protected String getDDLGraph(Connection connection, String ddl) {
        JdbcUtil.executeSuccess(connection, "trace " + ddl);
        List<List<String>> trace = new ArrayList<>();
        trace = getTrace(connection);
        return getGraphViz(trace);
    }

    protected String getGraphViz(List<List<String>> trace) {
        List<String> lastRow = trace.get(trace.size() - 1);
        return lastRow.get(lastRow.size() - 2);
    }

    public static void assertGraphsHaveSameStructure(String graph1, String graph2) {
        ExecutableDdlJob job1 = parseStructure(graph1);
        ExecutableDdlJob job2 = parseStructure(graph2);
        assertStructureEquals(job1, job2);
    }

    public static void assertStructureEquals(ExecutableDdlJob sourceJob, ExecutableDdlJob targetJob) {
        org.junit.Assert.assertNotNull(sourceJob);
        org.junit.Assert.assertNotNull(targetJob);
        org.junit.Assert.assertEquals(sourceJob.getTaskCount(), targetJob.getTaskCount());

        int sourceBatchCount = 0;
        List<String> sourceExecutionSequence = new ArrayList<>();
        TaskScheduler sourceTaskScheduler = sourceJob.createTaskScheduler();
        while (!sourceTaskScheduler.isAllTaskDone()) {
            List<DdlTask> batch = sourceTaskScheduler.pollBatch();
            batch.sort(Comparator.comparing(DdlTask::getName));
            batch.forEach(t -> {
                sourceTaskScheduler.markAsDone(t);
                sourceExecutionSequence.add(t.getName());
            });
            sourceBatchCount++;
        }
        org.junit.Assert.assertFalse(sourceTaskScheduler.hasMoreExecutable());
        org.junit.Assert.assertFalse(sourceTaskScheduler.hasFailedTask());
        org.junit.Assert.assertNull(sourceTaskScheduler.poll());
        org.junit.Assert.assertTrue(sourceTaskScheduler.pollBatch().isEmpty());

        int targetBatchCount = 0;
        List<String> targetExecutionSequence = new ArrayList<>();
        TaskScheduler targetTaskScheduler = targetJob.createTaskScheduler();
        while (!targetTaskScheduler.isAllTaskDone()) {
            List<DdlTask> batch = targetTaskScheduler.pollBatch();
            batch.sort(Comparator.comparing(DdlTask::getName));
            batch.forEach(t -> {
                targetTaskScheduler.markAsDone(t);
                targetExecutionSequence.add(t.getName());
            });
            targetBatchCount++;
        }
        org.junit.Assert.assertFalse(targetTaskScheduler.hasMoreExecutable());
        org.junit.Assert.assertFalse(targetTaskScheduler.hasFailedTask());
        org.junit.Assert.assertNull(targetTaskScheduler.poll());
        org.junit.Assert.assertTrue(targetTaskScheduler.pollBatch().isEmpty());

        org.junit.Assert.assertEquals(sourceBatchCount, targetBatchCount);
        org.junit.Assert.assertArrayEquals(sourceExecutionSequence.toArray(), targetExecutionSequence.toArray());
    }

    public static ExecutableDdlJob parseStructure(String graphViz) {
        List<String> lines = Splitter.on("\n").splitToList(graphViz);
        if (CollectionUtils.isEmpty(lines)) {
            return null;
        }

        Pattern taskDefPattern = Pattern.compile(".*label=\"\\{(\\w+)\\|taskId:(\\d{19}).*");
        Pattern taskRelPattern = Pattern.compile("(\\w+)\\s+->\\s+(\\w+)");

        Map<Long, TestNamedDdlTask> taskMap = new HashMap<>();

        ExecutableDdlJob job = new ExecutableDdlJob();
        for (String line : lines) {
            line = line.trim();
            if (StringUtils.isEmpty(line)) {
                continue;
            }
            if (StringUtils.containsIgnoreCase(line, "digraph")) {
                continue;
            }
            if (StringUtils.containsIgnoreCase(line, "shape=record")) {
                Matcher matcher = taskDefPattern.matcher(line);
                matcher.find();
                String taskName = matcher.group(1);
                String taskId = matcher.group(2);
                TestNamedDdlTask task = new TestNamedDdlTask(Long.valueOf(taskId), taskName);
                taskMap.put(Long.valueOf(taskId), task);
                job.addTask(task);
            }
            if (StringUtils.containsIgnoreCase(line, "->")) {
                Matcher matcher = taskRelPattern.matcher(line);
                matcher.find();
                String taskIdFrom = matcher.group(1);
                String taskIdTo = matcher.group(2);
                job.addTaskRelationship(
                    taskMap.get(Long.valueOf(taskIdFrom)),
                    taskMap.get(Long.valueOf(taskIdTo))
                );
            }
        }

        return job;
    }

}
