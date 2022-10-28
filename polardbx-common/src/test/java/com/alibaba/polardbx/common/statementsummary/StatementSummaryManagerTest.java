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

package com.alibaba.polardbx.common.statementsummary;

import com.alibaba.polardbx.common.statementsummary.model.ExecInfo;
import com.alibaba.polardbx.common.statementsummary.model.StatementSummaryByDigestEntry;
import com.alibaba.polardbx.common.statementsummary.model.StatementSummaryElementByDigest;
import com.alibaba.polardbx.common.utils.Assert;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 * @author busu
 * date: 2021/11/10 4:40 下午
 */
public class StatementSummaryManagerTest {

    public static final String SCHEMA = "schema";
    public static final int TEMPLATE_HASH = 1;
    public static final int PLAN_HASH = 2;
    public static final String SQL_SAMPLE = "sql sample";
    public static final String SQL_TEMPLATE_TEXT = "sql template text";
    public static final String SQL_TYPE = "sqlType";
    public static final int PREV_TEMPLATE_HASH = 3;
    public static final String PREV_SAMPLE_SQL = "prev sample sql";
    public static final String SAMPLE_TRACE_ID = "sample trace id";
    public static final String WORKLOAD_TYPE = "workload type";
    public static final String EXECUTE_MODE = "execute mode";

    private StatementSummaryManager statementSummaryManager = StatementSummaryManager.getInstance();

    @Before
    public void before() {
        statementSummaryManager.getConfig().setConfig(1, -1, -1, -1, -1, -1);
        statementSummaryManager.getConfig().setStmtSummaryPercent(100);
    }

    @After
    public void after() {
        statementSummaryManager.getConfig().setConfig(0, -1, -1, -1, -1, -1);
        statementSummaryManager.getConfig().setStmtSummaryPercent(1);
    }

    @Test
    public void testStmtSummaryOnlySlow() throws ExecutionException {
        ExecInfo execInfo = new ExecInfo();
        execInfo.setTimestamp(1636537498974L);
        execInfo.setSchema(SCHEMA);
        execInfo.setSqlType(SQL_TYPE);
        execInfo.setSampleSql(SQL_SAMPLE);
        execInfo.setPrevTemplateText(PREV_SAMPLE_SQL);
        execInfo.setTemplateText(SQL_TEMPLATE_TEXT);
        execInfo.setSampleTraceId(SAMPLE_TRACE_ID);
        execInfo.setWorkloadType(WORKLOAD_TYPE);
        execInfo.setExecuteMode(EXECUTE_MODE);
        execInfo.setTemplateHash(TEMPLATE_HASH);
        execInfo.setPrevTemplateHash(PREV_TEMPLATE_HASH);
        execInfo.setPlanHash(PLAN_HASH);
        execInfo.setErrorCount(0);
        execInfo.setAffectedRows(1);
        execInfo.setTransTime(2);
        execInfo.setResponseTime(3);
        execInfo.setPhysicalTime(4);
        execInfo.setPhysicalExecCount(5);
        execInfo.setParseTime(1);
        execInfo.setExecPlanCpuTime(1);
        execInfo.setPhyFetchRows(1);
        statementSummaryManager.getConfig().setStmtSummaryPercent(0);
        for (int i = 0; i < 1000; ++i) {
            statementSummaryManager.summaryStmt(execInfo);
        }
        execInfo.setSlow(true);
        statementSummaryManager.summaryStmt(execInfo);
        Iterator<StatementSummaryByDigestEntry> iterator =
            statementSummaryManager.getCurrentStmtSummaries(1636537498974L);
        Assert.assertTrue(iterator.hasNext());
        long queryCount = iterator.next().getValue().getCount();
        Assert.assertTrue(queryCount == 1);
    }

    @Test
    public void testStmtSummaryPercent() throws ExecutionException {
        ExecInfo execInfo = new ExecInfo();
        execInfo.setTimestamp(1636537498974L);
        execInfo.setSchema(SCHEMA);
        execInfo.setSqlType(SQL_TYPE);
        execInfo.setSampleSql(SQL_SAMPLE);
        execInfo.setPrevTemplateText(PREV_SAMPLE_SQL);
        execInfo.setTemplateText(SQL_TEMPLATE_TEXT);
        execInfo.setSampleTraceId(SAMPLE_TRACE_ID);
        execInfo.setWorkloadType(WORKLOAD_TYPE);
        execInfo.setExecuteMode(EXECUTE_MODE);
        execInfo.setTemplateHash(TEMPLATE_HASH);
        execInfo.setPrevTemplateHash(PREV_TEMPLATE_HASH);
        execInfo.setPlanHash(PLAN_HASH);
        execInfo.setErrorCount(0);
        execInfo.setAffectedRows(1);
        execInfo.setTransTime(2);
        execInfo.setResponseTime(3);
        execInfo.setPhysicalTime(4);
        execInfo.setPhysicalExecCount(5);
        execInfo.setParseTime(1);
        execInfo.setExecPlanCpuTime(1);
        execInfo.setPhyFetchRows(1);
        statementSummaryManager.getConfig().setStmtSummaryPercent(1);
        for (int i = 0; i < 10000; ++i) {
            statementSummaryManager.summaryStmt(execInfo);
        }
        Iterator<StatementSummaryByDigestEntry> iterator =
            statementSummaryManager.getCurrentStmtSummaries(1636537498974L);
        Assert.assertTrue(iterator.hasNext());
        long queryCount = iterator.next().getValue().getCount();
        Assert.assertTrue(queryCount > 50L && queryCount < 150L);
    }

    @Test
    public void testSummaryStmt() throws ExecutionException {
        ExecInfo execInfo = new ExecInfo();
        execInfo.setTimestamp(1636537498974L);
        execInfo.setSchema(SCHEMA);
        execInfo.setSqlType(SQL_TYPE);
        execInfo.setSampleSql(SQL_SAMPLE);
        execInfo.setPrevTemplateText(PREV_SAMPLE_SQL);
        execInfo.setTemplateText(SQL_TEMPLATE_TEXT);
        execInfo.setSampleTraceId(SAMPLE_TRACE_ID);
        execInfo.setWorkloadType(WORKLOAD_TYPE);
        execInfo.setExecuteMode(EXECUTE_MODE);
        execInfo.setTemplateHash(TEMPLATE_HASH);
        execInfo.setPrevTemplateHash(PREV_TEMPLATE_HASH);
        execInfo.setPlanHash(PLAN_HASH);
        execInfo.setErrorCount(0);
        execInfo.setAffectedRows(1);
        execInfo.setTransTime(2);
        execInfo.setResponseTime(3);
        execInfo.setPhysicalTime(4);
        execInfo.setPhysicalExecCount(5);
        execInfo.setParseTime(1);
        execInfo.setExecPlanCpuTime(1);
        execInfo.setPhyFetchRows(1);
        statementSummaryManager.getConfig().setConfig(1, -1, -1, 10, -1, -1);
        for (int i = 0; i < 3000; ++i) {
            execInfo.setTemplateHash(i);
            statementSummaryManager.summaryStmt(execInfo);
            execInfo.setTimestamp(
                execInfo.getTimestamp() + statementSummaryManager.getConfig().getStmtSummaryRefreshInterval() / 10);
        }
        Assert.assertTrue(statementSummaryManager.getCachedStatementSummaryByDigests().size() == 10);
        Iterator<StatementSummaryElementByDigest> iterator =
            statementSummaryManager.getOtherStatementSummaryByDigest().iterator();
        iterator.next();
        Assert.assertTrue(iterator.next().getCount() == 10);
    }

    @Test
    public void testGetCurrentStatementSummaries() throws ExecutionException {
        randomFillData(false);
        Iterator<StatementSummaryByDigestEntry> iterator =
            statementSummaryManager.getCurrentStmtSummaries(1636537498974L);
        Assert.assertTrue(iterator != null);
        List<StatementSummaryByDigestEntry> entryList = Lists.newArrayList();
        if (iterator != null) {
            while (iterator.hasNext()) {
                entryList.add(iterator.next());
            }
        }
        Assert.assertTrue(entryList.size() == 11);
    }

    @Test
    public void testGetHistoryStatementsSummaries() throws ExecutionException {
        randomFillData(true);
        Iterator<StatementSummaryByDigestEntry> iterator =
            statementSummaryManager.getStmtHistorySummaries(1636537498974L);
        Assert.assertTrue(iterator != null);
        List<StatementSummaryByDigestEntry> entryList = Lists.newArrayList();
        if (iterator != null) {
            while (iterator.hasNext()) {
                entryList.add(iterator.next());
            }
        }
        Assert.assertTrue(entryList.size() == 34);
    }

    private void randomFillData(boolean incrementTimestamp) throws ExecutionException {
        ExecInfo execInfo = new ExecInfo();
        execInfo.setTimestamp(1636537498974L);
        execInfo.setSchema(SCHEMA);
        execInfo.setSqlType(SQL_TYPE);
        execInfo.setSampleSql(SQL_SAMPLE);
        execInfo.setPrevTemplateText(PREV_SAMPLE_SQL);
        execInfo.setTemplateText(SQL_TEMPLATE_TEXT);
        execInfo.setSampleTraceId(SAMPLE_TRACE_ID);
        execInfo.setWorkloadType(WORKLOAD_TYPE);
        execInfo.setExecuteMode(EXECUTE_MODE);
        execInfo.setTemplateHash(TEMPLATE_HASH);
        execInfo.setPrevTemplateHash(PREV_TEMPLATE_HASH);
        execInfo.setPlanHash(PLAN_HASH);
        execInfo.setErrorCount(0);
        execInfo.setParseTime(1);
        execInfo.setExecPlanCpuTime(1);
        execInfo.setPhyFetchRows(1);
        execInfo.setAffectedRows(1);
        execInfo.setTransTime(2);
        execInfo.setResponseTime(3);
        execInfo.setPhysicalTime(4);
        execInfo.setPhysicalExecCount(5);
        statementSummaryManager.getConfig().setConfig(1, -1, -1, 10, -1, -1);
        for (int i = 0; i < 3000; ++i) {
            execInfo.setTemplateHash(i);
            statementSummaryManager.summaryStmt(execInfo);
            if (incrementTimestamp) {
                execInfo.setTimestamp(
                    execInfo.getTimestamp() + statementSummaryManager.getConfig().getStmtSummaryRefreshInterval() / 10);
            }
        }
    }

    @Test
    public void testMultiThread() throws ExecutionException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread thread = new Thread() {
            @SneakyThrows
            @Override
            public void run() {
                try {
                    Random rand = new Random();
                    for (int i = 0; i < 1000; ++i) {
                        statementSummaryManager.getConfig()
                            .setConfig(1, 10, 10,
                                rand.nextInt(10) + 5,
                                rand.nextInt(1), -1);
                        Thread.sleep(1);
                    }
                } finally {
                    countDownLatch.countDown();
                }

            }
        };
        thread.start();
        while (countDownLatch.getCount() == 1) {
            ExecInfo execInfo = new ExecInfo();
            execInfo.setTimestamp(1636537498974L);
            execInfo.setSchema(SCHEMA);
            execInfo.setSqlType(SQL_TYPE);
            execInfo.setSampleSql(SQL_SAMPLE);
            execInfo.setPrevTemplateText(PREV_SAMPLE_SQL);
            execInfo.setTemplateText(SQL_TEMPLATE_TEXT);
            execInfo.setSampleTraceId(SAMPLE_TRACE_ID);
            execInfo.setWorkloadType(WORKLOAD_TYPE);
            execInfo.setExecuteMode(EXECUTE_MODE);
            execInfo.setTemplateHash(TEMPLATE_HASH);
            execInfo.setPrevTemplateHash(PREV_TEMPLATE_HASH);
            execInfo.setPlanHash(PLAN_HASH);
            execInfo.setErrorCount(0);
            execInfo.setParseTime(1);
            execInfo.setExecPlanCpuTime(1);
            execInfo.setPhyFetchRows(1);
            execInfo.setAffectedRows(1);
            execInfo.setTransTime(2);
            execInfo.setResponseTime(3);
            execInfo.setPhysicalTime(4);
            execInfo.setPhysicalExecCount(5);
            Random random = new Random();
            for (int i = 0; i < 3000; ++i) {
                execInfo.setTemplateHash(random.nextInt(10));
                statementSummaryManager.summaryStmt(execInfo);
                execInfo.setTimestamp(
                    execInfo.getTimestamp() + statementSummaryManager.getConfig().getStmtSummaryRefreshInterval());
            }
        }
    }

    @Test
    public void testOpenCloseStmtSummary() throws NoSuchFieldException, IllegalAccessException {
        statementSummaryManager.getConfig().setConfig(1, 10, 10, 5, 10, -1);
        Field stmtCacheField = StatementSummaryManager.class.getDeclaredField("stmtCache");
        stmtCacheField.setAccessible(true);
        Assert.assertTrue(stmtCacheField.get(statementSummaryManager) != null);
        Field otherField = StatementSummaryManager.class.getDeclaredField("other");
        otherField.setAccessible(true);
        Assert.assertTrue(otherField.get(statementSummaryManager) != null);
        statementSummaryManager.getConfig().setConfig(0, 10, 10, 5, 10, -1);
        Assert.assertTrue(stmtCacheField.get(statementSummaryManager) == null);
        Assert.assertTrue(otherField.get(statementSummaryManager) == null);
    }

}
