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
import com.alibaba.polardbx.common.statementsummary.model.StatementSummaryByDigest;
import com.alibaba.polardbx.common.statementsummary.model.StatementSummaryElementByDigest;
import com.alibaba.polardbx.common.utils.Assert;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.util.List;
import java.util.Random;

/**
 * @author busu
 * date: 2021/11/10 4:40 下午
 */
public class StatementSummaryByDigestTest {

    public static final String SCHEMA = "schema";
    public static final int TEMPLATE_HASH = 1;
    public static final int PLAN_HASH = 2;
    public static final String SQL_SAMPLE = "sql sample";
    public static final String SQL_TEMPLATE_TEXT = "sql template text";
    public static final String SQL_TYPE = "sqlType";
    public static final int PREV_TEMPLATE_HASH = 3;
    public static final String PREV_SAMPLE_SQL = "prev sample sql";
    private static final String SAMPLE_TRACE_ID = "sample trace id";
    private static final String WORKLOAD_TYPE = "workload type";
    private static final String EXECUTE_MODE = "execute mode";

    @Test
    public void testCreateDefault() {
        StatementSummaryByDigest statementSummaryByDigest = new StatementSummaryByDigest();
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getSchema(), ""));
        Assert.assertTrue(statementSummaryByDigest.getTemplateHash() == 0);
        Assert.assertTrue(statementSummaryByDigest.getPlanHash() == 0);
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getSqlTemplateText(), ""));
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getSqlType(), ""));
        Assert.assertTrue(statementSummaryByDigest.getPrevTemplateHash() == 0);
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getPrevSqlTemplate(), ""));
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getSampleTraceId(), ""));
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getWorkloadType(), ""));
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getExecuteMode(), ""));
        Assert.assertTrue(statementSummaryByDigest.getData().length == 1);
        Assert.assertTrue(statementSummaryByDigest.getSize() == 0);
        Assert.assertTrue(statementSummaryByDigest.getEndIndex() == 0);
        Assert.assertTrue(statementSummaryByDigest.getCapacity() == 1);
    }

    @Test
    public void testCreateOne() {
        StatementSummaryByDigest statementSummaryByDigest =
            new StatementSummaryByDigest(SCHEMA, TEMPLATE_HASH, PLAN_HASH, SQL_SAMPLE, SQL_TEMPLATE_TEXT, SQL_TYPE,
                PREV_TEMPLATE_HASH, PREV_SAMPLE_SQL, SAMPLE_TRACE_ID, WORKLOAD_TYPE, EXECUTE_MODE);
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getSchema(), SCHEMA));
        Assert.assertTrue(statementSummaryByDigest.getTemplateHash() == TEMPLATE_HASH);
        Assert.assertTrue(statementSummaryByDigest.getPlanHash() == PLAN_HASH);
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getSqlTemplateText(), SQL_TEMPLATE_TEXT));
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getSqlType(), SQL_TYPE));
        Assert.assertTrue(statementSummaryByDigest.getPrevTemplateHash() == PREV_TEMPLATE_HASH);
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getPrevSqlTemplate(), PREV_SAMPLE_SQL));
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getSampleTraceId(), SAMPLE_TRACE_ID));
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getWorkloadType(), WORKLOAD_TYPE));
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getExecuteMode(), EXECUTE_MODE));
        Assert.assertTrue(statementSummaryByDigest.getData().length == 1);
        Assert.assertTrue(statementSummaryByDigest.getSize() == 0);
        Assert.assertTrue(statementSummaryByDigest.getEndIndex() == 0);
        Assert.assertTrue(statementSummaryByDigest.getCapacity() == 1);
    }

    @Test
    public void testAddOneExecInfo() {
        StatementSummaryByDigest statementSummaryByDigest =
            new StatementSummaryByDigest(SCHEMA, TEMPLATE_HASH, PLAN_HASH, SQL_SAMPLE, SQL_TEMPLATE_TEXT, SQL_TYPE,
                PREV_TEMPLATE_HASH, PREV_SAMPLE_SQL, SAMPLE_TRACE_ID, WORKLOAD_TYPE, EXECUTE_MODE);
        ExecInfo execInfo = new ExecInfo();
        execInfo.setTimestamp(1636537498974L);
        execInfo.setSchema(SCHEMA);
        execInfo.setSqlType(SQL_TYPE);
        execInfo.setSampleSql(SQL_SAMPLE);
        execInfo.setPrevTemplateText(PREV_SAMPLE_SQL);
        execInfo.setSampleTraceId(SAMPLE_TRACE_ID);
        execInfo.setWorkloadType(WORKLOAD_TYPE);
        execInfo.setExecuteMode(EXECUTE_MODE);
        execInfo.setTemplateText(SQL_TEMPLATE_TEXT);
        execInfo.setTemplateHash(TEMPLATE_HASH);
        execInfo.setPrevTemplateHash(PREV_TEMPLATE_HASH);
        execInfo.setPlanHash(PLAN_HASH);
        execInfo.setErrorCount(0);
        execInfo.setAffectedRows(1);
        execInfo.setTransTime(2);
        execInfo.setResponseTime(3);
        execInfo.setPhysicalTime(10);
        execInfo.setPhysicalExecCount(5);
        int historySize = 16;
        int refreshInterval = 20;
        int addCount = 10;
        for (int i = 0; i < addCount; ++i) {
            statementSummaryByDigest.add(execInfo, historySize, refreshInterval);
        }
        List<StatementSummaryElementByDigest> statementSummaryElementByDigests = Lists.newArrayList();
        for (StatementSummaryElementByDigest statementSummaryElementByDigest : statementSummaryByDigest) {
            statementSummaryElementByDigests.add(statementSummaryElementByDigest);
        }

        Assert.assertTrue(statementSummaryElementByDigests.size() == 1);
        StatementSummaryElementByDigest statementSummaryElementByDigest = statementSummaryElementByDigests.get(0);
        Assert.assertTrue(
            statementSummaryElementByDigest.getBeginTime() == 1636537498974L / refreshInterval * refreshInterval);
        Assert.assertTrue(statementSummaryElementByDigest.getFirstSeen() == 1636537498974L);
        Assert.assertTrue(statementSummaryElementByDigest.getLastSeen() == 1636537498974L);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxAffectedRows() == 1);
        Assert.assertTrue(statementSummaryElementByDigest.getSumAffectedRows() == addCount);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxPhysicalExecCount() == 5);
        Assert.assertTrue(statementSummaryElementByDigest.getSumPhysicalExecCount() == addCount * 5);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxPhysicalTime() == 2);
        Assert.assertTrue(statementSummaryElementByDigest.getSumPhysicalTime() == 10 * addCount);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxResponseTime() == 3);
        Assert.assertTrue(statementSummaryElementByDigest.getSumResponseTime() == 3 * addCount);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxTransTime() == 2);
        Assert.assertTrue(statementSummaryElementByDigest.getSumTransTime() == 2 * addCount);
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getSchema(), SCHEMA));
        Assert.assertTrue(statementSummaryByDigest.getTemplateHash() == TEMPLATE_HASH);
        Assert.assertTrue(statementSummaryByDigest.getPlanHash() == PLAN_HASH);
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getSqlTemplateText(), SQL_TEMPLATE_TEXT));
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getSqlType(), SQL_TYPE));
        Assert.assertTrue(statementSummaryByDigest.getPrevTemplateHash() == PREV_TEMPLATE_HASH);
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getPrevSqlTemplate(), PREV_SAMPLE_SQL));
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getSampleTraceId(), SAMPLE_TRACE_ID));
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getWorkloadType(), WORKLOAD_TYPE));
        Assert.assertTrue(StringUtils.equals(statementSummaryByDigest.getExecuteMode(), EXECUTE_MODE));
        Assert.assertTrue(statementSummaryByDigest.getData().length == historySize + 1);
        Assert.assertTrue(statementSummaryByDigest.getSize() == 1);
        Assert.assertTrue(statementSummaryByDigest.getEndIndex() == 1);
        Assert.assertTrue(statementSummaryByDigest.getCapacity() == 17);

        execInfo.setErrorCount(1);
        execInfo.setAffectedRows(11);
        execInfo.setTransTime(21);
        execInfo.setResponseTime(31);
        execInfo.setPhysicalTime(5100);
        execInfo.setPhysicalExecCount(51);
        statementSummaryByDigest.add(execInfo, historySize, refreshInterval);
        Assert.assertTrue(
            statementSummaryElementByDigest.getBeginTime() == 1636537498974L / refreshInterval * refreshInterval);
        Assert.assertTrue(statementSummaryElementByDigest.getFirstSeen() == 1636537498974L);
        Assert.assertTrue(statementSummaryElementByDigest.getLastSeen() == 1636537498974L);
        Assert.assertTrue(statementSummaryElementByDigest.getErrorCount() == 1);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxAffectedRows() == 11);
        Assert.assertTrue(statementSummaryElementByDigest.getSumAffectedRows() == addCount + 11);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxPhysicalExecCount() == 51);
        Assert.assertTrue(statementSummaryElementByDigest.getSumPhysicalExecCount() == addCount * 5 + 51);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxPhysicalTime() == 100);
        Assert.assertTrue(statementSummaryElementByDigest.getSumPhysicalTime() == 10 * addCount + 5100);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxResponseTime() == 31);
        Assert.assertTrue(statementSummaryElementByDigest.getSumResponseTime() == 3 * addCount + 31);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxTransTime() == 21);
        Assert.assertTrue(statementSummaryElementByDigest.getSumTransTime() == 2 * addCount + 21);
    }

    @Test
    public void testElementMerge() {
        StatementSummaryElementByDigest statementSummaryElementByDigest = new StatementSummaryElementByDigest();
        Random random = new Random();
        List<Long> values = Lists.newArrayList();
        for (int i = 0; i < 15; ++i) {
            values.add(Long.valueOf(random.nextInt()));
        }
        statementSummaryElementByDigest.setBeginTime(values.get(0));
        statementSummaryElementByDigest.setCount(values.get(1));
        statementSummaryElementByDigest.setErrorCount(values.get(2));
        statementSummaryElementByDigest.setMaxAffectedRows(values.get(3));
        statementSummaryElementByDigest.setSumAffectedRows(values.get(4));
        statementSummaryElementByDigest.setMaxTransTime(values.get(5));
        statementSummaryElementByDigest.setSumTransTime(values.get(6));
        statementSummaryElementByDigest.setMaxResponseTime(values.get(7));
        statementSummaryElementByDigest.setSumResponseTime(values.get(8));
        statementSummaryElementByDigest.setMaxPhysicalTime(values.get(9));
        statementSummaryElementByDigest.setSumPhysicalTime(values.get(10));
        statementSummaryElementByDigest.setSumPhysicalExecCount(values.get(11));
        statementSummaryElementByDigest.setMaxPhysicalExecCount(values.get(12));
        statementSummaryElementByDigest.setFirstSeen(values.get(13));
        statementSummaryElementByDigest.setLastSeen(values.get(14));

        StatementSummaryElementByDigest statementSummaryElementByDigest1 = new StatementSummaryElementByDigest();
        statementSummaryElementByDigest1.setCount(values.get(1) + 1);
        statementSummaryElementByDigest1.setErrorCount(values.get(2) + 1);
        statementSummaryElementByDigest1.setMaxAffectedRows(values.get(3) + 1);
        statementSummaryElementByDigest1.setSumAffectedRows(values.get(4) + 1);
        statementSummaryElementByDigest1.setMaxTransTime(values.get(5) + 1);
        statementSummaryElementByDigest1.setSumTransTime(values.get(6) + 1);
        statementSummaryElementByDigest1.setMaxResponseTime(values.get(7) + 1);
        statementSummaryElementByDigest1.setSumResponseTime(values.get(8) + 1);
        statementSummaryElementByDigest1.setMaxPhysicalTime(values.get(9) + 1);
        statementSummaryElementByDigest1.setSumPhysicalTime(values.get(10) + 1);
        statementSummaryElementByDigest1.setSumPhysicalExecCount(values.get(11) + 1);
        statementSummaryElementByDigest1.setMaxPhysicalExecCount(values.get(12) + 1);
        statementSummaryElementByDigest1.setFirstSeen(values.get(13) + 1);
        statementSummaryElementByDigest1.setLastSeen(values.get(14) + 1);

        statementSummaryElementByDigest.merge(statementSummaryElementByDigest1);
        Assert.assertTrue(statementSummaryElementByDigest.getBeginTime() == values.get(0));
        Assert.assertTrue(statementSummaryElementByDigest.getCount() == values.get(1) * 2 + 1);
        Assert.assertTrue(statementSummaryElementByDigest.getErrorCount() == values.get(2) * 2 + 1);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxAffectedRows() == values.get(3) + 1);
        Assert.assertTrue(statementSummaryElementByDigest.getSumAffectedRows() == values.get(4) * 2 + 1);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxTransTime() == values.get(5) + 1);
        Assert.assertTrue(statementSummaryElementByDigest.getSumTransTime() == values.get(6) * 2 + 1);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxResponseTime() == values.get(7) + 1);
        Assert.assertTrue(statementSummaryElementByDigest.getSumResponseTime() == values.get(8) * 2 + 1);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxPhysicalTime() == values.get(9) + 1);
        Assert.assertTrue(statementSummaryElementByDigest.getSumPhysicalTime() == values.get(10) * 2 + 1);
        Assert.assertTrue(statementSummaryElementByDigest.getSumPhysicalExecCount() == values.get(11) * 2 + 1);
        Assert.assertTrue(statementSummaryElementByDigest.getMaxPhysicalExecCount() == values.get(12) + 1);
        Assert.assertTrue(statementSummaryElementByDigest.getFirstSeen() == values.get(13));
        Assert.assertTrue(statementSummaryElementByDigest.getLastSeen() == values.get(14) + 1);
    }

    @Test
    public void testAddExecInfo() {
        StatementSummaryByDigest statementSummaryByDigest =
            new StatementSummaryByDigest(SCHEMA, TEMPLATE_HASH, PLAN_HASH, SQL_SAMPLE, SQL_TEMPLATE_TEXT, SQL_TYPE,
                PREV_TEMPLATE_HASH, PREV_SAMPLE_SQL, SAMPLE_TRACE_ID, WORKLOAD_TYPE, EXECUTE_MODE);
        ExecInfo execInfo = new ExecInfo();
        execInfo.setTimestamp(1636537498974L);
        execInfo.setSchema(SCHEMA);
        execInfo.setSqlType(SQL_TYPE);
        execInfo.setSampleSql(SQL_SAMPLE);
        execInfo.setPrevTemplateText(PREV_SAMPLE_SQL);
        execInfo.setSampleTraceId(SAMPLE_TRACE_ID);
        execInfo.setWorkloadType(WORKLOAD_TYPE);
        execInfo.setExecuteMode(EXECUTE_MODE);
        execInfo.setTemplateText(SQL_TEMPLATE_TEXT);
        execInfo.setTemplateHash(TEMPLATE_HASH);
        execInfo.setPrevTemplateHash(PREV_TEMPLATE_HASH);
        execInfo.setPlanHash(PLAN_HASH);
        execInfo.setErrorCount(0);
        execInfo.setAffectedRows(1);
        execInfo.setTransTime(2);
        execInfo.setResponseTime(3);
        execInfo.setPhysicalTime(4);
        execInfo.setPhysicalExecCount(5);
        int historySize = 16;
        int refreshInterval = 20;
        int addCount = 50;
        for (int i = 0; i < addCount; ++i) {
            statementSummaryByDigest.add(execInfo, historySize, refreshInterval);
            execInfo.setTimestamp(execInfo.getTimestamp() + refreshInterval);
        }
        long preBeginTime = 0;
        for (StatementSummaryElementByDigest statementSummaryElementByDigest : statementSummaryByDigest) {
            if (preBeginTime == 0) {
                preBeginTime = statementSummaryElementByDigest.getBeginTime();
                continue;
            }
            Assert.assertTrue(statementSummaryElementByDigest.getBeginTime() - preBeginTime == refreshInterval);
            preBeginTime = statementSummaryElementByDigest.getBeginTime();
        }

        refreshInterval = 21;
        for (int i = 0; i < addCount; ++i) {
            statementSummaryByDigest.add(execInfo, historySize, refreshInterval);
            execInfo.setTimestamp(execInfo.getTimestamp() + refreshInterval);
        }
        preBeginTime = 0;
        for (StatementSummaryElementByDigest statementSummaryElementByDigest : statementSummaryByDigest) {
            if (preBeginTime == 0) {
                preBeginTime = statementSummaryElementByDigest.getBeginTime();
                continue;
            }
            Assert.assertTrue(statementSummaryElementByDigest.getBeginTime() - preBeginTime == refreshInterval);
            preBeginTime = statementSummaryElementByDigest.getBeginTime();
        }
    }

    @Test
    public void testAddEarlyExecInfo() {
        StatementSummaryByDigest statementSummaryByDigest =
            new StatementSummaryByDigest(SCHEMA, TEMPLATE_HASH, PLAN_HASH, SQL_SAMPLE, SQL_TEMPLATE_TEXT, SQL_TYPE,
                PREV_TEMPLATE_HASH, PREV_SAMPLE_SQL, SAMPLE_TRACE_ID, WORKLOAD_TYPE, EXECUTE_MODE);
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
        int historySize = 16;
        int refreshInterval = 20;
        statementSummaryByDigest.add(execInfo, historySize, refreshInterval);
        execInfo.setTimestamp(1636537498974L + refreshInterval);
        statementSummaryByDigest.add(execInfo, historySize, refreshInterval);
        execInfo.setTimestamp(1636537498974L);
        statementSummaryByDigest.add(execInfo, historySize, refreshInterval);
        Assert.assertTrue(statementSummaryByDigest.iterator().next().getCount() == 2);
    }

    @Test
    public void testRandomAddExecInfo() {
        StatementSummaryByDigest statementSummaryByDigest =
            new StatementSummaryByDigest(SCHEMA, TEMPLATE_HASH, PLAN_HASH, SQL_SAMPLE, SQL_TEMPLATE_TEXT, SQL_TYPE,
                PREV_TEMPLATE_HASH, PREV_SAMPLE_SQL, SAMPLE_TRACE_ID, WORKLOAD_TYPE, EXECUTE_MODE);
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
        int historySize = 16;
        int refreshInterval = 21;
        int addCount = 50;
        Random rand = new Random();
        for (int i = 0; i < addCount; ++i) {
            execInfo.setErrorCount(rand.nextInt());
            execInfo.setAffectedRows(rand.nextInt());
            execInfo.setTransTime(rand.nextInt());
            execInfo.setResponseTime(rand.nextInt());
            execInfo.setPhysicalTime(rand.nextInt());
            execInfo.setPhysicalExecCount(rand.nextInt());
            statementSummaryByDigest
                .add(execInfo, rand.nextInt(historySize), Math.max(rand.nextInt(refreshInterval), 1));
            execInfo.setTimestamp(execInfo.getTimestamp() + rand.nextInt(refreshInterval));
        }
        long preBeginTime = 0;
        for (StatementSummaryElementByDigest statementSummaryElementByDigest : statementSummaryByDigest) {
            if (preBeginTime == 0) {
                preBeginTime = statementSummaryElementByDigest.getBeginTime();
                continue;
            }
            Assert.assertTrue(statementSummaryElementByDigest.getBeginTime() - preBeginTime > 0);
            preBeginTime = statementSummaryElementByDigest.getBeginTime();
        }
    }

    @Test
    public void testMerge() {
        StatementSummaryByDigest statementSummaryByDigest =
            new StatementSummaryByDigest(SCHEMA, TEMPLATE_HASH, PLAN_HASH, SQL_SAMPLE, SQL_TEMPLATE_TEXT, SQL_TYPE,
                PREV_TEMPLATE_HASH, PREV_SAMPLE_SQL, SAMPLE_TRACE_ID, WORKLOAD_TYPE, EXECUTE_MODE);
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
        int historySize = 16;
        int refreshInterval = 21;
        int addCount = 50;
        Random rand = new Random();
        for (int i = 0; i < addCount; ++i) {
            execInfo.setErrorCount(1);
            execInfo.setAffectedRows(rand.nextInt());
            execInfo.setTransTime(rand.nextInt());
            execInfo.setResponseTime(rand.nextInt());
            execInfo.setPhysicalTime(rand.nextInt());
            execInfo.setPhysicalExecCount(rand.nextInt());
            statementSummaryByDigest
                .add(execInfo, rand.nextInt(historySize), Math.max(rand.nextInt(refreshInterval), 1));
            execInfo.setTimestamp(execInfo.getTimestamp() + rand.nextInt(refreshInterval));
            statementSummaryByDigest.merge(statementSummaryByDigest, rand.nextInt(historySize));
        }
    }

}
