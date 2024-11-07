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

package com.alibaba.polardbx.parser;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.executor.sync.BaselineDeleteHotEvolvedSyncAction;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.sync.BaselineDeleteSyncAction;
import com.alibaba.polardbx.executor.sync.BaselineInvalidatePlanSyncAction;
import com.alibaba.polardbx.executor.sync.BaselineInvalidateSchemaSyncAction;
import com.alibaba.polardbx.executor.sync.BaselineUpdateSyncAction;
import com.alibaba.polardbx.executor.sync.DeleteBaselineSyncAction;
import com.alibaba.polardbx.executor.sync.FetchSPMSyncAction;
import com.alibaba.polardbx.executor.sync.MetricSyncAction;
import com.alibaba.polardbx.executor.sync.RemoveColumnStatisticSyncAction;
import com.alibaba.polardbx.executor.sync.RemoveTableStatisticSyncAction;
import com.alibaba.polardbx.executor.sync.RenameStatisticSyncAction;
import com.alibaba.polardbx.executor.sync.RenameTableSyncAction;
import com.alibaba.polardbx.executor.sync.UpdateStatisticSyncAction;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.Histogram;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticFeedbackSyncAction;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.server.response.ShowNodeSyncAction;
import com.alibaba.polardbx.server.response.ShowSQLSlowSyncAction;
import com.alibaba.polardbx.stats.metric.FeatureStats;
import com.alibaba.polardbx.stats.metric.FeatureStatsItem;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.alibaba.polardbx.common.utils.Assert.assertTrue;

public class SyncActionTest {

    @Test
    public void testShowSqlJson() {
        ShowSQLSlowSyncAction showNodeSyncAction = new ShowSQLSlowSyncAction();
        String data = JSON.toJSONString(showNodeSyncAction, SerializerFeature.WriteClassName);
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        Object obj = JSON.parse(data);
        Assert.assertEquals(showNodeSyncAction.getClass(), obj.getClass());
    }

    @Test
    public void testShowNodeJson() {
        ShowNodeSyncAction showNodeSyncAction = new ShowNodeSyncAction("Test");
        String data = JSON.toJSONString(showNodeSyncAction, SerializerFeature.WriteClassName);
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        Object obj = JSON.parse(data);
        Assert.assertEquals(showNodeSyncAction.getClass(), obj.getClass());
    }

    @Test
    public void testBaselineInvalidatePlanSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.BaselineInvalidatePlanSyncAction");

        BaselineInvalidatePlanSyncAction action = new BaselineInvalidatePlanSyncAction("Test", "test_table", true);

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        BaselineInvalidatePlanSyncAction obj = (BaselineInvalidatePlanSyncAction) JSON.parse(data);
        Assert.assertEquals(action.isIsForce(), obj.isIsForce());
    }

    @Test
    public void testDeleteBaselineSyncAction1() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.BaselineDeleteSyncAction");
        int baselineId = 123;
        int planId = 1234;
        BaselineDeleteSyncAction action = new BaselineDeleteSyncAction("test1", baselineId);

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        BaselineDeleteSyncAction obj = (BaselineDeleteSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getSchemaName(), obj.getSchemaName());
        Assert.assertEquals(action.getBaselineId(), obj.getBaselineId());
        Assert.assertTrue(0 == obj.getPlanInfoId());

        action.setBaselineId(baselineId);
        action.setPlanInfoId(planId);
        action.setSchemaName("test1");
        data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        obj = (BaselineDeleteSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getSchemaName(), obj.getSchemaName());
        Assert.assertEquals(action.getBaselineId(), obj.getBaselineId());
        Assert.assertEquals(action.getPlanInfoId(), obj.getPlanInfoId());
    }

    @Test
    public void testBaselineInvalidateSchemaSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.BaselineInvalidateSchemaSyncAction");

        BaselineInvalidateSchemaSyncAction action = new BaselineInvalidateSchemaSyncAction("Test");

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        BaselineInvalidateSchemaSyncAction obj = (BaselineInvalidateSchemaSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getSchema(), obj.getSchema());
    }

    @Test
    public void testDeleteBaselineSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.DeleteBaselineSyncAction");

        DeleteBaselineSyncAction action = new DeleteBaselineSyncAction("test1", "test2");

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        DeleteBaselineSyncAction obj = (DeleteBaselineSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getSchemaName(), obj.getSchemaName());
        Assert.assertEquals(action.getParameterSql(), obj.getParameterSql());
        Assert.assertTrue(0 == obj.getPlanInfoId());

        action = new DeleteBaselineSyncAction("test1", "test2", 123);
        data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        obj = (DeleteBaselineSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getSchemaName(), obj.getSchemaName());
        Assert.assertEquals(action.getParameterSql(), obj.getParameterSql());
        Assert.assertEquals(action.getPlanInfoId(), obj.getPlanInfoId());
    }

    @Test
    public void testBaselineDeleteHotEvolvedSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.BaselineDeleteHotEvolvedSyncAction");

        BaselineDeleteHotEvolvedSyncAction action = new BaselineDeleteHotEvolvedSyncAction("Test");

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        BaselineDeleteHotEvolvedSyncAction obj = (BaselineDeleteHotEvolvedSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getSchema(), obj.getSchema());
    }

    @Test
    public void testUpdateStatisticSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.UpdateStatisticSyncAction");

        Histogram h = new Histogram(7, DataTypes.IntegerType, 1);
        Integer[] list = new Integer[10000];
        Random r1 = new Random();
        for (int i = 0; i < list.length; i++) {
            list[i] = r1.nextInt(list.length * 100);
        }
        h.buildFromData(list);

        StatisticManager.CacheLine cacheLine = new StatisticManager.CacheLine();
        cacheLine.setHistogram("test_col", h);
        UpdateStatisticSyncAction action = new UpdateStatisticSyncAction("test1", "test2", cacheLine);

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        UpdateStatisticSyncAction obj = (UpdateStatisticSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getSchemaName(), obj.getSchemaName());
        Assert.assertEquals(action.getLogicalTableName(), obj.getLogicalTableName());
        Assert.assertEquals(action.getJsonString(), obj.getJsonString());
    }

    @Test
    public void testRenameTableSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.RenameTableSyncAction");

        RenameTableSyncAction action = new RenameTableSyncAction("test1", "test2", "test3");

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        RenameTableSyncAction obj = (RenameTableSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getSchemaName(), obj.getSchemaName());
        Assert.assertEquals(action.getSourceTableName(), obj.getSourceTableName());
        Assert.assertEquals(action.getTargetTableName(), obj.getTargetTableName());
    }

    @Test
    public void testRenameStatisticSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.RenameStatisticSyncAction");

        RenameStatisticSyncAction action = new RenameStatisticSyncAction("test1", "test2", "test3");

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        RenameStatisticSyncAction obj = (RenameStatisticSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getSchemaName(), obj.getSchemaName());
        Assert.assertEquals(action.getNewlogicalTableName(), obj.getNewlogicalTableName());
        Assert.assertEquals(action.getOldlogicalTableName(), obj.getOldlogicalTableName());
    }

    @Test
    public void testRemoveTableStatisticSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.RemoveTableStatisticSyncAction");

        RemoveTableStatisticSyncAction action = new RemoveTableStatisticSyncAction("test1", "test2");

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        RemoveTableStatisticSyncAction obj = (RemoveTableStatisticSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getSchemaName(), obj.getSchemaName());
        Assert.assertEquals(action.getLogicalTableName(), obj.getLogicalTableName());
    }

    @Test
    public void testFetchSPMSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.FetchSPMSyncAction");

        FetchSPMSyncAction action = new FetchSPMSyncAction("test1", false);

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        FetchSPMSyncAction obj = (FetchSPMSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getSchemaName(), obj.getSchemaName());
        Assert.assertEquals(action.isWithPlan(), obj.isWithPlan());
    }

    @Test
    public void testBaselineUpdateSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.BaselineUpdateSyncAction");

        Map<String, List<String>> baselineMap = Maps.newConcurrentMap();
        List<String> baselineStrList1 = Lists.newArrayList();

        baselineStrList1.add(BaselineInfo.serializeToJson(buildBaseline(), false));
        baselineStrList1.add(BaselineInfo.serializeToJson(buildBaseline(), false));
        baselineStrList1.add(BaselineInfo.serializeToJson(buildBaseline(), false));
        baselineMap.put("test1", baselineStrList1);

        List<String> baselineStrList2 = Lists.newArrayList();
        baselineStrList2.add(BaselineInfo.serializeToJson(buildBaseline(), false));
        baselineStrList2.add(BaselineInfo.serializeToJson(buildBaseline(), false));

        baselineMap.put("test2", baselineStrList2);

        BaselineUpdateSyncAction action = new BaselineUpdateSyncAction(baselineMap);

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        BaselineUpdateSyncAction obj = (BaselineUpdateSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getBaselineMap(), obj.getBaselineMap());
    }

    @Test
    public void testStatisticFeedbackSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.optimizer.config.table.statistic.StatisticFeedbackSyncAction");

        StatisticFeedbackSyncAction action = new StatisticFeedbackSyncAction("test1", "test2");

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        StatisticFeedbackSyncAction obj = (StatisticFeedbackSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getSchema(), obj.getSchema());
        Assert.assertEquals(action.getTable(), obj.getTable());
    }

    @Test
    public void testMetricSyncAction() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.MetricSyncAction");
        MetricSyncAction metricSyncAction = new MetricSyncAction();
        FeatureStats.getInstance().plus(FeatureStatsItem.FIX_PLAN_NUM, 99);
        FeatureStats.getInstance().plus(FeatureStatsItem.NEW_BASELINE_NUM, 100);
        FeatureStats.getInstance().plus(FeatureStatsItem.HOT_EVOLVE_PLAN_NUM, 105);
        FeatureStats.getInstance().plus(FeatureStatsItem.HLL_TASK_SUCC, 101);
        FeatureStats.getInstance().plus(FeatureStatsItem.HLL_TASK_FAIL, 102);
        FeatureStats.getInstance().plus(FeatureStatsItem.SAMPLE_TASK_SUCC, 103);
        FeatureStats.getInstance().plus(FeatureStatsItem.SAMPLE_TASK_FAIL, 104);

        FeatureStats.getInstance().increment(FeatureStatsItem.FIX_PLAN_NUM);
        FeatureStats.getInstance().increment(FeatureStatsItem.NEW_BASELINE_NUM);
        FeatureStats.getInstance().increment(FeatureStatsItem.HOT_EVOLVE_PLAN_NUM);
        FeatureStats.getInstance().increment(FeatureStatsItem.HLL_TASK_SUCC);
        FeatureStats.getInstance().increment(FeatureStatsItem.HLL_TASK_FAIL);
        FeatureStats.getInstance().increment(FeatureStatsItem.SAMPLE_TASK_SUCC);
        FeatureStats.getInstance().increment(FeatureStatsItem.SAMPLE_TASK_FAIL);
        ResultCursor rc = metricSyncAction.sync();
        String line = rc.doNext().getString(0);
        System.out.println(line);
        assertTrue("{\"longStats\":[101,100,106,104,105,102,103],\"sign\":797014233}".equalsIgnoreCase(line));

        FeatureStats fs = FeatureStats.deserialize(line);
        System.out.println(fs.log());
        Assert.assertTrue(fs.log().equalsIgnoreCase(
            "NEW_BASELINE_NUM:101,FIX_PLAN_NUM:100,HOT_EVOLVE_PLAN_NUM:106,SAMPLE_TASK_SUCC:104,SAMPLE_TASK_FAIL:105,HLL_TASK_SUCC:102,HLL_TASK_FAIL:103"));
    }

    @Test
    public void testBaselineQuerySyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.FetchSPMSyncAction");

        FetchSPMSyncAction action = new FetchSPMSyncAction("test1", false);

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        FetchSPMSyncAction obj = (FetchSPMSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getSchemaName(), obj.getSchemaName());
        Assert.assertEquals(action.isWithPlan(), obj.isWithPlan());
    }

    @Test
    public void testRemoveColumnStatisticSyncAction() {
        ParserConfig parserConfig = ParserConfig.getGlobalInstance();
        parserConfig.setAutoTypeSupport(true);
        ParserConfig.getGlobalInstance()
            .addAccept("com.alibaba.polardbx.executor.sync.FetchSPMSyncAction");

        List<String> colList = Lists.newArrayList();
        colList.add("col1");
        colList.add("col2");
        colList.add("col3");
        RemoveColumnStatisticSyncAction action = new RemoveColumnStatisticSyncAction("test1", "test2", colList);

        String data = JSON.toJSONString(action, SerializerFeature.WriteClassName);
        RemoveColumnStatisticSyncAction obj = (RemoveColumnStatisticSyncAction) JSON.parse(data);
        Assert.assertEquals(action.getSchemaName(), obj.getSchemaName());
        Assert.assertEquals(action.getLogicalTableName(), obj.getLogicalTableName());
        Assert.assertEquals(action.getColumnNameList(), obj.getColumnNameList());
    }

    private BaselineInfo buildBaseline() {
        Random r = new Random();
        BaselineInfo b = new BaselineInfo("test sql", Collections.emptySet());
        int planNum = r.nextInt(3);
        for (int i = 0; i < planNum; i++) {
            PlanInfo p =
                new PlanInfo(r.nextInt(), "", System.currentTimeMillis() / 1000, System.currentTimeMillis() / 1000, 0,
                    1D,
                    1D,
                    true, false, "", "", "", 1);
            p.setId(r.nextInt());
            b.addAcceptedPlan(p);
        }

        return b;
    }
}
