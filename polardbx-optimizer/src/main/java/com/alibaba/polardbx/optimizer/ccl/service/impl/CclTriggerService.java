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

package com.alibaba.polardbx.optimizer.ccl.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.optimizer.ccl.common.CclCondition;
import com.alibaba.polardbx.optimizer.ccl.common.CclSqlMetric;
import com.alibaba.polardbx.optimizer.ccl.common.CclSqlMetricChecker;
import com.alibaba.polardbx.optimizer.ccl.common.CclTriggerInfo;
import com.alibaba.polardbx.optimizer.ccl.service.ICclConfigService;
import com.alibaba.polardbx.optimizer.ccl.service.ICclTriggerService;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleAccessor;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleRecord;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerAccessor;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.utils.CclUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author busu
 * date: 2021/4/6 2:17 下午
 */
public class CclTriggerService implements ICclTriggerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CclTriggerService.class);

    private final static String CCL_RULE_NAME_FORMAT = "AUTO_%s_%s_%s_%s";

    public final static int TRIGGER_TASK_QUEUE_SIZE = 20;

    private final AtomicInteger triggerTaskCount = new AtomicInteger(0);

    private final Queue<TriggerTask> taskQueue = new ConcurrentLinkedQueue<>();

    private final ICclConfigService cclConfigService;

    private volatile boolean working;

    public CclTriggerService(ICclConfigService cclConfigService) {
        this.cclConfigService = cclConfigService;
    }

    @Override
    public boolean offerSample(CclSqlMetric cclSqlMetric, boolean hasBound) {
        if (hasBound && triggerTaskCount.get() >= TRIGGER_TASK_QUEUE_SIZE) {
            return false;
        }
        List<CclTriggerInfo> cclTriggerInfos = cclConfigService.getCclTriggerInfos();
        boolean success = false;
        try {
            out:
            for (CclTriggerInfo cclTriggerInfo : cclTriggerInfos) {
                if (StringUtils.length(cclSqlMetric.getOriginalSql()) > cclTriggerInfo.getTriggerRecord().maxSQLSize) {
                    continue;
                }

                if (!StringUtils.equals(cclTriggerInfo.getTriggerRecord().schema, "*") && !StringUtils
                    .equals(cclSqlMetric.getSchemaName(), cclTriggerInfo.getTriggerRecord().schema)) {
                    continue;
                }

                for (CclSqlMetricChecker cclSqlMetricChecker : cclTriggerInfo.getCclSqlMetricCheckers()) {
                    boolean check = cclSqlMetricChecker.check(cclSqlMetric);
                    if (!check) {
                        continue out;
                    }
                }
                //match the ccl trigger
                TriggerTask triggerTask =
                    new TriggerTask(cclSqlMetric.getOriginalSql(), cclSqlMetric.getSchemaName(),
                        cclSqlMetric.getTemplateId(), cclSqlMetric.getActiveSession(), cclSqlMetric.getParams(),
                        cclTriggerInfo);
                if (hasBound) {
                    if (triggerTaskCount.getAndIncrement() < TRIGGER_TASK_QUEUE_SIZE) {
                        taskQueue.offer(triggerTask);
                        success = true;
                    }
                } else {
                    taskQueue.offer(triggerTask);
                }
                break;
            }
        } catch (Throwable throwable) {
            LOGGER.error("Fail to offer sample. ", throwable);
        }

        if (!isWorking()) {
            clearSamples();
        }
        return success;
    }

    @Override
    public void processSamples() {
        LOGGER.debug("node running count  = " + ServerThreadPool.AppStats.nodeTaskCount.get());
        if (!isWorking()) {
            return;
        }
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {

            metaDbConn.setAutoCommit(false);

            LOGGER.debug("begin generateCclRules");
            int generatedCount = generateCclRules(metaDbConn);

            if (generatedCount != 0) {
                LOGGER.debug("begin mergeCclRules");
                int mergedTriggerCount = mergeCclRules(metaDbConn);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(String
                        .format("end mergeCclRulesl, mergedTriggerCount= %d", mergedTriggerCount));
                }
                String dataId = MetaDbDataIdBuilder.getCclRuleDataId(InstIdUtil.getInstId());
                MetaDbConfigManager metaDbConfigManager = MetaDbConfigManager.getInstance();
                metaDbConfigManager.notify(dataId, metaDbConn);
                MetaDbUtil.commit(metaDbConn);
            }

        } catch (Throwable throwable) {
            LOGGER.error("Failed to process samples ", throwable);
        } finally {
            this.triggerTaskCount.set(0);
        }
    }

    @Override
    public void clearSamples() {
        taskQueue.clear();
        this.triggerTaskCount.set(0);
        this.working = false;
    }

    @Override
    public boolean isWorking() {
        return working;
    }

    @Override
    public void startWorking() {
        this.working = true;
    }

    private int generateCclRules(Connection metaDbConn) {
        int len = triggerTaskCount.get();
        triggerTaskCount.set(Math.min(len, TRIGGER_TASK_QUEUE_SIZE - 1));
        int generatedCount = 0;
        if (taskQueue.isEmpty()) {
            return generatedCount;
        }

        CclRuleAccessor cclRuleAccessor = new CclRuleAccessor();
        cclRuleAccessor.setConnection(metaDbConn);
        CclTriggerAccessor cclTriggerAccessor = new CclTriggerAccessor();
        cclTriggerAccessor.setConnection(metaDbConn);
        Set<String> ruleNameSet = Sets.newHashSet();
        while (true) {
            TriggerTask triggerTask = taskQueue.poll();
            if (triggerTask == null) {
                break;
            }
            if (!triggerTask.getCclTriggerInfo().isEnabled()) {
                continue;
            }

            String triggerName = triggerTask.getCclTriggerInfo().getTriggerRecord().id;
            int triggerPriority = triggerTask.getCclTriggerInfo().getTriggerRecord().priority;
            List<CclTriggerRecord> cclTriggerRecords =
                cclTriggerAccessor.queryByPriorities(Lists.newArrayList(triggerPriority));
            if (CollectionUtils.isEmpty(cclTriggerRecords)) {
                continue;
            }
            CclTriggerRecord physicalCclTriggerRecord = cclTriggerRecords.get(0);
            if (physicalCclTriggerRecord.cclRuleCount >= physicalCclTriggerRecord.maxCclRule) {
                continue;
            }

            CclRuleRecord cclRuleRecord = new CclRuleRecord();
            cclRuleRecord.sqlType = "ALL";
            cclRuleRecord.dbName = triggerTask.schemaName;
            cclRuleRecord.tableName = "*";
            cclRuleRecord.query = triggerTask.sql;
            cclRuleRecord.userName = null;
            cclRuleRecord.clientIp = "%";

            Map<Integer, Object> posParamValueMap = Maps.newHashMap();
            List<Pair<Integer, ParameterContext>> params = triggerTask.getParams();
            if (params != null) {
                for (Pair<Integer, ParameterContext> pair : params) {
                    posParamValueMap.put(pair.getKey(), pair.getValue().getValue());
                }
            } else {
                SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(cclRuleRecord.query, false);
                posParamValueMap = CclUtils.getPosParamValueMap(sqlParameterized);
            }

            String json = JSON.toJSONString(posParamValueMap);
            cclRuleRecord.params = json;
            cclRuleRecord.queryTemplateId = triggerTask.getTemplateId();
            LOGGER.info(
                String.format("sql: %s, queryTemplateId: %s", cclRuleRecord.query, cclRuleRecord.queryTemplateId));
            String cclRuleName =
                generateCclRuleNameByTrigger(triggerName, triggerTask.schemaName, cclRuleRecord.queryTemplateId,
                    json);
            if (!ruleNameSet.add(cclRuleName)) {
                continue;
            }
            cclRuleRecord.id = cclRuleName;

            List<CclRuleRecord> existedCclRuleRecords = cclRuleAccessor.queryByIds(Lists.newArrayList(cclRuleName));
            if (CollectionUtils.isNotEmpty(existedCclRuleRecords)) {
                LOGGER.info("Existed ccl rule name " + cclRuleName);
                continue;
            }

            cclRuleRecord.parallelism = CclRuleRecord.DEFAULT_PARALLELISM;
            cclRuleRecord.queueSize = CclRuleRecord.DEFAULT_QUEUE_SIZE;
            cclRuleRecord.waitTimeout = CclRuleRecord.DEFAULT_WAIT_TIMEOUT;
            cclRuleRecord.fastMatch = CclRuleRecord.DEFAULT_FAST_MATCH;
            cclRuleRecord.lightWait = CclRuleRecord.DEFAULT_LIGHT_WAIT;
            cclRuleRecord.triggerPriority = triggerPriority;

            //根据withRules设置ccl rule
            List<CclCondition> cclRuleConditions = triggerTask.getCclTriggerInfo().getRuleWiths();
            initCclRuleWiths(cclRuleRecord, cclRuleConditions);
            int inserted = cclRuleAccessor.insert(cclRuleRecord);
            LOGGER.info(String.format("inserted %d", inserted));
            if (inserted > 0) {
                cclTriggerAccessor
                    .updateCclRuleCount(triggerPriority, physicalCclTriggerRecord.cclRuleCount + inserted);
            }
            generatedCount++;
        }
        return generatedCount;
    }

    private int mergeCclRules(Connection metaDbConn) {
        int totalMergedTriggerCount = 0;
        //只在一个节点上做合并任务
        //把所有的ccl rule都拉上来
        CclRuleAccessor cclRuleAccessor = new CclRuleAccessor();
        cclRuleAccessor.setConnection(metaDbConn);
        List<CclRuleRecord> cclRuleRecords = cclRuleAccessor.query();
        if (CollectionUtils.isNotEmpty(cclRuleRecords)) {
            Map<Integer, List<CclRuleRecord>> triggerRuleMap = cclRuleRecords.stream()
                .filter((e) -> e.triggerPriority != CclRuleRecord.DEFAULT_TRIGGER_PRIORITY).collect(
                    Collectors.groupingBy((e) -> e.triggerPriority));
            CclTriggerAccessor cclTriggerAccessor = new CclTriggerAccessor();
            cclTriggerAccessor.setConnection(metaDbConn);
            for (Map.Entry<Integer, List<CclRuleRecord>> entry : triggerRuleMap.entrySet()) {
                List<CclTriggerRecord> cclTriggerRecords =
                    cclTriggerAccessor.queryByPriorities(Lists.newArrayList(entry.getKey()));
                if (CollectionUtils.isEmpty(cclTriggerRecords)) {
                    continue;
                }
                CclTriggerRecord cclTriggerRecord = cclTriggerRecords.get(0);
                boolean merged = mergeOneCclTriggerRules(cclTriggerRecord, entry.getValue(), cclTriggerAccessor,
                    cclRuleAccessor);
                if (merged) {
                    totalMergedTriggerCount++;
                }
            }
        }
        return totalMergedTriggerCount;
    }

    private boolean mergeOneCclTriggerRules(CclTriggerRecord
                                                cclTriggerRecord, List<CclRuleRecord> cclRuleRecords,
                                            CclTriggerAccessor cclTriggerAccessor, CclRuleAccessor cclRuleAccessor) {
        //查找基于模版的限流规则
        Set<String> templateIdCclRuleSet = Sets.newTreeSet();

        cclRuleRecords.stream()
            .filter(
                (e) -> StringUtils.isNotEmpty(e.templateId) && StringUtils.isEmpty(e.queryTemplateId) && StringUtils
                    .isEmpty(e.query) && StringUtils.isEmpty(e.params)
                    && StringUtils
                    .isEmpty(e.keywords))
            .collect(Collectors.toList()).forEach((e) -> {
                String[] templateIds = StringUtils.split(e.templateId);
                for (String templateId : templateIds) {
                    templateIdCclRuleSet.add(templateId + "-" + e.dbName);
                }
            });

        //查找相同查询模版的SQL，对其合并
        Map<String, List<CclRuleRecord>> sameQueryTemplateIdQueryRuleMap =
            cclRuleRecords.stream()
                .filter(
                    (e) -> StringUtils.isNotBlank(e.queryTemplateId) && StringUtils.isEmpty(e.templateId)
                        && StringUtils
                        .isEmpty(e.keywords))
                .collect(Collectors.groupingBy((e) -> e.queryTemplateId + "-" + e.dbName));

        Set<String> toDeleteCclRuleIds = Sets.newHashSet();

        List<CclRuleRecord> templateCclRuleList = Lists.newArrayList();
        for (Map.Entry<String, List<CclRuleRecord>> entry : sameQueryTemplateIdQueryRuleMap.entrySet()) {
            List<CclRuleRecord> cclRuleRecordList = entry.getValue();
            //检查模版是否已经合并过
            if (templateIdCclRuleSet.contains(entry.getKey())) {
                for (CclRuleRecord cclRuleRecord : cclRuleRecordList) {
                    toDeleteCclRuleIds.add(cclRuleRecord.id);
                }
                continue;
            }
            //检查是否需要合并模版
            if (cclRuleRecordList.size() >= cclTriggerRecord.ruleUpgrade) {
                //开始合并
                cclRuleRecordList.sort(Comparator.comparing((e) -> -e.priority));
                CclRuleRecord firstCclRuleRecord = cclRuleRecordList.get(0);
                for (int i = 1; i < cclRuleRecordList.size(); i++) {
                    toDeleteCclRuleIds.add(cclRuleRecordList.get(i).id);
                }
                CclRuleRecord mergedCclRuleRecord = firstCclRuleRecord.copy();
                mergedCclRuleRecord.templateId = mergedCclRuleRecord.queryTemplateId;
                mergedCclRuleRecord.queryTemplateId = null;
                mergedCclRuleRecord.query = null;
                mergedCclRuleRecord.params = null;
                templateCclRuleList.add(mergedCclRuleRecord);
                cclRuleAccessor.update(mergedCclRuleRecord);
            }
        }

        for (CclRuleRecord cclRuleRecord : cclRuleRecords) {
            if (StringUtils.isNotBlank(cclRuleRecord.templateId) && StringUtils
                .isEmpty(cclRuleRecord.queryTemplateId)
                && StringUtils.isEmpty(cclRuleRecord.query) && StringUtils
                .isEmpty(cclRuleRecord.keywords)) {
                templateCclRuleList.add(cclRuleRecord);
            }
        }

        //ccl trigger 上的 ccl rule需要减少的数量
        int substractCclRuleCount = toDeleteCclRuleIds.size();
        if (substractCclRuleCount != 0) {
            int cclRuleCount = cclTriggerRecord.cclRuleCount - substractCclRuleCount;
            cclTriggerAccessor.updateCclRuleCount(cclTriggerRecord.priority, cclRuleCount);
        }

        //需要在一个db纬度下做模版的合并
        Map<String, List<CclRuleRecord>> dbCclRulesMap =
            templateCclRuleList.stream().collect(Collectors.groupingBy((e) -> e.dbName));
        for (List<CclRuleRecord> value : dbCclRulesMap.values()) {
            value.sort(Comparator.comparing((e) -> -e.priority));
            //将模版做合并，合并到一个ccl rule中
            CclRuleRecord mergedCclRuleRecord = value.get(0).copy();
            Set<String> templateIdSet = value.stream().map((e) -> e.templateId).collect(Collectors.toSet());
            mergedCclRuleRecord.templateId = StringUtils.join(templateIdSet, ",");
            cclRuleAccessor.update(mergedCclRuleRecord);

            for (int i = 1; i < value.size(); ++i) {
                toDeleteCclRuleIds.add(value.get(i).id);
            }
        }
        LOGGER.info(String.format("To Delete Ccl Rule Ids %s", toDeleteCclRuleIds.toString()));
        cclRuleAccessor.deleteByIds(Lists.newArrayList(toDeleteCclRuleIds));
        return true;
    }

    private String generateCclRuleNameByTrigger(String triggerName, String schema, String templateId, String sql) {
        int hashCode = Objects.hash(schema, sql);
        String hashCodeHexStr = TStringUtil.int2FixedLenHexStr(hashCode);
        String cclRuleName = String.format(CCL_RULE_NAME_FORMAT, triggerName, schema, templateId, hashCodeHexStr);
        return cclRuleName;
    }

    private void initCclRuleWiths(CclRuleRecord cclRuleRecord, List<CclCondition> cclRuleConditions) {
        for (CclCondition cclCondition : cclRuleConditions) {
            String metricName = cclCondition.getSqlMetricName();
            int value = (int) cclCondition.getValue();
            switch (metricName.toUpperCase()) {
            case "MAX_CONCURRENCY":
                cclRuleRecord.parallelism = value;
                break;
            case "WAIT_QUEUE_SIZE":
                cclRuleRecord.queueSize = value;
                break;
            case "WAIT_TIMEOUT":
                cclRuleRecord.waitTimeout = value;
                break;
            case "FAST_MATCH":
                cclRuleRecord.fastMatch = value;
                break;
            case "LIGHT_WAIT":
                cclRuleRecord.lightWait = value;
                break;
            }
        }
    }

    @Data
    @AllArgsConstructor
    private static class TriggerTask {
        private final String sql;
        private final String schemaName;
        private final String templateId;
        private final long activeSession;
        private final List<Pair<Integer, ParameterContext>> params;
        private final CclTriggerInfo cclTriggerInfo;
    }

}
