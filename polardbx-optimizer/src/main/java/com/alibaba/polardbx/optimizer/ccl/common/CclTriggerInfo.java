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

package com.alibaba.polardbx.optimizer.ccl.common;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerRecord;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.List;

/**
 * @author busu
 * date: 2021/4/2 11:26 上午
 */
@Data
public class CclTriggerInfo {
    private final CclTriggerRecord triggerRecord;
    private final List<CclCondition> triggerConditions;
    private final List<CclCondition> ruleWiths;
    private final List<CclSqlMetricChecker> cclSqlMetricCheckers;
    private volatile boolean enabled;
    private volatile boolean full;

    public CclTriggerInfo(CclTriggerRecord cclTriggerRecord, List<CclCondition> triggerConditions,
                          List<CclCondition> ruleWiths, List<CclSqlMetricChecker> cclSqlMetricCheckers) {
        this.triggerRecord = cclTriggerRecord;
        this.triggerConditions = triggerConditions;
        this.ruleWiths = ruleWiths;
        this.cclSqlMetricCheckers = cclSqlMetricCheckers;
    }

    public static CclTriggerInfo create(CclTriggerRecord cclTriggerRecord) {

        String conditions = cclTriggerRecord.conditions;
        List<CclCondition> triggerConditions = Collections.EMPTY_LIST;
        if (StringUtils.isNotBlank(conditions)) {
            triggerConditions = JSON.parseArray(conditions, CclCondition.class);
        }

        String ruleConfig = cclTriggerRecord.ruleConfig;
        List<CclCondition> ruleWiths = Collections.EMPTY_LIST;
        if (StringUtils.isNotBlank(ruleConfig)) {
            ruleWiths = JSON.parseArray(ruleConfig, CclCondition.class);
        }

        List<CclSqlMetricChecker> cclSqlMetricCheckers = Lists.newArrayListWithCapacity(triggerConditions.size());
        for (CclCondition cclTriggerCondition : triggerConditions) {
            String sqlMetricName = cclTriggerCondition.getSqlMetricName();
            CclSqlMetricChecker.CclSqlMetricValueExtractor cclSqlMetricValueExtractor =
                CclSqlMetricChecker.METRIC_VALUE_EXTRACTOR_MAP.get(sqlMetricName);
            String comparison = cclTriggerCondition.getComparison();
            CclSqlMetricChecker.CheckConditionService checkConditionService =
                CclSqlMetricChecker.COMPARATOR_CHECK_CONDITION_MAP.get(comparison);
            CclSqlMetricChecker cclSqlMetricChecker =
                new CclSqlMetricChecker(checkConditionService, cclTriggerCondition.getValue(),
                    cclSqlMetricValueExtractor);
            cclSqlMetricCheckers.add(cclSqlMetricChecker);
        }

        CclTriggerInfo cclTriggerInfo =
            new CclTriggerInfo(cclTriggerRecord, triggerConditions, ruleWiths, cclSqlMetricCheckers);

        return cclTriggerInfo;
    }

    @Override
    public int hashCode() {
        if (triggerRecord != null) {
            return triggerRecord.priority;
        }

        //when cclRuleRecord is null. return the same hashCode of zero
        return 0;
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }

        if (obj == null || !(obj instanceof CclTriggerInfo)) {
            return false;
        }

        CclTriggerInfo that = (CclTriggerInfo) obj;
        if (this.triggerRecord == that.triggerRecord) {
            return true;
        }

        if (this.triggerRecord != null && that.triggerRecord != null
            && this.triggerRecord.priority == that.triggerRecord.priority) {
            return true;
        }

        return false;
    }

    public int getOrderValue() {
        return -triggerRecord.priority;
    }

}
