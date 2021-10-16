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

import com.google.common.collect.Maps;
import lombok.Data;

import java.util.Map;

/**
 * @author busu
 * date: 2021/4/2 2:34 下午
 */
@Data
public class CclSqlMetricChecker {

    private final CheckConditionService checkConditionService;
    private final long conditionValue;
    private final CclSqlMetricValueExtractor cclSqlMetricValueExtractor;

    public CclSqlMetricChecker(CheckConditionService checkConditionService, long conditionValue,
                               CclSqlMetricValueExtractor cclSqlMetricValueExtractor) {
        this.checkConditionService = checkConditionService;
        this.conditionValue = conditionValue;
        this.cclSqlMetricValueExtractor = cclSqlMetricValueExtractor;
    }

    public final static Map<String, CclSqlMetricValueExtractor> METRIC_VALUE_EXTRACTOR_MAP =
        Maps.newHashMapWithExpectedSize(5);

    static {
        METRIC_VALUE_EXTRACTOR_MAP.put(CclSqlMetric.METRIC_NAME_RESPONSE_TIME, (e) -> e.getResponseTime());
        METRIC_VALUE_EXTRACTOR_MAP.put(CclSqlMetric.METRIC_NAME_AFFECTED_ROWS, (e) -> e.getAffectedRows());
        METRIC_VALUE_EXTRACTOR_MAP.put(CclSqlMetric.METRIC_NAME_FETCH_ROWS, (e) -> e.getFetchRows());
        METRIC_VALUE_EXTRACTOR_MAP.put(CclSqlMetric.METRIC_NAME_PHY_AFFECTED_ROWS, (e) -> e.getAffectedPhyRows());
        METRIC_VALUE_EXTRACTOR_MAP.put(CclSqlMetric.METRIC_NAME_PHYSICAL_SQL_COUNT, (e) -> e.getPhySqlCount());
        METRIC_VALUE_EXTRACTOR_MAP.put(CclSqlMetric.METRIC_NAME_ACTIVE_SESSION, (e) -> e.getActiveSession());
        METRIC_VALUE_EXTRACTOR_MAP.put(CclSqlMetric.METRIC_SQL_TYPE, (e) -> e.getSqlType());
    }

    /**
     * 判断这个SQL的执行情况是否符合SQL策略的匹配条件
     */
    public boolean check(CclSqlMetric cclSqlMetric) {
        assert checkConditionService != null;
        assert cclSqlMetricValueExtractor != null;

        long actualValue = cclSqlMetricValueExtractor.extractValue(cclSqlMetric);
        if (actualValue == CclSqlMetric.DEFAULT_VALUE) {
            return false;
        }
        boolean checkResult = checkConditionService.check(actualValue, conditionValue);
        return checkResult;
    }

    public interface CclSqlMetricValueExtractor {
        long extractValue(CclSqlMetric cclSqlMetric);
    }

    /**
     * 判断等于，=
     */
    public final static CheckConditionService CHECK_EQUAL =
        (actualValue, conditionValue) -> actualValue != CclSqlMetric.DEFAULT_VALUE && actualValue == conditionValue;

    /**
     * 判断不等于, !=
     */
    public final static CheckConditionService CHECK_NOT_EQUAL =
        (actualValue, conditionValue) -> actualValue != CclSqlMetric.DEFAULT_VALUE && actualValue != conditionValue;

    /**
     * 判断大于，>
     */
    public final static CheckConditionService CHECK_LARGER_THAN =
        (actualValue, conditionValue) -> actualValue != CclSqlMetric.DEFAULT_VALUE && actualValue > conditionValue;

    /**
     * 判断小于等于, >=
     */
    public final static CheckConditionService CHECK_LARGER_THAN_OR_EQUAL =
        (actualValue, conditionValue) -> actualValue != CclSqlMetric.DEFAULT_VALUE && actualValue >= conditionValue;

    /**
     * 判断小于，<
     */
    public final static CheckConditionService CHECK_LESS_THAN =
        (actualValue, conditionValue) -> actualValue != CclSqlMetric.DEFAULT_VALUE && actualValue < conditionValue;

    /**
     * 判断小于等于，<=
     */
    public final static CheckConditionService CHECK_LESS_THAN_OR_EQUAL =
        (actualValue, conditionValue) -> actualValue != CclSqlMetric.DEFAULT_VALUE && actualValue <= conditionValue;

    public final static Map<String, CheckConditionService> COMPARATOR_CHECK_CONDITION_MAP =
        Maps.newHashMapWithExpectedSize(5);

    static {
        COMPARATOR_CHECK_CONDITION_MAP.put(CclComparisonOperator.EQUAL, CHECK_EQUAL);
        COMPARATOR_CHECK_CONDITION_MAP.put(CclComparisonOperator.NOT_EQUAL, CHECK_NOT_EQUAL);
        COMPARATOR_CHECK_CONDITION_MAP.put(CclComparisonOperator.LESS_THAN, CHECK_LESS_THAN);
        COMPARATOR_CHECK_CONDITION_MAP.put(CclComparisonOperator.LARGER_THAN, CHECK_LARGER_THAN);
        COMPARATOR_CHECK_CONDITION_MAP.put(CclComparisonOperator.LARGER_THAN_OR_EQUAL, CHECK_LARGER_THAN_OR_EQUAL);
        COMPARATOR_CHECK_CONDITION_MAP.put(CclComparisonOperator.LESS_THAN_OR_EQUAL, CHECK_LESS_THAN_OR_EQUAL);
    }

    public interface CheckConditionService {
        boolean check(long actualValue, long conditionValue);
    }

}
