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

package com.alibaba.polardbx.rule.model;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 匹配结果对象
 *
 * @author jianghang 2013-10-29 下午4:10:52
 * @since 5.0.0
 */
public class MatcherResult {

    private final List<TargetDB> calculationResult;     // 匹配的db结果
    private final Map<String, Comparative> databaseComparativeMap; // 计算出该结果Rule中匹配的库参数
    private final Map<String, Comparative> tableComparativeMap;   // 计算出该结果Rule中匹配的表参数

    public MatcherResult(List<TargetDB> calculationResult, Map<String, Comparative> databaseComparativeMap,
                         Map<String, Comparative> tableComparativeMap) {
        this.calculationResult = calculationResult;
        this.databaseComparativeMap = databaseComparativeMap;
        this.tableComparativeMap = tableComparativeMap;
    }

    /**
     * 规则计算后的结果对象
     */
    public List<TargetDB> getCalculationResult() {
        return calculationResult;
    }

    /**
     * 产生这个匹配结果时，对应的库参数是什么,不会出现Null值
     */
    public Map<String, Comparative> getDatabaseComparativeMap() {
        return databaseComparativeMap;
    }

    /**
     * 产生这个匹配结果时，对应的表参数是什么,不会出现Null值
     */
    public Map<String, Comparative> getTableComparativeMap() {
        return tableComparativeMap;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }
}
