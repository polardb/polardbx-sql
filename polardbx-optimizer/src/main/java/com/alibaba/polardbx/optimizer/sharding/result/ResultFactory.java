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

package com.alibaba.polardbx.optimizer.sharding.result;

import java.util.Map;
import java.util.Optional;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.label.LabelOptNode;
import com.alibaba.polardbx.optimizer.sharding.label.PredicateNode;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexNode;

/**
 * @author chenmo.cm
 * @date 2020/2/25 10:26 上午
 */
public class ResultFactory {

    public static ConditionResult normalConditionOf(ExtractorContext context, ExtractionResultVisitor.ResultBean resultBean, RelOptTable table) {
        final Map<String, RexNode> conditions = resultBean.getConditions().get(table);

        if (GeneralUtil.isEmpty(conditions)) {
            return EmptyConditionResult.EMPTY;
        }

        return NormalConditionResult.create(context, resultBean.getLabels().get(table).get(0), conditions.values());
    }

    public static ConditionResult pushdownConditionIn(ExtractorContext context, Label label) {
        return Optional.ofNullable(label)
            .map(LabelOptNode::getPushdown)
            .map(PredicateNode::getPredicates)
            .map(p -> (ConditionResult) NormalConditionResult.create(context, label, p))
            .orElse(EmptyConditionResult.EMPTY);
    }

    public static ConditionResult columnEqualityIn(ExtractorContext context, Label label) {
        return Optional.ofNullable(label)
            .map(l -> (ConditionResult) ColumnEqualityConditionResult.create(context, l))
            .orElse(EmptyConditionResult.EMPTY);
    }
}
