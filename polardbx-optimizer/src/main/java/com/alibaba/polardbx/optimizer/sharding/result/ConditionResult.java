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

import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rex.RexNode;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chenmo.cm
 */
public interface ConditionResult {

    ConditionResult simplify();

    /**
     * Wrap all RexCall of scalar function (e.g. rand()) with RexCallParam,
     * so that we can replace RexCallParam with literal value in following step
     *
     * @return ResultBean with all scalar function replaced by RexCallParam
     */
    ConditionResult convertScalarFunction2RexCallParam(AtomicInteger maxParamIndex, ExecutionContext executionContext);

    List<RexNode> toRexNodes();

    Map<String, Comparative> toPartitionCondition(ExecutionContext executionContext);

    Map<String, Comparative> toFullPartitionCondition(ExecutionContext executionContext);

    Map<Integer, BitSet> toColumnEquality();

    Map<String, Comparative> toColumnCondition(List<String> columns);

}
