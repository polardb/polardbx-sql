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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rex.RexNode;

import com.alibaba.polardbx.common.model.sqljep.Comparative;

/**
 * @author chenmo.cm
 */
class EmptyConditionResult implements ConditionResult {

    public static final ConditionResult EMPTY = new EmptyConditionResult();

    public EmptyConditionResult() {
    }

    @Override
    public ConditionResult simplify() {
        return this;
    }

    @Override
    public List<RexNode> toRexNodes() {
        return new ArrayList<>();
    }

    @Override
    public Map<String, Comparative> toPartitionCondition(ExecutionContext executionContext) {
        return new HashMap<>();
    }

    @Override
    public Map<String, Comparative> toColumnCondition(List<String> columns) {
        return new HashMap<>();
    }

    @Override
    public Map<String, Comparative> toFullPartitionCondition(ExecutionContext executionContext) {
        return new HashMap<>();
    }

    @Override
    public Map<Integer, BitSet> toColumnEquality() {
        return new HashMap<>();
    }
}
