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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.utils.SimpleRuleProcessor;

import java.math.BigInteger;
import java.util.Map;

/**
 * Simple Rule
 *
 * @author lingce.ldm 2017-12-08 14:15
 */
public class SimpleShardProcessor extends ShardProcessor {

    private int condIndex;
    private boolean isUnsignedType;
    private DataType dataType;

    protected SimpleShardProcessor(TableRule tableRule, int condIndex, DataType dataType) {
        super(tableRule);
        this.condIndex = condIndex;
        this.isUnsignedType = dataType.isUnsigned();
        this.dataType = dataType;
    }

    protected Pair<String, String> shardInner(Map<Integer, ParameterContext> param, ExecutionContext executionContext) {
        final ParameterContext context = param.get(condIndex + 1);
        if (null == context) {
            return new Pair<>("", tableRule.getVirtualTbName());
        }
        return shard(context.getValue(), executionContext);
    }

    @Override
    Pair<String, String> shard(Map<Integer, ParameterContext> param, ExecutionContext executionContext) {
        return shardInner(param, executionContext);
    }

    public Pair<String, String> shard(Object value, ExecutionContext executionContext) {
        if (isUnsignedType) {
            BigInteger shardVal = (BigInteger) DataTypes.ULongType.convertJavaFrom(value);
            if (shardVal != null && shardVal.compareTo(BigInteger.ZERO) < 0) {
                value = 0;
            }
        } else if (dataType != null) {
            value = dataType.convertJavaFrom(value);
        }
        String encoding = null;
        if (executionContext != null) {
            encoding = executionContext.getEncoding();
        }
        return SimpleRuleProcessor.shardReturnPair(tableRule, value, encoding);
    }

    /**
     * simple shard for simple rule table. return table index instead of group&phy_table name
     */
    public int simpleShard(Object value, String encoding) {
        if (isUnsignedType) {
            BigInteger shardVal = (BigInteger) DataTypes.ULongType.convertJavaFrom(value);
            if (shardVal != null && shardVal.compareTo(BigInteger.ZERO) < 0) {
                value = 0;
            }
        } else if (dataType != null) {
            value = dataType.convertJavaFrom(value);
        }
        return SimpleRuleProcessor.simpleShard(tableRule, value, encoding);
    }
}
