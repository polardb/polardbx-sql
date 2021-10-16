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

package com.alibaba.polardbx.optimizer.utils.newrule;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.ddl.PartitionByType;
import com.alibaba.polardbx.rule.meta.ShardFunctionMeta;

import java.util.List;

/**
 * @author chenghui.lch 2017年6月12日 下午1:29:01
 * @since 5.0.0
 */
public class ShardFuncParamsChecker {

    public static boolean checkColmunMeta(TableMeta tableMeta, PartitionByType partitionByType,
                                          ShardFunctionMeta shardFunMeta) {

        List<String> shardKeys = shardFunMeta.getShardKeyList();

        // 共同的校验逻辑
        for (int i = 0; i < shardKeys.size(); i++) {
            String shardKey = shardKeys.get(i);

            /**
             * 只有当dateKey有内容时才会生成dbRuleArray
             */
            ColumnMeta columnMeta = tableMeta.getColumn(shardKey);
            if (columnMeta == null) {
                throw new OptimizerException("not found partition column:" + shardKey);
            }

            if (TStringUtil.containsAny(shardKey, PartitionGen.illegalChars)) {
                throw new OptimizerException("partition column:" + shardKey
                    + " should not contains any illegal chars '{', '}', '$', '#', ',', '\', ';'");
            }

        }

        // 各个shardFunc的校验逻辑
        if (partitionByType == PartitionByType.RIGHT_SHIFT) {

            // RIGHT_SHIFT 函数相关的校验

            if (shardKeys.size() > 1) {
                throw new OptimizerException(String.format("shard function RIGHT_SHIFT support only one shard key"));
            }

            String shardKey = shardKeys.get(0);

            /**
             * 只有当dateKey有内容时才会生成dbRuleArray
             */
            ColumnMeta columnMeta = tableMeta.getColumn(shardKey);
            if (columnMeta == null) {
                throw new OptimizerException("not found partition column:" + shardKey);
            }

            DataType dataType = columnMeta.getDataType();
            if (!DataTypeUtil.isUnderLongType(dataType)) {
                throw new OptimizerException(String
                    .format("shard function RIGHT_SHIFT does not support using `%s` shardKey", dataType.toString()));
            }

        } else if (partitionByType == PartitionByType.UNI_HASH) {

            // UNI_HASH 函数相关的校验
            if (shardKeys.size() > 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                    String.format("shard function UNI_HASH support only one shard key"));
            }

            String shardKey = shardKeys.get(0);

            /**
             * 只有当dateKey有内容时才会生成dbRuleArray
             */
            ColumnMeta columnMeta = tableMeta.getColumn(shardKey);
            if (columnMeta == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                    "not found partition column:" + shardKey);
            }

            DataType dataType = columnMeta.getDataType();
            if ((!ConfigDataMode.isFastMock()) && DataTypeUtil.isDateType(dataType)) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                    String.format("shard function UNI_HASH does not support using `%s` shardKey which is DateType",
                        dataType.getStringSqlType()));
            }

            // Shard by StringType, Support
            if (DataTypeUtil.isStringType(dataType)) {
                return true;
            }

            if ((!ConfigDataMode.isFastMock()) && !DataTypeUtil.isUnderLongType(dataType)) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, String
                    .format(
                        "shard function UNI_HASH does NOT support using shardKey which data type is BIGINT UNSIGNED"));
            }

        } else if (partitionByType == PartitionByType.RANGE_HASH || partitionByType == PartitionByType.RANGE_HASH1) {

            // RANGE_HASH 函数相关的校验

            if (shardKeys.size() != 2) {
                throw new OptimizerException(
                    String.format("shard function RANGE_HASH/RANGE_HASH1 support only two shard keys"));
            }

            DataType firstColDataType = null;
            for (int i = 0; i < shardKeys.size(); ++i) {
                String shardKeyStr = shardKeys.get(i).trim();

                /**
                 * 只有当dateKey有内容时才会生成dbRuleArray
                 */
                ColumnMeta columnMeta = tableMeta.getColumn(shardKeyStr);
                if (columnMeta == null) {
                    throw new OptimizerException("not found partition column:" + shardKeyStr);
                }

                DataType dataType = columnMeta.getDataType();
                if (!DataTypeUtil.isStringType(dataType) && !DataTypeUtil.isUnderLongType(dataType)
                    && dataType != DataTypes.ULongType) {
                    throw new OptimizerException(
                        String.format("shard function RANGE_HASH/RANGE_HASH1 does not support using `%s` shardKey",
                            dataType.toString()));
                }

                if (firstColDataType == null) {
                    firstColDataType = dataType;
                } else {
                    if (dataType != firstColDataType) {

                        // RANGE_HASH is not allows shard columns that has both StringType and NumberType
                        boolean isStringTypeOfFirstDataType = DataTypeUtil.isStringType(firstColDataType);
                        boolean isStringTypeOfSecondDataType = DataTypeUtil.isStringType(dataType);
                        if (isStringTypeOfFirstDataType != isStringTypeOfSecondDataType) {
                            throw new OptimizerException(
                                String.format(
                                    "shard function RANGE_HASH/RANGE_HASH1 not support use the shard columns with different dataTypes[%s,%s]",
                                    firstColDataType.getStringSqlType(), dataType.getStringSqlType()));
                        }
                    }
                }
            }

        } else if (partitionByType == PartitionByType.STR_HASH) {

            // STR_HASH 函数相关的校验

            if (shardKeys.size() > 1) {
                throw new OptimizerException(String.format("shard function STR_HASH support only one shard key"));
            }

            String shardKey = shardKeys.get(0);

            /**
             * 只有当dateKey有内容时才会生成dbRuleArray
             */
            ColumnMeta columnMeta = tableMeta.getColumn(shardKey);
            if (columnMeta == null) {
                throw new OptimizerException("not found partition column:" + shardKey);
            }

            DataType dataType = columnMeta.getDataType();
            if (!DataTypeUtil.isStringType(dataType)) {
                throw new OptimizerException(
                    String.format("shard function STR_HASH does not support using `%s` shardKey", dataType.toString()));
            }

        } else {
            // 以后支持其它函数的校验逻辑
        }

        shardFunMeta.verifyFunMetaParams();

        return true;
    }

    public static boolean validateShardFuncionForTableRule(TableRule tableRule) {

        boolean checkResult = true;
        if (tableRule == null) {
            return checkResult;
        }
        checkResult = tableRule.varifyTableShardFuncMetaInfo();
        return checkResult;
    }
}
