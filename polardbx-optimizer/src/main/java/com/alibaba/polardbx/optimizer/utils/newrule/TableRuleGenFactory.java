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

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.rule.ddl.PartitionByType;
import com.alibaba.polardbx.rule.ddl.PartitionByTypeUtils;

/**
 * Created by simiao on 14-12-2.
 */
public class TableRuleGenFactory {

    private static TableRuleGenFactory _instance = new TableRuleGenFactory();

    public static TableRuleGenFactory getInstance() {
        return _instance;
    }

    public IRuleGen createRuleGenerator(String schemaName, DataType type) {
        if (DataTypeUtil.isStringType(type)) {
            return new StringRuleGen();
        } else if (DataTypeUtil.isUnderLongType(type)
            || DataTypeUtil.equalsSemantically(DataTypes.ULongType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.DecimalType, type)) {
            return new IntegerRuleGen();
        } else if (DataTypeUtil.equalsSemantically(DataTypes.BinaryType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.BytesType, type)) {
            return new BinaryRuleGen();
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DateType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.DatetimeType, type)
            || DataTypeUtil.equalsSemantically(DataTypes.TimestampType, type)) {
            return new DateRuleGen(schemaName);
        } else {
            throw new IllegalArgumentException("Rule generator dataType is not supported! dbMethod:"
                + type.getClass().getSimpleName());
        }
    }

    public IPartitionGen createDBPartitionGenerator(String schemaName, PartitionByType dbMethod) {
        if (PartitionByTypeUtils.isSupportedDbPartitionByType(dbMethod)) {
            return new PartitionGen(schemaName, dbMethod);
        } else {
            throw new IllegalArgumentException(
                String.format("Not support dbpartition method [%s]", dbMethod.toString()));
        }

    }

    public ISubpartitionGen createTBPartitionGenerator(String schemaName, PartitionByType dbMethod,
                                                       PartitionByType tbMethod) {
        if (PartitionByTypeUtils.isSupportedTbPartitionByType(tbMethod)) {
            return new SubpartitionGen(schemaName, dbMethod, tbMethod);
        } else {
            throw new IllegalArgumentException(String.format("Subpartition method %s is not supported!",
                tbMethod.toString()));

        }
    }
}
