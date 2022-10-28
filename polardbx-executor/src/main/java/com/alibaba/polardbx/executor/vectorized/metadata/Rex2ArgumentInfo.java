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

package com.alibaba.polardbx.executor.vectorized.metadata;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;

import java.util.Set;
import java.util.stream.Collectors;

public class Rex2ArgumentInfo extends RexVisitorImpl<ArgumentInfo> {
    private static final Logger LOG = LoggerFactory.getLogger(Rex2ArgumentInfo.class);
    private static final Rex2ArgumentInfo SINGLETON = new Rex2ArgumentInfo();

    private Rex2ArgumentInfo() {
        super(false);
    }

    public static ArgumentInfo toArgumentInfo(RexNode rexNode) {
        try {
            return rexNode.accept(SINGLETON);
        } catch (Exception e) {
            LOG.error("Failed to convert " + rexNode + " to vectorized expression argument info!", e);
            return null;
        }
    }

    private static DataType<?> convertStructDataType(RelDataType relDataType) {
        Preconditions.checkArgument(relDataType.isStruct());
        Set<DataType<?>> dataTypes = relDataType.getFieldList().stream()
            .map(RelDataTypeField::getType)
            .map(DataTypeUtil::calciteToDrdsType)
            .map(t -> (DataType<?>) t)
            .collect(Collectors.toSet());

        if (dataTypes.size() > 1) {
            // struct type
            return null;
        }

        return dataTypes.iterator().next();
    }

    @Override
    public ArgumentInfo visitLiteral(RexLiteral literal) {
        return new ArgumentInfo(DataTypeUtil.calciteToDrdsType(literal.getType()), ArgumentKind.Const);
    }

    @Override
    public ArgumentInfo visitInputRef(RexInputRef inputRef) {
        return new ArgumentInfo(DataTypeUtil.calciteToDrdsType(inputRef.getType()), ArgumentKind.Variable);
    }

    @Override
    public ArgumentInfo visitCall(RexCall call) {
        if (call.getType().isStruct()) {
            DataType<?> dataTypes;
            try {
                dataTypes = convertStructDataType(call.getType());
                if (dataTypes == null) {
                    // fail to convert struct value.
                    return null;
                }
            } catch (Exception e) {
                LOG.error("Failed to convert return type " + call.getType() + " of " + call + " to argument info.", e);
                return null;
            }

            boolean allConsts = true;
            for (RexNode operand : call.getOperands()) {
                ArgumentInfo info = operand.accept(this);
                if (info == null) {
                    return null;
                }

                if (info.getKind() != ArgumentKind.Const) {
                    allConsts = false;
                }
            }

            if (allConsts) {
                return new ArgumentInfo(dataTypes, ArgumentKind.ConstVargs);
            } else {
                return new ArgumentInfo(dataTypes, ArgumentKind.Varargs);
            }
        } else {
            return new ArgumentInfo(DataTypeUtil.calciteToDrdsType(call.getType()), ArgumentKind.Variable);
        }
    }

    @Override
    public ArgumentInfo visitDynamicParam(RexDynamicParam dynamicParam) {
        if (dynamicParam.getIndex() != -3) {
            return new ArgumentInfo(DataTypeUtil.calciteToDrdsType(dynamicParam.getType()), ArgumentKind.Const);
        }

        return null;
    }
}