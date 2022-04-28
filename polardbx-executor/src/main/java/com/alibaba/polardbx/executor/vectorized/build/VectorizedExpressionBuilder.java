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

package com.alibaba.polardbx.executor.vectorized.build;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * The Builder of vectorized expressions.
 */
public class VectorizedExpressionBuilder {
    private final static RelDataTypeFactory FACTORY = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(FACTORY);

    /**
     * For FilterExec / VectorizedFilterExec, we must ensure the root of the node is in type of BIGINT / BOOLEAN.
     */
    public static RexNode rewriteRoot(RexNode rexNode, boolean isFilterExec) {
        if (isFilterExec && !SqlTypeUtil.isNumeric(rexNode.getType()) && !SqlTypeUtil.isBoolean(rexNode.getType())) {
            return REX_BUILDER.makeCall(
                FACTORY.createSqlType(SqlTypeName.BIGINT),
                TddlOperatorTable.IS_TRUE,
                ImmutableList.of(rexNode));
        } else {
            return rexNode;
        }
    }

    public static Pair<VectorizedExpression, MutableChunk> buildVectorizedExpression(List<DataType<?>> inputTypes,
                                                                                     RexNode rexNode,
                                                                                     ExecutionContext executionContext) {
        return buildVectorizedExpression(inputTypes, rexNode, executionContext, false);
    }

    public static Pair<VectorizedExpression, MutableChunk> buildVectorizedExpression(List<DataType<?>> inputTypes,
                                                                                     RexNode rexNode,
                                                                                     ExecutionContext executionContext,
                                                                                     boolean isFilterExec) {
        Preconditions
            .checkArgument(canConvertToVectorizedExpression(executionContext, rexNode),
                "Unable to create vectorized expression!");

        RexNode root = rewriteRoot(rexNode, isFilterExec);

        InputRefTypeChecker inputRefTypeChecker = new InputRefTypeChecker(inputTypes);
        root = root.accept(inputRefTypeChecker);

        Rex2VectorizedExpressionVisitor converter =
            new Rex2VectorizedExpressionVisitor(executionContext, inputTypes.size());
        VectorizedExpression expr = root.accept(converter);

        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(executionContext.getExecutorChunkLimit())
            .addEmptySlots(inputTypes)
            .addEmptySlots(converter.getOutputDataTypes())
            .build();

        return new Pair<>(expr, preAllocatedChunk);
    }

    public static boolean canConvertToVectorizedExpression(ExecutionContext context, RexNode... rexNodes) {
        return canConvertToVectorizedExpression(context, Arrays.asList(rexNodes));
    }

    public static boolean canConvertToVectorizedExpression(ExecutionContext context, Collection<RexNode> rexNodes) {
        return context.getParamManager().getBoolean(ConnectionParams.ENABLE_EXPRESSION_VECTORIZATION)
            && rexNodes.stream().allMatch(Rex2VectorizedExpressionChecker::check);
    }
}

