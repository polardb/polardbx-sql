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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlSyntax;

import java.util.Map;
import java.util.Optional;

public class CommonExpressionNode {
    private RexNode rexNode;
    private Map<Integer, ParameterContext> params;
    private String digest;

    public CommonExpressionNode(RexNode rexNode, Map<Integer, ParameterContext> params) {
        this.rexNode = rexNode;
        this.params = params;
    }

    public RexNode getRexNode() {
        return rexNode;
    }

    String digest() {
        if (digest == null) {
            StringBuilder builder = new StringBuilder();
            RexVisitor visitor = new NodeDigestVisitor(builder);
            this.rexNode.accept(visitor);
            digest = builder.toString();
        }

        return digest;
    }

    private class NodeDigestVisitor extends RexVisitorImpl<Void> {
        private StringBuilder builder;

        protected NodeDigestVisitor(StringBuilder stringBuilder) {
            super(false);
            this.builder = stringBuilder;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            builder.append("$");
            builder.append(inputRef.getIndex());
            return null;
        }

        @Override
        public Void visitLiteral(RexLiteral literal) {
            builder.append(literal.getValue3());
            builder.append(":");
            digestType(literal);
            return null;
        }

        @Override
        public Void visitCall(RexCall call) {
            builder.append(call.op.getName());
            if (call.getOperands().size() == 0 && (call.op.getSyntax() == SqlSyntax.FUNCTION_ID)) {
                // Don't print params for empty arg list.
            } else {
                builder.append("(");
                for (int i = 0; i < call.getOperands().size(); i++) {
                    if (i > 0) {
                        builder.append(", ");
                    }
                    RexNode operand = call.getOperands().get(i);
                    operand.accept(this);
                }
                builder.append(")");
            }
            // type info
            builder.append(":");
            digestType(call);
            return null;
        }

        @Override
        public Void visitDynamicParam(RexDynamicParam dynamicParam) {
            Optional.of(params.get(dynamicParam.getIndex() + 1))
                .map(ParameterContext::getValue)
                .ifPresent(builder::append);
            builder.append(":");
            digestType(dynamicParam);
            return null;
        }

        private void digestType(RexNode rexNode) {
            RelDataType relDataType = rexNode.getType();
            if (!relDataType.isStruct()) {
                builder.append(DataTypeUtil.calciteToDrdsType(relDataType).getClass().getSimpleName());
            } else if (relDataType instanceof RelRecordType) {
                RelRecordType recordType = (RelRecordType) relDataType;
                builder.append("{");
                for (int i = 0; i < recordType.getFieldList().size(); i++) {
                    if (i != 0) {
                        builder.append(",");
                    }
                    builder.append(DataTypeUtil.calciteToDrdsType(recordType.getFieldList().get(i).getType()).getClass()
                        .getSimpleName());
                }
                builder.append("}");
            } else {
                builder.append("{}");
            }
        }
    }

    @Override
    public int hashCode() {
        return digest().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof CommonExpressionNode)) {
            return false;
        }
        return digest().equals(((CommonExpressionNode) obj).digest());
    }
}
