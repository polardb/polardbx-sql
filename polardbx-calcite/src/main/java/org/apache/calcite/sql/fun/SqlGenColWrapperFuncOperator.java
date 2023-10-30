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

package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Do nothing, just wrap a SqlNode with parentheses
 */
public class SqlGenColWrapperFuncOperator extends SqlSpecialOperator {
    public SqlGenColWrapperFuncOperator() {
        super("GEN_COL_WRAPPER_FUNC", SqlKind.DEFAULT, 100, true, ReturnTypes.ARG0, InferTypes.RETURN_TYPE,
            OperandTypes.ONE);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
                        int rightPrec) {
        if (!call.getOperandList().isEmpty()) {
            final SqlWriter.Frame frame = writer.startList("(", ")");
            call.operand(0).unparse(writer, leftPrec, rightPrec);
            writer.endList(frame);
        }
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        if (opBinding.getOperandCount() == 1) {
            return opBinding.getOperandType(0);
        }
        return super.inferReturnType(opBinding);
    }

    @Override
    public boolean canPushDown() {
        return true;
    }

    @Override
    public boolean canPushDown(boolean withScaleOut) {
        return true;
    }
}
