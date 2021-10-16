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

package com.alibaba.polardbx.optimizer.core.function;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.Util;

import java.util.List;

public class SqlIfFunction extends SqlFunction {
    public SqlIfFunction() {
        super(
            "IF",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.CONTROL_FLOW_TYPE,
            InferTypes.RETURN_TYPE,
            null,
            SqlFunctionCategory.STRING
        );
    }

    @Override
    public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call) {
        // Do not try to derive the types of the operands. We will do that
        // later, top down.
        return validateOperands(validator, scope, call);
    }

    @Override
    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure) {
        return true;
    }

    /**
     * the operands type checking of CASE operator is rely on return type.
     *
     * @param opBinding description of invocation (not necessarily a
     * {@link SqlCall})
     */
    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        if (!(opBinding instanceof SqlCallBinding)) {
            // derive type when building RexNode.
            return inferTypeFromOperands(opBinding);
        }
        SqlCallBinding callBinding = (SqlCallBinding) opBinding;
        RelDataType returnType = super.inferReturnType(opBinding);
        // type coercion
        if (callBinding.getValidator().isTypeCoercionEnabled()) {
            TypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
            // pos 1 = then
            // pos 2 = else
            boolean coerced = typeCoercion.targetTypeCoercion(callBinding, returnType, 1, 2);
            Util.discard(coerced);
        }
        return returnType;
    }

    protected RelDataType inferTypeFromOperands(SqlOperatorBinding opBinding) {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final List<RelDataType> argTypes = opBinding.collectOperandTypes();
        RelDataType commonType = typeFactory.leastRestrictive(argTypes);
        return commonType;
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.any();
    }
}
