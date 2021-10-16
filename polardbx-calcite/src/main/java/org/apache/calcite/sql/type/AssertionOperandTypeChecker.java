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

package org.apache.calcite.sql.type;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;

import java.util.List;
import java.util.Map;

/**
 * Check the operand types by specified type family list.
 * And apply type coercion rules if given target type list.
 */
public class AssertionOperandTypeChecker extends FamilyOperandTypeChecker {
    /**
     * {allowed families} -> {target types}
     * list=[
     *        operand1: ({}->{}, {}->{})
     *        operand2: ({}->{})
     *      ]
     */
    protected final List<Map<SqlTypeFamily, SqlTypeName>> coercionTypeMatrix;

    /**
     * Package private. Create using {@link OperandTypes#family}.
     *

     */
    public AssertionOperandTypeChecker(List<SqlTypeFamily> families,
                                       List<Map<SqlTypeFamily, SqlTypeName>> coercionTypeMatrix,
                                       Predicate<Integer> optional) {
        super(families, optional);

        if (coercionTypeMatrix != null) {
            // we must ensure families list size is equal to available coercion list size.
            Preconditions.checkArgument(coercionTypeMatrix.size() == families.size());
            for (int i = 0; i < coercionTypeMatrix.size(); i++) {
                SqlTypeFamily family = families.get(i);
                coercionTypeMatrix.get(i).forEach(

                    // and we must ensure each of the target type is contained in type family.
                    (k, v) -> Preconditions.checkArgument(family.getTypeNames().contains(v))
                );
            }
        }

        this.coercionTypeMatrix = coercionTypeMatrix;
    }

    /**
     * Package private. Create using {@link OperandTypes#family}.
     */
    public AssertionOperandTypeChecker(List<SqlTypeFamily> families,
                                       Predicate<Integer> optional) {
        this(families, null, optional);
    }

    @Override
    protected boolean tryCast(SqlCallBinding callBinding, TypeCoercion typeCoercion) {
        if (coercionTypeMatrix == null) {
            return false;
        }
        boolean coerced = false;

        // check if we can apply type coercion to this operator.
        int operandSize = callBinding.operands().size();

        for (int position = 0; position < operandSize; position++) {
            // get the type of node in specified position
            RelDataType type = callBinding.getOperandType(position);
            SqlTypeName typeName = type.getSqlTypeName();

            if (families.get(position).contains(typeName)) {
                // don't need to cast.
                continue;
            }

            Map<SqlTypeFamily, SqlTypeName> coercionMap = coercionTypeMatrix.get(position);
            for(Map.Entry<SqlTypeFamily, SqlTypeName> entry
                : coercionMap.entrySet()) {
                // try to match the type family
                SqlTypeFamily family = entry.getKey();
                if (family.getTypeNames().contains(typeName)) {
                    // match the coercion rule, try to cast.
                    SqlTypeName targetTypeName = entry.getValue();

                    RelDataType targetType = callBinding
                        .getTypeFactory()
                        .createSqlType(targetTypeName);

                    // do cast
                    coerced |= typeCoercion.targetTypeCoercion(callBinding, targetType, position);
                    break;
                }
            }
        }

        return coerced;
    }
}
