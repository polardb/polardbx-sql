/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.Util;

import java.util.List;

/**
 * The <code>COALESCE</code> function.
 */
public class SqlCoalesceFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlCoalesceFunction() {
    super(
        "COALESCE",
        SqlKind.COALESCE,
        ReturnTypes.CONTROL_FLOW_TYPE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM
    );
  }

  protected SqlCoalesceFunction(
      String name,
      SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      SqlFunctionCategory category) {
    super(name, kind, returnTypeInference, operandTypeInference,
        operandTypeChecker, category);
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
   * the operands type checking of COALESCE operator is rely on return type.
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
      boolean coerced = typeCoercion.targetTypeCoercion(callBinding, returnType);
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

  //~ Methods ----------------------------------------------------------------

  // override SqlOperator
  public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
    validateQuantifier(validator, call); // check DISTINCT/ALL

    List<SqlNode> operands = call.getOperandList();

    if (operands.size() == 1) {
      // No CASE needed
      return operands.get(0);
    }

    SqlParserPos pos = call.getParserPosition();

    SqlNodeList whenList = new SqlNodeList(pos);
    SqlNodeList thenList = new SqlNodeList(pos);

    // todo: optimize when know operand is not null.

    for (SqlNode operand : Util.skipLast(operands)) {
      whenList.add(
          SqlStdOperatorTable.IS_NOT_NULL.createCall(pos, operand));
      thenList.add(SqlNode.clone(operand));
    }
    SqlNode elseExpr = Util.last(operands);
    assert call.getFunctionQuantifier() == null;
    return SqlCase.createSwitched(pos, null, whenList, thenList, elseExpr);
  }
}

// End SqlCoalesceFunction.java
