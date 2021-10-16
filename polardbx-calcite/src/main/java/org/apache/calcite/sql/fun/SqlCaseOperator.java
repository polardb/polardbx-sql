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

import com.google.common.base.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.Pair;

import org.apache.calcite.util.Util;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * An operator describing a <code>CASE</code>, <code>NULLIF</code> or <code>
 * COALESCE</code> expression. All of these forms are normalized at parse time
 * to a to a simple <code>CASE</code> statement like this:
 *
 * <blockquote><pre><code>CASE
 *   WHEN &lt;when expression_0&gt; THEN &lt;then expression_0&gt;
 *   WHEN &lt;when expression_1&gt; THEN &lt;then expression_1&gt;
 *   ...
 *   WHEN &lt;when expression_N&gt; THEN &lt;then expression_N&gt;
 *   ELSE &lt;else expression&gt;
 * END</code></pre></blockquote>
 *
 * <p>The switched form of the <code>CASE</code> statement is normalized to the
 * simple form by inserting calls to the <code>=</code> operator. For
 * example,</p>
 *
 * <blockquote><pre><code>CASE x + y
 *   WHEN 1 THEN 'fee'
 *   WHEN 2 THEN 'fie'
 *   ELSE 'foe'
 * END</code></pre></blockquote>
 *
 * <p>becomes</p>
 *
 * <blockquote><pre><code>CASE
 * WHEN Equals(x + y, 1) THEN 'fee'
 * WHEN Equals(x + y, 2) THEN 'fie'
 * ELSE 'foe'
 * END</code></pre></blockquote>
 *
 * <p>REVIEW jhyde 2004/3/19 Does <code>Equals</code> handle NULL semantics
 * correctly?</p>
 *
 * <p><code>COALESCE(x, y, z)</code> becomes</p>
 *
 * <blockquote><pre><code>CASE
 * WHEN x IS NOT NULL THEN x
 * WHEN y IS NOT NULL THEN y
 * ELSE z
 * END</code></pre></blockquote>
 *
 * <p><code>NULLIF(x, -1)</code> becomes</p>
 *
 * <blockquote><pre><code>CASE
 * WHEN x = -1 THEN NULL
 * ELSE x
 * END</code></pre></blockquote>
 *
 * <p>Note that some of these normalizations cause expressions to be duplicated.
 * This may make it more difficult to write optimizer rules (because the rules
 * will have to deduce that expressions are equivalent). It also requires that
 * some part of the planning process (probably the generator of the calculator
 * program) does common sub-expression elimination.</p>
 *
 * <p>REVIEW jhyde 2004/3/19. Expanding expressions at parse time has some other
 * drawbacks. It is more difficult to give meaningful validation errors: given
 * <code>COALESCE(DATE '2004-03-18', 3.5)</code>, do we issue a type-checking
 * error against a <code>CASE</code> operator? Second, I'd like to use the
 * {@link SqlNode} object model to generate SQL to send to 3rd-party databases,
 * but there's now no way to represent a call to COALESCE or NULLIF. All in all,
 * it would be better to have operators for COALESCE, NULLIF, and both simple
 * and switched forms of CASE, then translate to simple CASE when building the
 * {@link org.apache.calcite.rex.RexNode} tree.</p>
 *
 * <p>The arguments are physically represented as follows:</p>
 *
 * <ul>
 * <li>The <i>when</i> expressions are stored in a {@link SqlNodeList}
 * whenList.</li>
 * <li>The <i>then</i> expressions are stored in a {@link SqlNodeList}
 * thenList.</li>
 * <li>The <i>else</i> expression is stored as a regular {@link SqlNode}.</li>
 * </ul>
 */
public class SqlCaseOperator extends SqlOperator {
  public static final SqlCaseOperator INSTANCE = new SqlCaseOperator();

  private static final SqlWriter.FrameType FRAME_TYPE =
      SqlWriter.FrameTypeEnum.create("CASE");

  //~ Constructors -----------------------------------------------------------

  private SqlCaseOperator() {
//    super("CASE", SqlKind.CASE, MDX_PRECEDENCE, true, null,
//        InferTypes.RETURN_TYPE, null);
    super("CASE", SqlKind.CASE, 28, true, ReturnTypes.CASE_TYPE, //needs double check
        InferTypes.RETURN_TYPE, null);
  }

  //~ Methods ----------------------------------------------------------------

  public void validateCall(
      SqlCall call,
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlValidatorScope operandScope) {
    final SqlCase sqlCase = (SqlCase) call;
    final SqlNodeList whenOperands = sqlCase.getWhenOperands();
    final SqlNodeList thenOperands = sqlCase.getThenOperands();
    final SqlNode elseOperand = sqlCase.getElseOperand();
    for (SqlNode operand : whenOperands) {
      operand.validateExpr(validator, operandScope);
    }
    for (SqlNode operand : thenOperands) {
      operand.validateExpr(validator, operandScope);
    }
    if (elseOperand != null) {
      elseOperand.validateExpr(validator, operandScope);
    }
  }

  public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    // Do not try to derive the types of the operands. We will do that
    // later, top down.
    return validateOperands(validator, scope, call);
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    SqlCase caseCall = (SqlCase) callBinding.getCall();
    SqlNodeList whenList = caseCall.getWhenOperands();
    SqlNodeList thenList = caseCall.getThenOperands();
    assert whenList.size() == thenList.size();

    // checking that search conditions are ok...
    for (SqlNode node : whenList) {
      // should throw validation error if something wrong...
      RelDataType type =
          callBinding.getValidator().deriveType(
              callBinding.getScope(),
              node);
      if (!SqlTypeUtil.inBooleanFamily(type)) {
        if (throwOnFailure) {
          throw callBinding.newError(RESOURCE.expectedBoolean());
        }
        return false;
      }
    }

    boolean foundNotNull = false;
    for (SqlNode node : thenList) {
      if (!SqlUtil.isNullLiteral(node, false)) {
        foundNotNull = true;
      }
    }

    if (!SqlUtil.isNullLiteral(
        caseCall.getElseOperand(),
        false)) {
      foundNotNull = true;
    }

    if (!foundNotNull) {
      // according to the sql standard we can not have all of the THEN
      // statements and the ELSE returning null
      if (throwOnFailure) {
        throw callBinding.newError(RESOURCE.mustNotNullInElse());
      }
      return false;
    }
    return true;
  }

  /**
   * the operands type checking of CASE operator is rely on return type.
   * @param opBinding description of invocation (not necessarily a
   *                  {@link SqlCall})
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
      boolean coerced = typeCoercion.caseWhenCoercion(callBinding, returnType);
      Util.discard(coerced);
    }
    return returnType;
  }

  private RelDataType inferTypeFromOperands(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final List<RelDataType> argTypes = opBinding.collectOperandTypes();
    Preconditions.checkArgument(argTypes.size() % 2 == 1, "odd number of arguments expected: " + argTypes.size());
    Preconditions.checkArgument(argTypes.size() > 1,
        "CASE must have more than 1 argument. Given " + argTypes.size() + ", " + argTypes);

    List<RelDataType> thenAndElseTypes = IntStream
        .range(0, argTypes.size())
        .filter(i -> i % 2 == 1 || i == argTypes.size() - 1)
        .mapToObj(i -> argTypes.get(i))
        .collect(Collectors.toList());
    RelDataType commonType = typeFactory.leastRestrictive(thenAndElseTypes);
    //todo handle IS_NOT_NULL conditional expression.
    return commonType;
  }

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.any();
  }

  public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  public SqlCall createCall(
      SqlLiteral functionQualifier,
      SqlParserPos pos,
      SqlNode... operands) {
    assert functionQualifier == null;
    assert operands.length == 4;
    return new SqlCase(pos, operands[0], (SqlNodeList) operands[1],
        (SqlNodeList) operands[2], operands[3]);
  }

  @Override public void unparse(SqlWriter writer, SqlCall call_, int leftPrec,
      int rightPrec) {
    SqlCase kase = (SqlCase) call_;
    final SqlWriter.Frame frame =
        writer.startList(FRAME_TYPE, "CASE", "END");
    assert kase.whenList.size() == kase.thenList.size();
    if (kase.value != null) {
      kase.value.unparse(writer, 0, 0);
    }
    for (Pair<SqlNode, SqlNode> pair : Pair.zip(kase.whenList, kase.thenList)) {
      writer.sep("WHEN");
      pair.left.unparse(writer, 0, 0);
      writer.sep("THEN");
      pair.right.unparse(writer, 0, 0);
    }

    writer.sep("ELSE");
    kase.elseExpr.unparse(writer, 0, 0);
    writer.endList(frame);
  }

  public void accept(RexVisitor visitor, RexCall call) {
    visitor.visit(this, call);
  }
}

// End SqlCaseOperator.java
