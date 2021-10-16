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
package org.apache.calcite.sql.validate.implicit;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation of Calcite implicit type cast.
 */
public class TypeCoercionImpl extends AbstractTypeCoercion {

  public TypeCoercionImpl(RelDataTypeFactory typeFactory, SqlValidator validator) {
    super(typeFactory, validator);
  }

  /**
   * Widen a SqlNode's field type to common type,
   * mainly used for set operations like UNION, INTERSECT and EXCEPT.
   * <p>Rules:
   * <pre>
   *       type1, type2  type3       select a, b, c from t1
   *          \      \      \
   *         type4  type5  type6              UNION
   *          /      /      /
   *       type7  type8  type9       select d, e, f from t2
   * </pre>
   * For struct type (type1, type2, type3) union type (type4, type5, type6),
   * infer the first result column type type7 as the wider type of type1 and type4,
   * the second column type as the wider type of type2 and type5 and so on.
   * @param scope       Validator scope
   * @param query       Query node to update the field type for
   * @param columnIndex Target column index
   * @param targetType  Target type to cast to
   */
  public boolean rowTypeCoercion(
      SqlValidatorScope scope,
      SqlNode query,
      int columnIndex,
      RelDataType targetType) {
    final SqlKind kind = query.getKind();
    switch (kind) {
    case SELECT:
      SqlSelect selectNode = (SqlSelect) query;
      SqlValidatorScope scope1 = validator.getSelectScope(selectNode);
      if (!coerceColumnType(scope1, selectNode.getSelectList(), columnIndex, targetType)) {
        return false;
      }
      updateInferredColumnType(scope1, query, columnIndex, targetType);
      return true;
    case VALUES:
      for (SqlNode rowConstructor : ((SqlCall) query).getOperandList()) {
        if (!coerceOperandType(scope, (SqlCall) rowConstructor, columnIndex, targetType)) {
          return false;
        }
      }
      updateInferredColumnType(scope, query, columnIndex, targetType);
      return true;
    case WITH:
      SqlNode body = ((SqlWith) query).body;
      return rowTypeCoercion(validator.getOverScope(query), body, columnIndex, targetType);
    case UNION:
    case INTERSECT:
    case EXCEPT:
      // Set operations are binary for now.
      final SqlCall operand0 = ((SqlCall) query).operand(0);
      final SqlCall operand1 = ((SqlCall) query).operand(1);
      final boolean coerced = rowTypeCoercion(scope, operand0, columnIndex, targetType)
          && rowTypeCoercion(scope, operand1, columnIndex, targetType);
      // Update the nested SET operator node type.
      if (coerced) {
        updateInferredColumnType(scope, query, columnIndex, targetType);
      }
      return coerced;
    default:
      return false;
    }
  }

  /**
   * Coerces operands in binary arithmetic expressions to NUMERIC types.
   * <p>For binary arithmetic operators like [+, -, *, /, %]:
   * If the operand is VARCHAR,
   * coerce it to data type of the other operand if its data type is NUMERIC;
   * If the other operand is DECIMAL,
   * coerce the STRING operand to max precision/scale DECIMAL.
   */
  public boolean binaryArithmeticCoercion(SqlCallBinding binding) {
    // Assume the operator has NUMERIC family operand type checker.
    SqlOperator operator = binding.getOperator();
    SqlKind kind = operator.getKind();
    boolean coerced = false;
    // Binary operator
    if (binding.getOperandCount() == 2) {
      RelDataType type1 = binding.getOperandType(0);
      RelDataType type2 = binding.getOperandType(1);
      // Special case for datetime + interval or datetime - interval
      if (kind == SqlKind.PLUS || kind == SqlKind.MINUS) {
        if (SqlTypeUtil.isInterval(type1) || SqlTypeUtil.isInterval(type2)) {
          return false;
        }
      }
      // Binary arithmetic operator like: + - * / %.
      // Our goal is to get a numeric-numeric arithmetic.
      if (kind.belongsTo(SqlKind.BINARY_ARITHMETIC)
          && kind != SqlKind.MOD
          && kind != SqlKind.DIVIDE) {
        // cast operands of string type to double, and cast operands of datetime type to bigint.
        RelDataType commonTypeForArithmetic = getCommonTypeForArithmetic(binding, type1, type2);
        if (commonTypeForArithmetic != null) {
          coerced = SqlTypeUtil.isNumeric(type1) ?
              coerced : coerceOperandType(binding.getScope(), binding.getCall(), 0, commonTypeForArithmetic);
          coerced = SqlTypeUtil.isNumeric(type2) ? coerced :
              coerceOperandType(binding.getScope(), binding.getCall(), 1, commonTypeForArithmetic) || coerced;
        }
      }
    }
    return coerced;
  }

  private RelDataType getCommonTypeForArithmetic(SqlCallBinding binding, RelDataType type1, RelDataType type2) {
    // string, numeric -> all to double
    // string, string -> all to double
    if ((SqlTypeUtil.isNumeric(type1) && SqlTypeUtil.isString(type2))
        || (SqlTypeUtil.isNumeric(type2) && SqlTypeUtil.isString(type1))
        || (SqlTypeUtil.isString(type1) && SqlTypeUtil.isString(type2))) {
      return binding.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
    }

    // datetime, int -> all to bigint
    if ((SqlTypeUtil.isNumeric(type1) && SqlTypeUtil.isDatetime(type2))
        || (SqlTypeUtil.isNumeric(type2) && SqlTypeUtil.isDatetime(type1))) {
      return binding.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
    }

    return null;
  }

  /**
   * Coerces operands in binary comparison expressions.
   * <p>Rules:</p>
   * <ul>
   *   <li>For EQUALS(=) operator: 1. If operands are BOOLEAN and NUMERIC, evaluate
   *   `1=true` and `0=false` all to be true; 2. If operands are datetime and string,
   *   do nothing because the SqlToRelConverter already makes the type coercion;</li>
   *   <li>For binary comparision [=, &gt;, &gt;=, &lt;, &lt;=]: try to find the common type,
   *   i.e. "1 &gt; '1'" will be converted to "1 &gt; 1";</li>
   *   <li>For BETWEEN operator, find the common comparison data type of all the operands,
   *   the common type is deduced from left to right, i.e. for expression "A between B and C",
   *   finds common comparison type D between A and B
   *   then common comparison type E between D and C as the final common type.</li>
   * </ul>
   */
  public boolean binaryComparisonCoercion(SqlCallBinding binding) {
    SqlOperator operator = binding.getOperator();
    SqlKind kind = operator.getKind();
    int operandCnt = binding.getOperandCount();
    boolean coerced = false;
    // Binary operator
    if (operandCnt == 2) {
      final RelDataType type1 = binding.getOperandType(0);
      final RelDataType type2 = binding.getOperandType(1);
      if(SqlTypeUtil.equalSansNullability(factory, type1, type2)) {
        return false;
      }
      // Binary comparision operator like: = > >= < <= <=> !=
      if (kind.belongsTo(SqlKind.BINARY_COMPARISON)) {
        SqlNode lNode = binding.operand(0);
        SqlNode rNode = binding.operand(1);
        // this rule is not in MySQL Reference Manual but it does exist.
        coerced = dateTimeStringComparison(binding, type1, type2) || coerced;
        // The following rules is noted on MySQL Reference Manual
        // If one of the arguments is a TIMESTAMP or DATETIME column and the other argument is a constant,
        // the constant is converted to a timestamp before the comparison is performed.
        if (SqlTypeUtil.isDatetime(type1) && (SqlUtil.isLiteral(rNode, false) || SqlUtil.isDynamicParam(rNode))) {
          coerced = coerceOperandType(binding.getScope(), binding.getCall(), 1, type1) || coerced;
        } else if (SqlTypeUtil.isDatetime(type2) && (SqlUtil.isLiteral(lNode, false) || SqlUtil.isDynamicParam(lNode))) {
          coerced = coerceOperandType(binding.getScope(), binding.getCall(), 0, type2) || coerced;
        } else if (SqlTypeUtil.isNumeric(type1) && SqlTypeUtil.isNumeric(type2)) {
          // the following is guaranteed by vectorized evaluation or scalar function evaluation:
          // If one of the arguments is a decimal value, comparison depends on the other argument.
          // The arguments are compared as decimal values if the other argument is a decimal or integer value,
          // or as floating-point values if the other argument is a floating-point value.
          // so we do nothing.
        } else if (SqlTypeUtil.isString(type1) && SqlTypeUtil.isString(type2)) {
          // If both arguments in a comparison operation are strings, they are compared as strings.
          // so we do nothing.
        } else if ((SqlTypeUtil.isEnum(type1) && SqlTypeUtil.isString(type2)) || ((SqlTypeUtil.isEnum(type2) && SqlTypeUtil.isString(type1)))) {
          
        } else {
          // In all other cases, the arguments are compared as floating-point (real) numbers.
          RelDataType doubleType = factory.createSqlType(SqlTypeName.DOUBLE);
          coerced = coerceOperandType(binding.getScope(), binding.getCall(), 0, doubleType) || coerced;
          coerced = coerceOperandType(binding.getScope(), binding.getCall(), 1, doubleType) || coerced;
        }
      }
    }
    return coerced;
  }

  /**
   * Datetime and STRING equality: cast STRING type to datetime type, SqlToRelConverter already
   * makes the conversion but we still keep this interface overridable
   * so user can have their custom implementation.
   */
  protected boolean dateTimeStringComparison(
      SqlCallBinding binding,
      RelDataType left,
      RelDataType right) {
    // REVIEW Danny 2018-05-23 we do not need to coerce type for EQUALS
    // because SqlToRelConverter already does this.
    // REVIEW Danny 2019-09-23, we should unify the coercion rules in TypeCoercion
    // instead of SqlToRelConverter.
    if (SqlTypeUtil.isCharacter(left)
        && SqlTypeUtil.isDatetime(right)) {
      return coerceOperandType(binding.getScope(), binding.getCall(), 0, right);
    }
    if (SqlTypeUtil.isCharacter(right)
        && SqlTypeUtil.isDatetime(left)) {
      return coerceOperandType(binding.getScope(), binding.getCall(), 1, left);
    }
    return false;
  }

  @Override
  public boolean caseWhenCoercion(SqlCallBinding callBinding, RelDataType targetType) {
    // For sql statement like:
    // `case when ... then (a, b, c) when ... then (d, e, f) else (g, h, i)`
    // an exception throws when entering this method.
    SqlCase caseCall = (SqlCase) callBinding.getCall();
    SqlNodeList thenList = caseCall.getThenOperands();
    SqlNode elseOp = caseCall.getElseOperand();

    if (null != targetType) {
      boolean coerced = false;
      for (int i = 0; i < thenList.size(); i++) {
        coerced = coerceOperandType(callBinding.getScope(), thenList, i, targetType) || coerced;
      }
      if (needToCast(callBinding.getScope(), elseOp, targetType)) {
        coerced = coerceOperandType(callBinding.getScope(), caseCall, 3, targetType)
            || coerced;
      }
      return coerced;
    }
    return false;
  }

  @Override
  public boolean targetTypeCoercion(SqlCallBinding callBinding, RelDataType targetType, int... positions) {
    SqlCall call = callBinding.getCall();
    List<SqlNode> operandList = call.getOperandList();

    if (null != targetType) {
      boolean coerced = false;
      if(positions.length == 0) {
        for(int i = 0; i < operandList.size(); i++) {
          coerced = coerceOperandType(callBinding.getScope(), call, i, targetType) || coerced;
        }
      } else {
        for(int pos : positions) {
          coerced = coerceOperandType(callBinding.getScope(), call, pos, targetType) || coerced;
        }
      }
      return coerced;
    }
    return false;
  }

  /**
   * STRATEGIES
   * <p>With(Without) sub-query:
   * <ul>
   * <li>With sub-query: find the common type through comparing the left hand
   * side (LHS) expression types with corresponding right hand side (RHS)
   * expression derived from the sub-query expression's row type. Wrap the
   * fields of the LHS and RHS in CAST operators if it is needed.
   * <li>Without sub-query: convert the nodes of the RHS to the common type by
   * checking all the argument types and find out the minimum common type that
   * all the arguments can be cast to.
   * </ul>
   * <p>How to find the common type:
   * <ul>
   * <li>For both struct sql types (LHS and RHS), find the common type of every
   * LHS and RHS fields pair:
   * <pre>
   * (field1, field2, field3)    (field4, field5, field6)
   *    |        |       |          |       |       |
   *    +--------+---type1----------+       |       |
   *             |       |                  |       |
   *             +-------+----type2---------+       |
   *                     |                          |
   *                     +-------------type3--------+
   * </pre>
   *   </li>
   *   <li>For both basic sql types(LHS and RHS),
   *   find the common type of LHS and RHS nodes.</li>
   * </ul>
   */
  public boolean inOperationCoercion(SqlCallBinding binding) {
    SqlOperator operator = binding.getOperator();
    if (operator.getKind() == SqlKind.IN) {
      assert binding.getOperandCount() == 2;
      final RelDataType type1 = binding.getOperandType(0);
      final RelDataType type2 = binding.getOperandType(1);
      final SqlNode node1 = binding.operand(0);
      final SqlNode node2 = binding.operand(1);
      final SqlValidatorScope scope = binding.getScope();
      if (type1.isStruct()
          && type2.isStruct()
          && type1.getFieldCount() != type2.getFieldCount()) {
        return false;
      }
      int colCount = type1.isStruct() ? type1.getFieldCount() : 1;
      RelDataType[] argTypes = new RelDataType[2];
      argTypes[0] = type1;
      argTypes[1] = type2;
      boolean coerced = false;
      List<RelDataType> widenTypes = new ArrayList<>();
      for (int i = 0; i < colCount; i++) {
        final int i2 = i;
        List<RelDataType> columnIthTypes = new AbstractList<RelDataType>() {
          public RelDataType get(int index) {
            return argTypes[index].isStruct()
                ? argTypes[index].getFieldList().get(i2).getType()
                : argTypes[index];
          }

          public int size() {
            return argTypes.length;
          }
        };

        RelDataType widenType = commonTypeForBinaryComparison(columnIthTypes.get(0),
            columnIthTypes.get(1));
        if (widenType == null) {
          widenType = getTightestCommonType(columnIthTypes.get(0), columnIthTypes.get(1));
        }
        if (widenType == null) {
          // Can not find any common type, just return early.
          return false;
        }
        widenTypes.add(widenType);
      }
      // Find all the common type for RSH and LSH columns.
      assert widenTypes.size() == colCount;
      for (int i = 0; i < widenTypes.size(); i++) {
        RelDataType desired = widenTypes.get(i);
        // LSH maybe a row values or single node.
        if (node1.getKind() == SqlKind.ROW) {
          assert node1 instanceof SqlCall;
          if (coerceOperandType(scope, (SqlCall) node1, i, desired)) {
            updateInferredColumnType(scope, node1, i, widenTypes.get(i));
            coerced = true;
          }
        } else {
          coerced = coerceOperandType(scope, binding.getCall(), 0, desired)
              || coerced;
        }
        // RHS may be a row values expression or sub-query.
        if (node2 instanceof SqlNodeList) {
          final SqlNodeList node3 = (SqlNodeList) node2;
          boolean listCoerced = false;
          if (type2.isStruct()) {
            for (SqlNode node : (SqlNodeList) node2) {
              assert node instanceof SqlCall;
              listCoerced = coerceOperandType(scope, (SqlCall) node, i, desired) || listCoerced;
            }
            if (listCoerced) {
              updateInferredColumnType(scope, node2, i, desired);
            }
          } else {
            for (int j = 0; j < ((SqlNodeList) node2).size(); j++) {
              listCoerced = coerceColumnType(scope, node3, j, desired) || listCoerced;
            }
            if (listCoerced) {
              updateInferredType(node2, desired);
            }
          }
        } else {
          // Another sub-query.
          SqlValidatorScope scope1 = node2 instanceof SqlSelect
              ? validator.getSelectScope((SqlSelect) node2)
              : scope;
          coerced = rowTypeCoercion(scope1, node2, i, desired) || coerced;
        }
      }
      return coerced;
    }
    return false;
  }

  public boolean builtinFunctionCoercion(
      SqlCallBinding binding,
      List<RelDataType> operandTypes,
      List<SqlTypeFamily> expectedFamilies) {
    assert binding.getOperandCount() == operandTypes.size();
    if (!canImplicitTypeCast(operandTypes, expectedFamilies)) {
      return false;
    }
    boolean coerced = false;
    for (int i = 0; i < operandTypes.size(); i++) {
      RelDataType implicitType = implicitCast(operandTypes.get(i), expectedFamilies.get(i));
      coerced = null != implicitType
          && operandTypes.get(i) != implicitType
          && coerceOperandType(binding.getScope(), binding.getCall(), i, implicitType)
          || coerced;
    }
    return coerced;
  }

  /**
   * Type coercion for user defined functions(UDFs).
   */
  public boolean userDefinedFunctionCoercion(SqlValidatorScope scope,
      SqlCall call, SqlFunction function) {
    final List<RelDataType> paramTypes = function.getParamTypes();
    assert paramTypes != null;
    boolean coerced = false;
    for (int i = 0; i < call.operandCount(); i++) {
      SqlNode operand = call.operand(i);
      if (operand.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
        final List<SqlNode> operandList = ((SqlCall) operand).getOperandList();
        String name = ((SqlIdentifier) operandList.get(1)).getSimple();
        int formalIndex = function.getParamNames().indexOf(name);
        if (formalIndex < 0) {
          return false;
        }
        // Column list operand type is not supported now.
        coerced = coerceOperandType(scope, (SqlCall) operand, 0,
            paramTypes.get(formalIndex)) || coerced;
      } else {
        coerced = coerceOperandType(scope, call, i, paramTypes.get(i)) || coerced;
      }
    }
    return coerced;
  }

  public boolean querySourceCoercion(SqlValidatorScope scope,
      RelDataType sourceRowType, RelDataType targetRowType, SqlNode query) {
    final List<RelDataTypeField> sourceFields = sourceRowType.getFieldList();
    final List<RelDataTypeField> targetFields = targetRowType.getFieldList();
    final int sourceCount = sourceFields.size();
    for (int i = 0; i < sourceCount; i++) {
      RelDataType sourceType = sourceFields.get(i).getType();
      RelDataType targetType = targetFields.get(i).getType();
      if (!SqlTypeUtil.equalSansNullability(validator.getTypeFactory(), sourceType, targetType)
          && !SqlTypeUtil.canCastFrom(targetType, sourceType, true)) {
        // Returns early if types not equals and can not do type coercion.
        return false;
      }
    }
    boolean coerced = false;
    for (int i = 0; i < sourceFields.size(); i++) {
      RelDataType targetType = targetFields.get(i).getType();
      coerced = coerceSourceRowType(scope, query, i, targetType) || coerced;
    }
    return coerced;
  }

  /**
   * Coerces the field expression at index {@code columnIndex} of source
   * in an INSERT or UPDATE query to target type.
   * @param sourceScope Query source scope
   * @param query       Query
   * @param columnIndex Source column index to coerce type
   * @param targetType  Target type
   */
  private boolean coerceSourceRowType(
      SqlValidatorScope sourceScope,
      SqlNode query,
      int columnIndex,
      RelDataType targetType) {
    switch (query.getKind()) {
    case INSERT:
      SqlInsert insert = (SqlInsert) query;
      return coerceSourceRowType(sourceScope,
          insert.getSource(),
          columnIndex,
          targetType);
    case UPDATE:
      SqlUpdate update = (SqlUpdate) query;
      if (update.getSourceExpressionList() != null) {
        final SqlNodeList sourceExpressionList = update.getSourceExpressionList();
        return coerceColumnType(sourceScope, sourceExpressionList, columnIndex, targetType);
      } else {
        return coerceSourceRowType(sourceScope,
            update.getSourceSelect(),
            columnIndex,
            targetType);
      }
    default:
      return rowTypeCoercion(sourceScope, query, columnIndex, targetType);
    }
  }
}
