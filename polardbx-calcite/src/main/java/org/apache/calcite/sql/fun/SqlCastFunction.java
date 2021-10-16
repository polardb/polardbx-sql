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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.SqlDataTypeSpec.*;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;
import java.util.Optional;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * SqlCastFunction. Note that the std functions are really singleton objects,
 * because they always get fetched via the StdOperatorTable. So you can't store
 * any local info in the class and hence the return type data is maintained in
 * operand[1] through the validation phase.
 */
public class SqlCastFunction extends SqlFunction {
  //~ Instance fields --------------------------------------------------------
  private final boolean isImplicit;

  /** Map of all casts that do not preserve monotonicity. */
  private final SetMultimap<SqlTypeFamily, SqlTypeFamily> nonMonotonicCasts =
      ImmutableSetMultimap.<SqlTypeFamily, SqlTypeFamily>builder()
          .put(SqlTypeFamily.EXACT_NUMERIC, SqlTypeFamily.CHARACTER)
          .put(SqlTypeFamily.NUMERIC, SqlTypeFamily.CHARACTER)
          .put(SqlTypeFamily.APPROXIMATE_NUMERIC, SqlTypeFamily.CHARACTER)
          .put(SqlTypeFamily.DATETIME_INTERVAL, SqlTypeFamily.CHARACTER)
          .put(SqlTypeFamily.CHARACTER, SqlTypeFamily.EXACT_NUMERIC)
          .put(SqlTypeFamily.CHARACTER, SqlTypeFamily.NUMERIC)
          .put(SqlTypeFamily.CHARACTER, SqlTypeFamily.APPROXIMATE_NUMERIC)
          .put(SqlTypeFamily.CHARACTER, SqlTypeFamily.DATETIME_INTERVAL)
          .put(SqlTypeFamily.DATETIME, SqlTypeFamily.TIME)
          .put(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.TIME)
          .put(SqlTypeFamily.TIME, SqlTypeFamily.DATETIME)
          .put(SqlTypeFamily.TIME, SqlTypeFamily.TIMESTAMP)
          .build();

  private final List<DrdsTypeName> MYSQL_PERMITTED_CAST_TYPE = ImmutableList.of(
      DrdsTypeName.BINARY,
      DrdsTypeName.CHAR,
      DrdsTypeName.DATE,
      DrdsTypeName.DATETIME,
      DrdsTypeName.TIME,
      DrdsTypeName.JSON,
      DrdsTypeName.DECIMAL,
      DrdsTypeName.NCHAR,
      DrdsTypeName.UNSIGNED,
      DrdsTypeName.SIGNED
  );

  //~ Constructors -----------------------------------------------------------

  public SqlCastFunction(boolean isImplicit) {
    super(
        "CAST",
        SqlKind.CAST,
        null,
        InferTypes.FIRST_KNOWN,
        null,
        SqlFunctionCategory.SYSTEM);
    this.isImplicit = isImplicit;
  }

  //~ Methods ----------------------------------------------------------------
  public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    assert opBinding.getOperandCount() == 2;
    RelDataType ret = opBinding.getOperandType(1);
    RelDataType firstType = opBinding.getOperandType(0);

    RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    SqlTypeName retSqlType = ret.getSqlTypeName();

    // Numeric data types that permit the UNSIGNED attribute also permit SIGNED. However,
    // these data types are signed by default, so the SIGNED attribute has no effect.
    if(retSqlType == SqlTypeName.SIGNED) {
      ret = typeFactory.createSqlType(SqlTypeName.BIGINT);
    } else if (retSqlType == SqlTypeName.UNSIGNED) {
      ret = typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED);
    }

    ret = typeFactory.createTypeWithNullability(ret, firstType.isNullable());
    if (opBinding instanceof SqlCallBinding) {
      SqlCallBinding callBinding = (SqlCallBinding) opBinding;
      SqlNode operand0 = callBinding.operand(0);

      // dynamic parameters and null constants need their types assigned
      // to them using the type they are casted to.
      if (((operand0 instanceof SqlLiteral)
          && (((SqlLiteral) operand0).getValue() == null))
          || (operand0 instanceof SqlDynamicParam)) {
        final SqlValidatorImpl validator =
            (SqlValidatorImpl) callBinding.getValidator();
        validator.setValidatedNodeType(operand0, ret);
      }
    }
    return ret;
  }

  public String getSignatureTemplate(final int operandsCount) {
    assert operandsCount == 2;
    return "{0}({1} AS {2})";
  }

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(2);
  }

  /**
   * Makes sure that the number and types of arguments are allowable.
   * Operators (such as "ROW" and "AS") which do not check their arguments can
   * override this method.
   */
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final SqlNode left = callBinding.operand(0);
    final SqlNode right = callBinding.operand(1);
    if (SqlUtil.isNullLiteral(left, false)
        || left instanceof SqlDynamicParam) {
      return true;
    }

    if (!isImplicit && right instanceof SqlDataTypeSpec) {
      Optional.ofNullable(right)
          .map(SqlDataTypeSpec.class::cast)
          .map(SqlDataTypeSpec::getTypeName)
          .map(SqlIdentifier::getSimple)
          .map(DrdsTypeName::from)
          // check the permitted cast types.
          .filter(t -> MYSQL_PERMITTED_CAST_TYPE.contains(t))
          .orElseThrow(
              () -> {
                // throw validator error if type is not permitted.
                SqlValidator validator = callBinding.getValidator();
                return validator.newValidationError(right,
                    RESOURCE.typeNotSupportedForCast(((SqlDataTypeSpec) right).getTypeName().getSimple()));
              }
          );
    }

    RelDataType validatedNodeType =
        callBinding.getValidator().getValidatedNodeType(left);
    RelDataType returnType =
        callBinding.getValidator().deriveType(callBinding.getScope(), right);
    if (!SqlTypeUtil.canCastFrom(returnType, validatedNodeType, true)) {
      if (throwOnFailure) {
        throw callBinding.newError(
            RESOURCE.cannotCastValue(validatedNodeType.toString(),
                returnType.toString()));
      }
      return false;
    }
    if (SqlTypeUtil.areCharacterSetsMismatched(
        validatedNodeType,
        returnType)) {
      if (throwOnFailure) {
        // Include full type string to indicate character
        // set mismatch.
        throw callBinding.newError(
            RESOURCE.cannotCastValue(validatedNodeType.getFullTypeString(),
                returnType.getFullTypeString()));
      }
      return false;
    }
    return true;
  }

  public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    assert call.operandCount() == 2;
    if(!isImplicit) {
      // for normal cast
      final SqlWriter.Frame frame = writer.startFunCall(getName());
      call.operand(0).unparse(writer, 0, 0);
      writer.sep("AS");
      if (call.operand(1) instanceof SqlIntervalQualifier) {
        writer.sep("INTERVAL");
      }
      call.operand(1).unparse(writer, 0, 0);
      writer.endFunCall(frame);
    } else {
      // for type coercion cast, i.e. implicit cast.
      call.operand(0).unparse(writer, 0, 0);
    }
  }

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    RelDataTypeFamily castFrom = call.getOperandType(0).getFamily();
    RelDataTypeFamily castTo = call.getOperandType(1).getFamily();
    if (castFrom instanceof SqlTypeFamily
        && castTo instanceof SqlTypeFamily
        && nonMonotonicCasts.containsEntry(castFrom, castTo)) {
      return SqlMonotonicity.NOT_MONOTONIC;
    } else {
      return call.getOperandMonotonicity(0);
    }
  }

  public boolean isImplicit() {
    return isImplicit;
  }
}

// End SqlCastFunction.java
