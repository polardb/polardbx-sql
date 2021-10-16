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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import org.openjdk.jol.info.ClassLayout;

import java.math.BigDecimal;

/**
 * A numeric SQL literal.
 */
public class SqlNumericLiteral extends SqlLiteral {
  //~ Instance fields --------------------------------------------------------
  /**
   * The size of {@code Integer} is 16 bytes.
   */
  private static final long INTERNAL_OBJECT_SIZE_COUNT_ONCE = 16 * 2;
  /**
   * 40 for BigDecimal size estimation
   */
  private static final long INSTANCE_SIZE_WITH_OBJECT =
      ClassLayout.parseClass(SqlNumericLiteral.class).instanceSize() + 40;

  private Integer prec;
  private Integer scale;
  private boolean isExact;

  //~ Constructors -----------------------------------------------------------

  protected SqlNumericLiteral(
      Comparable value,
      Integer prec,
      Integer scale,
      boolean isExact,
      SqlParserPos pos) {
    super(
        value,
        isExact ? SqlTypeName.DECIMAL : SqlTypeName.DOUBLE,
        pos);
    this.prec = prec;
    this.scale = scale;
    this.isExact = isExact;
  }
  protected SqlNumericLiteral(
      Comparable value,
      Integer prec,
      Integer scale,
      boolean isExact,
      SqlParserPos pos,
      SqlTypeName typeName) {
     super(value, typeName, pos);
      this.prec = prec;
      this.scale = scale;
      this.isExact = isExact;
    }
  //~ Methods ----------------------------------------------------------------
  @Override
  public long getCountOnceObjectSize() {
    return super.getCountOnceObjectSize() + INTERNAL_OBJECT_SIZE_COUNT_ONCE;
  }

  @Override
  public long estimateSize() {
    return INSTANCE_SIZE_WITH_OBJECT;
  }

  public Integer getPrec() {
    return prec;
  }

  public Integer getScale() {
    return scale;
  }

  public boolean isExact() {
    return isExact;
  }

  @Override public SqlNumericLiteral clone(SqlParserPos pos) {
    return new SqlNumericLiteral((Comparable)value, getPrec(), getScale(),
        isExact, pos, this.getTypeName());
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.literal(toValue());
  }

  public String toValue() {
    if (isExact) {
      return value.toString();
    }
    BigDecimal bd = (BigDecimal) value;
      return Util.toScientificNotation(bd);
  }

  public RelDataType createSqlType(RelDataTypeFactory typeFactory) {
    if (isExact) {
      int scaleValue = scale.intValue();
      if (0 == scaleValue) {
          SqlTypeName result = this.getTypeName();
          if (value instanceof Integer) {
              result = SqlTypeName.INTEGER;
          } else if (value instanceof Long) {
              result = SqlTypeName.BIGINT;
          }
        return typeFactory.createSqlType(result);
      }

      // else we have a decimal
      return typeFactory.createSqlType(
          SqlTypeName.DECIMAL,
          prec.intValue(),
          scaleValue);
    }

    // else we have a a float, real or double.  make them all double for
    // now.
    return typeFactory.createSqlType(SqlTypeName.DOUBLE);
  }

  public boolean isInteger() {
    return 0 == scale.intValue();
  }
}

// End SqlNumericLiteral.java
