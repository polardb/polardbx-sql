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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.BitString;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

/**
 * A binary (or hexadecimal) string literal.
 *
 * <p>The {@link #value} field is a {@link BitString} and {@link #typeName} is
 * {@link SqlTypeName#BINARY}.
 */
public class SqlBinaryStringLiteral extends SqlAbstractStringLiteral {
  /**
   * 40 for empty String size estimation
   */
  private static final long INSTANCE_SIZE_WITH_OBJECT =
      ClassLayout.parseClass(SqlBinaryStringLiteral.class).instanceSize() +
          ClassLayout.parseClass(BitString.class).instanceSize() + 40;

  private static final Function<SqlLiteral, BitString> F =
      new Function<SqlLiteral, BitString>() {
        public BitString apply(SqlLiteral literal) {
          return ((SqlBinaryStringLiteral) literal).getBitString();
        }
      };
  private boolean binary = false;

  //~ Constructors -----------------------------------------------------------

  protected SqlBinaryStringLiteral(
      BitString val,
      SqlParserPos pos) {
    super(val, SqlTypeName.BINARY, pos);
  }

  protected SqlBinaryStringLiteral(
          BitString val,boolean binary,
          SqlParserPos pos) {
    super(val, SqlTypeName.BINARY, pos);
    this.binary = binary;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * @return the underlying BitString
   */
  public BitString getBitString() {
    return (BitString) value;
  }

  @Override public SqlBinaryStringLiteral clone(SqlParserPos pos) {
    return new SqlBinaryStringLiteral((BitString) value, pos);
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    assert value instanceof BitString;
    if (binary ) {
      writer.literal("B'" + ((BitString) value) + "'");
    } else {
      writer.literal("X'" + ((BitString) value).toHexString() + "'");
    }
  }

  protected SqlAbstractStringLiteral concat1(List<SqlLiteral> literals) {
    return new SqlBinaryStringLiteral(
        BitString.concat(Lists.transform(literals, F)),
        literals.get(0).getParserPosition());
  }

  @Override
  public int getValueLength() {
    return ((BitString)value).getBitCount();
  }

  @Override
  public long estimateSize() {
    return INSTANCE_SIZE_WITH_OBJECT + getValueBytes();
  }
}

// End SqlBinaryStringLiteral.java
