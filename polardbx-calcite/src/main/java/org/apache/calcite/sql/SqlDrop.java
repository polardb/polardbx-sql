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

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.Arrays;
import java.util.List;

/**
 * Base class for an DROP statements parse tree nodes. The portion of the
 * statement covered by this class is "DROP". Subclasses handle
 * whatever comes afterwards.
 */
public abstract class SqlDrop extends SqlDdl {

  /** Whether "IF EXISTS" was specified. */
  protected final boolean ifExists;

  /** Creates a SqlDrop. */
  public SqlDrop(SqlOperator operator, SqlParserPos pos, boolean ifExists) {
    super(operator, pos);
    this.ifExists = ifExists;
  }

  @Deprecated // to be removed before 2.0
  public SqlDrop(SqlParserPos pos) {
    this(DDL_OPERATOR, pos, false);
  }

  public SqlNode getName() {
    return name;
  }

  public List<SqlNode> getOperandList() {
    return Arrays.asList(name);
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateDdl(this , validator.getUnknownType());
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(getOperator().getName()); // "DROP TABLE" etc.
    if (ifExists) {
      writer.keyword("IF EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
  }

  public boolean isIfExists() {
    return ifExists;
  }
}

// End SqlDrop.java
