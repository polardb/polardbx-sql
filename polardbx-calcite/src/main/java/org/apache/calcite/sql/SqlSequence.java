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

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * Base class for an SqlSequence statements parse tree nodes. The portion of the
 * statement covered by this class is "DROP". Subclasses handle
 * whatever comes afterwards.
 */
public abstract class SqlSequence extends SqlDdl {
  SequenceBean sequenceBean;
  SqlCharStringLiteral sequence;
  SqlCharStringLiteral newSequence;


  /** Creates a SqlDrop. */
  public SqlSequence(SqlOperator operator, SqlParserPos pos) {
    super(operator, pos);
  }

  @Deprecated // to be removed before 2.0
  public SqlSequence(SqlParserPos pos) {
    this(DDL_OPERATOR, pos);
  }

  public SqlNode getName() {
    return name;
  }

  public List<SqlNode> getOperandList() {
    return Arrays.asList(name);
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateDdl(this, validator.getUnknownType(), scope);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(getOperator().getName());
    name.unparse(writer, leftPrec, rightPrec);
  }

  public SequenceBean getSequenceBean() {
    return sequenceBean;
  }

  public void setSequenceBean(SequenceBean sequenceBean) {
    this.sequenceBean = sequenceBean;
  }

  public SqlCharStringLiteral getSequence() {
    return sequence;
  }

  public String getSequenceName() {
    if (sequence != null)
      return sequence.toValue();
    return null;
  }

  public void setNewSequence(SqlCharStringLiteral newSequence) {
    this.newSequence = newSequence;
  }

  public void setSequence(SqlCharStringLiteral sequence) {
    this.sequence = sequence;
  }

  public SqlCharStringLiteral getNewSequence() {
    return newSequence;
  }

  public String getNewSequenceName() {
    if (newSequence != null)
      return newSequence.toValue();
    return null;
  }


}

// End SqlDrop.java
