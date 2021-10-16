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
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * A <code>SqlMultiUpdate</code> is a node of a parse tree which represents an UPDATE
 * multi table statement.
 */
public class SqlMultiUpdate extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("MULTI_UPDATE", SqlKind.MULTI_UPDATE);

  SqlNodeList targetTableList;
  SqlNodeList targetColumnList;
  SqlNodeList sourceExpressionList;
  SqlNode condition;
  SqlSelect sourceSelect;
  SqlNodeList aliasList;
  SqlNodeList joinCondition;

  //~ Constructors -----------------------------------------------------------

  public SqlMultiUpdate(SqlParserPos pos,
      SqlNodeList targetTableList,
      SqlNodeList targetColumnList,
      SqlNodeList sourceExpressionList,
      SqlNode condition,
      SqlSelect sourceSelect,
      SqlNodeList aliasList) {
    super(pos);
    this.targetTableList = targetTableList;
    this.targetColumnList = targetColumnList;
    this.sourceExpressionList = sourceExpressionList;
    this.condition = condition;
    this.sourceSelect = sourceSelect;
    assert sourceExpressionList.size() == targetColumnList.size();
    this.aliasList = aliasList;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.MULTI_UPDATE;
  }

  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(targetTableList, targetColumnList,
        sourceExpressionList, condition, aliasList);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      assert operand instanceof SqlIdentifier;
      targetTableList = (SqlNodeList) operand;
      break;
    case 1:
      targetColumnList = (SqlNodeList) operand;
      break;
    case 2:
      sourceExpressionList = (SqlNodeList) operand;
      break;
    case 3:
      condition = operand;
      break;
    case 4:
      sourceExpressionList = (SqlNodeList) operand;
      break;
    case 5:
      aliasList = (SqlNodeList) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  /**
   * @return the identifier for the target table of the update
   */
  public SqlNodeList getTargetTable() {
    return targetTableList;
  }

  /**
   * @return the alias for the target table of the update
   */
  public SqlNodeList getAlias() {
    return aliasList;
  }

  public void setAlias(SqlNodeList alias) {
    this.aliasList = alias;
  }

  /**
   * @return the list of target column names
   */
  public SqlNodeList getTargetColumnList() {
    return targetColumnList;
  }

  /**
   * @return the list of source expressions
   */
  public SqlNodeList getSourceExpressionList() {
    return sourceExpressionList;
  }

  /**
   * Gets the filter condition for rows to be updated.
   *
   * @return the condition expression for the data to be updated, or null for
   * all rows in the table
   */
  public SqlNode getCondition() {
    return condition;
  }

  /**
   * Gets the source SELECT expression for the data to be updated. Returns
   * null before the statement has been expanded by
   * {@link SqlValidatorImpl#performUnconditionalRewrites(SqlNode, boolean)}.
   *
   * @return the source SELECT for the data to be updated
   */
  public SqlSelect getSourceSelect() {
    return sourceSelect;
  }

  public void setSourceSelect(SqlSelect sourceSelect) {
    this.sourceSelect = sourceSelect;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SELECT, "UPDATE", "");
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    for (int i = 0; i < targetTableList.size(); i++) {
      writer.sep(",");
      targetTableList.get(i).unparse(writer, opLeft, opRight);
      if (aliasList.get(i) != null) {
        writer.keyword("AS");
        writer.print(aliasList.get(i).toString());
      }
    }
    final SqlWriter.Frame setFrame =
        writer.startList(SqlWriter.FrameTypeEnum.UPDATE_SET_LIST, " SET", "");
    for (Pair<SqlNode, SqlNode> pair
        : Pair.zip(getTargetColumnList(), getSourceExpressionList())) {
      writer.sep(",");
      SqlIdentifier id = (SqlIdentifier) pair.left;
      id.unparse(writer, opLeft, opRight);
      writer.keyword("=");
      SqlNode sourceExp = pair.right;
      sourceExp.unparse(writer, opLeft, opRight);
    }
    writer.endList(setFrame);
    if (condition != null) {
      writer.sep("WHERE");
      condition.unparse(writer, opLeft, opRight);
    }
    writer.endList(frame);
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateMultiUpdate(this);
  }

  public SqlNodeList getJoinCondition() {
    return joinCondition;
  }

  public void setJoinCondition(SqlNodeList joinCondition) {
    this.joinCondition = joinCondition;
  }
}

// End SqlMultiUpdate.java
