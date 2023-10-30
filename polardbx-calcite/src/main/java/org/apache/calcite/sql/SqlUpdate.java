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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.TableModify.TableInfo;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;

import com.alibaba.polardbx.common.utils.Assert;

/**
 * A <code>SqlUpdate</code> is a node of a parse tree which represents an UPDATE
 * statement.
 */
public class SqlUpdate extends SqlCall implements SqlHint {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("UPDATE", SqlKind.UPDATE);

  /**
   * Table reference part of update, with all table, alias and join included
   */
  SqlNode targetTable;
  SqlNodeList sourceTables;
  SqlNodeList aliases;
  Map<SqlNode, List<SqlIdentifier>> subQueryTableMap;
  SqlNodeList targetColumnList;
  SqlNodeList sourceExpressionList;
  SqlNode condition;
  SqlSelect sourceSelect;
  SqlIdentifier alias;
  SqlNodeList orderList;
  SqlNode offset;
  SqlNode fetch;
  SqlNodeList keywords;
  SqlNodeList hints;
  OptimizerHint optimizerHints;
  List<Pair<SqlNode, List<SqlNode>>> sourceTableColumns;
  private boolean targetTablesCollected;

  //~ Constructors -----------------------------------------------------------

  public SqlUpdate(SqlParserPos pos,
      SqlNode targetTable,
      SqlNodeList targetColumnList,
      SqlNodeList sourceExpressionList,
      SqlNode condition,
      SqlSelect sourceSelect,
      SqlIdentifier alias) {
    super(pos);
    this.targetTable = targetTable;
    this.targetColumnList = targetColumnList;
    this.sourceExpressionList = sourceExpressionList;
    this.condition = condition;
    this.sourceSelect = sourceSelect;
    assert sourceExpressionList.size() == targetColumnList.size();
    this.alias = alias;
    this.sourceTables = new SqlNodeList(ImmutableList.of(targetTable), SqlParserPos.ZERO);
    this.aliases = new SqlNodeList(ImmutableList.of(targetTable), SqlParserPos.ZERO);
    this.subQueryTableMap = new HashMap<>();
    this.sourceTableColumns = new ArrayList<>();
    this.targetTablesCollected = true;
  }

  public SqlUpdate(SqlParserPos pos,
      SqlNode targetTable,
      SqlNodeList targetColumnList,
      SqlNodeList sourceExpressionList,
      SqlNode condition,
      SqlSelect sourceSelect,
      SqlIdentifier alias,
      SqlNodeList orderList,
      SqlNode fetch,
      SqlNodeList keywords) {
    this(pos, targetTable, targetColumnList, sourceExpressionList, condition, sourceSelect, alias);
    this.orderList = orderList;
    this.fetch = fetch;
    this.keywords = keywords;
  }

  public SqlUpdate(SqlParserPos pos,
      SqlNode targetTable,
      SqlNodeList targetColumnList,
      SqlNodeList sourceExpressionList,
      SqlNode condition,
      SqlSelect sourceSelect,
      SqlIdentifier alias,
      SqlNodeList orderList,
      SqlNode fetch,
      SqlNodeList keywords,
      SqlNodeList hints) {
    this(pos, targetTable, targetColumnList, sourceExpressionList, condition, sourceSelect, alias, orderList, fetch, keywords);
    this.hints = hints;
  }

  public SqlUpdate(SqlParserPos pos,
                   SqlNode targetTable,
                   SqlNodeList targetColumnList,
                   SqlNodeList sourceExpressionList,
                   SqlNode condition,
                   SqlSelect sourceSelect,
                   SqlIdentifier alias,
                   SqlNodeList orderList,
                   SqlNode offset,
                   SqlNode fetch,
                   SqlNodeList keywords,
                   SqlNodeList hints, OptimizerHint optimizerHint) {
    this(pos, targetTable, targetColumnList, sourceExpressionList, condition, sourceSelect, alias, orderList, fetch, keywords, hints);
    this.optimizerHints = optimizerHint;
    this.offset = offset;
  }

  private SqlUpdate(SqlParserPos pos, SqlNode targetTable, SqlNodeList sourceTables,
                    SqlNodeList aliases,
                    Map<SqlNode, List<SqlIdentifier>> subQueryTableMap,
                    SqlNodeList targetColumnList, SqlNodeList sourceExpressionList,
                    SqlNode condition, SqlSelect sourceSelect, SqlIdentifier alias,
                    SqlNodeList orderList, SqlNode offset, SqlNode fetch, SqlNodeList keywords, SqlNodeList hints,
                    OptimizerHint optimizerHints, List<Pair<SqlNode, List<SqlNode>>> sourceTableColumns,
                    boolean targetTablesCollected) {
    super(pos);
    this.targetTable = targetTable;
    this.sourceTables = sourceTables;
    this.aliases = aliases;
    this.subQueryTableMap = subQueryTableMap;
    this.targetColumnList = targetColumnList;
    this.sourceExpressionList = sourceExpressionList;
    this.condition = condition;
    this.sourceSelect = sourceSelect;
    this.alias = alias;
    this.orderList = orderList;
    this.offset = offset;
    this.fetch = fetch;
    this.keywords = keywords;
    this.hints = hints;
    this.optimizerHints = optimizerHints;
    this.sourceTableColumns = sourceTableColumns;
    this.targetTablesCollected = targetTablesCollected;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.UPDATE;
  }

  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(targetTable, targetColumnList,
        sourceExpressionList, condition, alias, orderList, fetch, keywords, offset);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      assert (operand instanceof SqlIdentifier || operand instanceof SqlDynamicParam);
      targetTable = operand;
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
      alias = (SqlIdentifier) operand;
      break;
    case 6:
      orderList = (SqlNodeList) operand;
      break;
    case 7:
      fetch = operand;
      break;
    case 8:
      keywords = (SqlNodeList) operand;
      break;
    case 9:
      offset = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  /**
   * @return the identifier for the target table of the update
   */
  public SqlNode getTargetTable() {
    return targetTable;
  }

  /**
   * @return the alias for the target table of the update
   */
  public SqlIdentifier getAlias() {
    return alias;
  }

  public void setAlias(SqlIdentifier alias) {
    this.alias = alias;
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

  public SqlNodeList getOrderList() {
    return orderList;
  }

  public void setOrderList(SqlNodeList orderList) {
    this.orderList = orderList;
  }

  public SqlNode getOffset() {
    return offset;
  }

  public SqlNode getFetch() {
    return fetch;
  }

  public SqlNodeList getKeywords() {
    return keywords;
  }

  public void setKeywords(SqlNodeList keywords) {
    this.keywords = keywords;
  }

  public SqlNodeList getSourceTables() {
    Assert.assertTrue(targetTablesCollected, "SqlUpdate not initialized");
    return sourceTables;
  }

  public SqlNodeList getAliases() {
    Assert.assertTrue(targetTablesCollected, "SqlUpdate not initialized");
    return aliases;
  }

  public boolean singleTable() {
    return getSourceTables().size() <= 1;
  }

  public boolean withSubquery() {
    return getSourceTables().getList().stream().anyMatch(t -> t instanceof SqlSelect);
  }

  public void setTargetTable(SqlNode targetTable) {
    this.targetTable = targetTable;
  }

  public void setCondition(SqlNode condition) {
    this.condition = condition;
  }

  public List<Pair<SqlNode, List<SqlNode>>> getSourceTableColumns() {
    return sourceTableColumns;
  }

  public void setSourceTableColumns(
      List<Pair<SqlNode, List<SqlNode>>> sourceTableColumns) {
    this.sourceTableColumns = sourceTableColumns;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SELECT, "UPDATE", "");
    if (optimizerHints != null) {
      optimizerHints.unparse(writer, leftPrec, rightPrec);
    }
    
    if (keywords != null) {
      for (SqlNode keyword : keywords) {
        keyword.unparse(writer, 0, 0);
      }
    }
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();

    final SqlWriter.Frame fromFrame =
        writer.startList(SqlWriter.FrameTypeEnum.FROM_LIST);
    if (writer instanceof SqlPrettyWriter) {
      ((SqlPrettyWriter) writer).pushOptStack(SqlWriter.FrameTypeEnum.FROM_LIST);
    }
    if (singleTable()) {
      targetTable.unparse(writer, opLeft, opRight);
      if (alias != null) {
        writer.keyword("AS");
        alias.unparse(writer, opLeft, opRight);
      }
    } else {
      // multi table update
      targetTable.unparse(writer, opLeft, opRight);
    }
    if (writer instanceof SqlPrettyWriter) {
      ((SqlPrettyWriter) writer).popOptStack();
    }
    writer.endList(fromFrame);

    final SqlWriter.Frame setFrame =
        writer.startList(SqlWriter.FrameTypeEnum.UPDATE_SET_LIST, "SET", "");
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

    if (orderList != null && orderList.size() > 0) {
      writer.sep("ORDER BY");
      final SqlWriter.Frame orderFrame =
          writer.startList(SqlWriter.FrameTypeEnum.ORDER_BY_LIST);
      orderList.commaList(writer);
      writer.endList(orderFrame);
    }
    writer.fetchOffset(fetch, offset);
    writer.endList(frame);
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateUpdate(this);
  }

  public SqlUpdate initTableInfo(SqlNodeList sourceTables, SqlNodeList aliases,
                                 Map<SqlNode, List<SqlIdentifier>> subQueryTableMap) {
    this.sourceTables = sourceTables;
    this.aliases = aliases;
    this.subQueryTableMap = subQueryTableMap;
    this.targetTablesCollected = true;

    return this;
  }

  public SqlUpdate initTableInfo(TableInfo srcInfo) {
    final List<SqlNode> sourceTables = new ArrayList<>();
    final List<SqlNode> aliases = new ArrayList<>();
    final Map<SqlNode, List<SqlIdentifier>> subQueryTableMap = new HashMap<>();

    srcInfo.getSrcInfos().forEach(sti -> {
      sourceTables.add(sti.getTable());
      aliases.add(sti.getTableWithAlias());
      subQueryTableMap.put(sti.getTable(),
                sti.getRefTables()
                    .stream()
                    .map(t -> new SqlIdentifier(t.getQualifiedName(), SqlParserPos.ZERO))
                    .collect(Collectors.toList()));
      });

    this.sourceTables = new SqlNodeList(sourceTables, SqlParserPos.ZERO);
    this.aliases = new SqlNodeList(aliases, SqlParserPos.ZERO);
    this.subQueryTableMap = subQueryTableMap;

    this.targetTablesCollected = true;

    return this;
  }

  public List<SqlIdentifier> subQueryTables(SqlNode subQuery) {
    Assert.assertTrue(targetTablesCollected, "SqlUpdate not initialized");
    return subQueryTableMap.get(subQuery);
  }

  @Override
  public SqlNodeList getHints() {
    return hints;
  }

  @Override
  public void setHints(SqlNodeList hints) {
    this.hints = hints;
  }

  public OptimizerHint getOptimizerHints() {
    return optimizerHints;
  }

  public void setOptimizerHints(OptimizerHint optimizerHints) {
    this.optimizerHints = optimizerHints;
  }

  public void setTargetColumnList(SqlNodeList targetColumnList) {
    this.targetColumnList = targetColumnList;
  }

  public void setSourceExpressionList(SqlNodeList sourceExpressionList) {
    this.sourceExpressionList = sourceExpressionList;
  }

  @Override
  public SqlNode clone(SqlParserPos pos) {
    return new SqlUpdate(pos, getTargetTable(), getSourceTables(), getAliases(), this.subQueryTableMap,
        getTargetColumnList(), getSourceExpressionList(), getCondition(), getSourceSelect(), getAlias(), getOrderList(),
        getOffset(), getFetch(), getKeywords(), getHints(), getOptimizerHints(), getSourceTableColumns(),
        this.targetTablesCollected);
  }
}

// End SqlUpdate.java
