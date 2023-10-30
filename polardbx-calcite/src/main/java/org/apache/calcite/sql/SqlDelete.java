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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.utils.Assert;
import org.apache.calcite.rel.core.TableModify.TableInfo;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;

/**
 * A <code>SqlDelete</code> is a node of a parse tree which represents a DELETE
 * statement.
 */
public class SqlDelete extends SqlCall implements SqlHint {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DELETE", SqlKind.DELETE);

  SqlNode targetTable;
  SqlNode condition;
  SqlSelect sourceSelect;
  SqlIdentifier alias;
  SqlNode from;
  SqlNode using;
  SqlNodeList targetTables;
  SqlNodeList sourceTables;
  SqlNodeList aliases;
  SqlNodeList orderList;
  SqlNode offset;
  SqlNode fetch;
  SqlNodeList keywords;
  SqlNodeList hints;
  boolean withTableAlias;
  Map<SqlNode, List<SqlIdentifier>> subQueryTableMap;
  List<Pair<SqlNode, List<SqlNode>>> sourceTableColumns;
  private boolean sourceTableCollected;

  //~ Constructors -----------------------------------------------------------

  public SqlDelete(
      SqlParserPos pos,
      SqlNode targetTable,
      SqlNode condition,
      SqlSelect sourceSelect,
      SqlIdentifier alias ) {
    super(pos);
    this.targetTable = targetTable;
    this.condition = condition;
    this.sourceSelect = sourceSelect;
    this.alias = alias;
    this.subQueryTableMap = new HashMap<>();
    this.sourceTableColumns = new ArrayList<>();
    this.sourceTables = new SqlNodeList(ImmutableList.of(targetTable), SqlParserPos.ZERO);
    this.targetTables = new SqlNodeList(ImmutableList.of(targetTable), SqlParserPos.ZERO);
    this.aliases = new SqlNodeList(ImmutableList.of(targetTable), SqlParserPos.ZERO);
    this.sourceTableCollected = true;
  }

  public SqlDelete(
      SqlParserPos pos,
      SqlNode targetTable,
      SqlNode condition,
      SqlSelect sourceSelect,
      SqlIdentifier alias,
      SqlNode from,
      SqlNode using,
      SqlNodeList targetTables) {
    super(pos);
    this.targetTable = targetTable;
    this.condition = condition;
    this.sourceSelect = sourceSelect;
    this.alias = alias;
    this.from = from;
    this.using = using;
    this.targetTables = targetTables;
    this.subQueryTableMap = new HashMap<>();
    this.sourceTableColumns = new ArrayList<>();
  }

  public SqlDelete(
      SqlParserPos pos,
      SqlNode targetTable,
      SqlNode condition,
      SqlSelect sourceSelect,
      SqlIdentifier alias,
      SqlNode from,
      SqlNode using,
      SqlNodeList targetTables,
      SqlNodeList orderList,
      SqlNode fetch,
      SqlNodeList keywords) {
    this(pos, targetTable, condition, sourceSelect, alias, from, using, targetTables);
    this.orderList = orderList;
    this.fetch = fetch;
    this.keywords = keywords;
  }

  public SqlDelete(
      SqlParserPos pos,
      SqlNode targetTable,
      SqlNode condition,
      SqlSelect sourceSelect,
      SqlIdentifier alias,
      SqlNode from,
      SqlNode using,
      SqlNodeList targetTables,
      SqlNodeList orderList,
      SqlNode fetch,
      SqlNodeList keywords,
      SqlNodeList hints) {
    this(pos,
        targetTable,
        condition,
        sourceSelect,
        alias,
        from,
        using,
        targetTables,
        orderList,
        fetch,
        keywords);
    this.hints = hints;
  }

public SqlDelete(
    SqlParserPos pos,
    SqlNode targetTable,
    SqlNode condition,
    SqlSelect sourceSelect,
    SqlIdentifier alias,
    SqlNode from,
    SqlNode using,
    SqlNodeList targetTables,
    SqlNodeList orderList,
    SqlNode offset,
    SqlNode fetch,
    SqlNodeList keywords,
    SqlNodeList hints) {
    this(pos,
        targetTable,
        condition,
        sourceSelect,
        alias,
        from,
        using,
        targetTables,
        orderList,
        fetch,
        keywords);
    this.hints = hints;
    this.offset = offset;
}

  public SqlDelete(
      SqlParserPos pos,
      SqlNode targetTable,
      SqlNode condition,
      SqlSelect sourceSelect,
      SqlIdentifier alias,
      SqlNode from,
      SqlNode using,
      SqlNodeList targetTables,
      SqlNodeList orderList,
      SqlNode fetch,
      SqlNodeList keywords,
      SqlNodeList hints,
      SqlNodeList sourceTables,
      SqlNodeList aliases,
      boolean withTableAlias) {
    this(pos, targetTable, condition, sourceSelect, alias, from, using, targetTables, orderList, fetch, keywords, hints);
    this.sourceTables = sourceTables;
    this.aliases = aliases;
    this.withTableAlias = withTableAlias;
  }

  private SqlDelete(SqlParserPos pos, SqlNode targetTable, SqlNode condition, SqlSelect sourceSelect,
                    SqlIdentifier alias, SqlNode from, SqlNode using, SqlNodeList targetTables,
                    SqlNodeList sourceTables, SqlNodeList aliases, SqlNodeList orderList, SqlNode offset,
                    SqlNode fetch, SqlNodeList keywords, SqlNodeList hints, boolean withTableAlias,
                    Map<SqlNode, List<SqlIdentifier>> subQueryTableMap,
                    List<Pair<SqlNode, List<SqlNode>>> sourceTableColumns, boolean sourceTableCollected) {
    super(pos);
    this.targetTable = targetTable;
    this.condition = condition;
    this.sourceSelect = sourceSelect;
    this.alias = alias;
    this.from = from;
    this.using = using;
    this.targetTables = targetTables;
    this.sourceTables = sourceTables;
    this.aliases = aliases;
    this.orderList = orderList;
    this.offset = offset;
    this.fetch = fetch;
    this.keywords = keywords;
    this.hints = hints;
    this.withTableAlias = withTableAlias;
    this.subQueryTableMap = subQueryTableMap;
    this.sourceTableColumns = sourceTableColumns;
    this.sourceTableCollected = sourceTableCollected;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.DELETE;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(targetTable,
            condition,
            alias,
            orderList,
            fetch,
            keywords,
            from,
            using,
            targetTables,
            sourceTables,
            aliases,
            offset);
  }

  /**
   * only few of operands should be considered, only need get {6, 7, 8, 1, 4, 5};
   */
  public List<SqlNode> getParameterizableOperandList() {
    return ImmutableNullableList.of(from,
        using,
        targetTables,
        condition,
        fetch,
        keywords,
        offset);
  }
  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      targetTable = operand;
      break;
    case 1:
      condition = operand;
      break;
    case 2:
      sourceSelect = (SqlSelect) operand;
      break;
    case 3:
      alias = (SqlIdentifier) operand;
      break;
    case 4:
      orderList = (SqlNodeList) operand;
      break;
    case 5:
      fetch = operand;
      break;
    case 6:
      keywords = (SqlNodeList) operand;
      break;
    case 7:
      from = operand;
      break;
    case 8:
      using = operand;
      break;
    case 9:
      targetTables = (SqlNodeList) operand;
      break;
    case 10:
      sourceTables = (SqlNodeList) operand;
      break;
    case 11:
      aliases = (SqlNodeList) operand;
      break;
    case 12:
      offset = operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  /**
   * @return the identifier for the target table of the deletion
   */
  public SqlNode getTargetTable() {
    return targetTable;
  }

  /**
   * @return the alias for the target table of the deletion
   */
  public SqlIdentifier getAlias() {
    return alias;
  }

  /**
   * Gets the filter condition for rows to be deleted.
   *
   * @return the condition expression for the data to be deleted, or null for
   * all rows in the table
   */
  public SqlNode getCondition() {
    return condition;
  }

  public void setCondition(SqlNode condition) {
    this.condition = condition;
  }

  /**
   * Gets the source SELECT expression for the data to be deleted. This
   * returns null before the condition has been expanded by
   * {@link SqlValidatorImpl#performUnconditionalRewrites(SqlNode, boolean)}.
   *
   * @return the source SELECT for the data to be inserted
   */
  public SqlSelect getSourceSelect() {
    return sourceSelect;
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

  public void setFrom(SqlNode from) {
    this.from = from;
  }

  public SqlNode getFrom() {
    return from;
  }

  public void setUsing(SqlNode using) {
    this.using = using;
  }

  public SqlNode getUsing() {
    return using;
  }

  public SqlNodeList getTargetTables() {
    return targetTables;
  }

  public SqlNodeList getSourceTables() {
    Assert.assertTrue(sourceTableCollected, "SqlDelete not initialized");
    return sourceTables;
  }

  public SqlNodeList getAliases() {
    Assert.assertTrue(sourceTableCollected, "SqlDelete not initialized");
    return aliases;
  }

  public void setWithTableAlias(boolean withTableAlias) {
    this.withTableAlias = withTableAlias;
  }

  public boolean isWithTableAlias() {
    Assert.assertTrue(sourceTableCollected, "SqlDelete not initialized");
    return withTableAlias;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SELECT, "DELETE", "");

    if (keywords != null) {
      for (SqlNode keyword : keywords) {
        keyword.unparse(writer, 0, 0);
      }
    }

    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();

    if (singleTable()) {
      /**
       * <pre>
       * DELETE t1
       * DELETE FROM t1
       * </pre>
       */
      writer.keyword("FROM");
      targetTable.unparse(writer, opLeft, opRight);
      if (alias != null) {
        writer.keyword("AS");
        alias.unparse(writer, opLeft, opRight);
      }
    } else if (fromUsing()) {
      /**
       * <pre>
       * DELETE FROM t1 USING t1
       * DELETE FROM t1 USING table1 as t1
       * DELETE FROM t1 USING t1 JOIN t2
       * DELETE FROM t1 USING table1 t1 JOIN table2 t2
       * DELETE FROM t1,t2 USING table1 t1 JOIN table2 t2
       * </pre>
       */
      writer.keyword("FROM");
      targetTables.unparse(writer, 0, 0);
      writer.keyword("USING");
      using.unparse(writer, opLeft, opRight);
    } else if (null != from){
      /**
       * <pre>
       * DELETE t1 FROM table1 AS t1
       * DELETE t1 FROM t1 JOIN t2
       * DELETE t1 FROM table1 t1 JOIN table2 t2
       * DELETE t1,t2 FROM table1 t1 JOIN table2 t2
       * </pre>
       */
      targetTables.unparse(writer, leftPrec, rightPrec);

      writer.keyword("FROM");

      final SqlWriter.Frame fromFrame =
          writer.startList(SqlWriter.FrameTypeEnum.FROM_LIST);
      if (writer instanceof SqlPrettyWriter) {
        ((SqlPrettyWriter) writer).pushOptStack(SqlWriter.FrameTypeEnum.FROM_LIST);
      }
      from.unparse(writer, opLeft, opRight);
      if (writer instanceof SqlPrettyWriter) {
        ((SqlPrettyWriter) writer).popOptStack();
      }
      writer.endList(fromFrame);
    }

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

  public boolean fromUsing() {
    return null != using && targetTables.size() > 0;
  }

  /**
   * For 'DELETE t1 FROM table1 as t1' return false
   * @return true if only one table referred and with no alias
   */
  public boolean singleTable() {
    if (getSourceTables().size() < 1) {
      return true;
    } else if (getSourceTables().size() == 1) {
      return !withTableAlias;
    }

    return false;
  }

  public SqlNode getSourceTableNode() {
    if (fromUsing()) {
      return using;
    } else {
      return from;
    }
  }

  public static boolean collectSourceTable(SqlNode sourceTableNode, List<SqlNode> outSourceTables, List<SqlNode> outAliases, boolean withTableAlias) {
    if (null == sourceTableNode) {
      return withTableAlias;
    }

    /**
     * <pre>
     * For 'DELETE t1 WHERE id = 1' sourceTableNode is NULL
     * For other cases, see below
     * </pre>
     */
    if (sourceTableNode instanceof SqlIdentifier) {
      // DELETE t1 FROM t1
      outSourceTables.add(sourceTableNode);
      outAliases.add(sourceTableNode);
    } else if (sourceTableNode instanceof SqlBasicCall && sourceTableNode.getKind() == SqlKind.AS) {
      // DELETE t1 FROM t AS t1
      // DELETE FROM t1 USING t AS t1
      outSourceTables.add(((SqlBasicCall)sourceTableNode).operands[0]);
      outAliases.add(sourceTableNode);
      withTableAlias = true;
    } else if (sourceTableNode instanceof SqlJoin) {
      // DELETE t1 FROM t AS t1 JOIN t AS t2
      // DELETE FROM t1 USING t AS t1 JOIN t AS t2
      final SqlJoin join = (SqlJoin)sourceTableNode;
      withTableAlias |= collectSourceTable(join.left, outSourceTables, outAliases, withTableAlias);
      withTableAlias |= collectSourceTable(join.right, outSourceTables, outAliases, withTableAlias);
    }

    return withTableAlias;
  }

  public SqlDelete initTableInfo(SqlNodeList sourceTables, SqlNodeList aliases,
                                 Map<SqlNode, List<SqlIdentifier>> subQueryTableMap,
                                 boolean withTableAlias) {
    this.sourceTables = sourceTables;
    this.aliases = aliases;
    this.subQueryTableMap = subQueryTableMap;
    this.sourceTableCollected = true;
    this.withTableAlias = withTableAlias;

    return this;
  }

  public SqlDelete initTableInfo(TableInfo srcInfo) {
    final List<SqlNode> sourceTables = new ArrayList<>();
    final List<SqlNode> aliases = new ArrayList<>();
    final Map<SqlNode, List<SqlIdentifier>> subQueryTableMap = new HashMap<>();

    final AtomicBoolean withAlias = new AtomicBoolean(false);
    srcInfo.getSrcInfos().forEach(sti -> {
      sourceTables.add(sti.getTable());
      final SqlNode alias = sti.getTableWithAlias();
      withAlias.set(withAlias.get() & (alias instanceof SqlBasicCall && alias.getKind() == SqlKind.AS));
      aliases.add(alias);
      subQueryTableMap.put(sti.getTable(),
          sti.getRefTables()
              .stream()
              .map(t -> new SqlIdentifier(t.getQualifiedName(), SqlParserPos.ZERO))
              .collect(Collectors.toList()));
    });

    this.sourceTables = new SqlNodeList(sourceTables, SqlParserPos.ZERO);
    this.aliases = new SqlNodeList(aliases, SqlParserPos.ZERO);
    this.subQueryTableMap = subQueryTableMap;
    this.withTableAlias = withAlias.get();

    this.sourceTableCollected = true;

    return this;
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateDelete(this);
  }

  public void setSourceSelect(SqlSelect sourceSelect) {
    this.sourceSelect = sourceSelect;
  }

  @Override
  public SqlNodeList getHints() {
    return hints;
  }

  @Override
  public void setHints(SqlNodeList hints) {
    this.hints = hints;
  }

  public boolean withSubquery() {
    return getSourceTables().getList().stream().anyMatch(t -> t instanceof SqlSelect);
  }

  public List<SqlIdentifier> subQueryTables(SqlNode subQuery) {
    Assert.assertTrue(sourceTableCollected, "SqlDelete not initialized");
    return subQueryTableMap.get(subQuery);
  }

  public Map<SqlNode, List<SqlIdentifier>> getSubQueryTableMap() {
    return subQueryTableMap;
  }

  public List<Pair<SqlNode, List<SqlNode>>> getSourceTableColumns() {
    return sourceTableColumns;
  }

  public void setSourceTableColumns(List<Pair<SqlNode, List<SqlNode>>> sourceTableColumns) {
    this.sourceTableColumns = sourceTableColumns;
  }

  @Override
  public SqlNode clone(SqlParserPos pos) {
    return new SqlDelete(pos, getTargetTable(), getCondition(), getSourceSelect(), getAlias(), getFrom(), getUsing(),
            getTargetTables(), getSourceTables(), getAliases(), getOrderList(), getOffset(), getFetch(), getKeywords(),
            getHints(), isWithTableAlias(), this.subQueryTableMap, this.sourceTableColumns, this.sourceTableCollected);
  }
}

// End SqlDelete.java
