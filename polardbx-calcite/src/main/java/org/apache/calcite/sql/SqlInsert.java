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

import java.util.List;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * A <code>SqlInsert</code> is a node of a parse tree which represents an INSERT
 * statement.
 */
public class SqlInsert extends SqlCall implements SqlHint {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("INSERT", SqlKind.INSERT);

  SqlNodeList keywords;
  SqlNode targetTable;
  SqlNode source;
  SqlNodeList columnList;
  SqlNodeList updateList;
  SqlSelect updateSelect;
  int batchSize;
  SqlNodeList hints;

  //~ Constructors -----------------------------------------------------------

  public SqlInsert(SqlParserPos pos,
                   SqlNodeList keywords,
                   SqlNode targetTable,
                   SqlNode source,
                   SqlNodeList columnList) {
    this(pos, keywords, targetTable, source, columnList, new SqlNodeList(pos), 0, new SqlNodeList(pos));
  }

  public SqlInsert(SqlParserPos pos,
      SqlNodeList keywords,
      SqlNode targetTable,
      SqlNode source,
      SqlNodeList columnList,
      int batchSize,
      SqlNodeList hints) {
	 this(pos, keywords, targetTable, source, columnList, new SqlNodeList(pos), batchSize, hints);
  }

  public SqlInsert(SqlParserPos pos,
      SqlNodeList keywords,
      SqlNode targetTable,
      SqlNode source,
      SqlNodeList columnList,
      SqlNodeList updateList,
      int batchSize,
      SqlNodeList hints){
    super(pos);
    this.keywords = keywords;
    this.targetTable = targetTable;
    this.source = source;
    this.columnList = columnList;
    this.updateList = updateList;
    this.batchSize = batchSize;
    assert keywords != null;
    this.hints = hints;
  }

  //~ Methods ----------------------------------------------------------------

    public static SqlInsert create(SqlKind sqlKind, SqlParserPos pos, SqlNodeList keywords, SqlNode targetTable,
                                   SqlNode source, SqlNodeList columnList, SqlNodeList updateList, int batchSize,
                                   SqlNodeList hints) {
        if (sqlKind == SqlKind.INSERT) {
            return new SqlInsert(pos, keywords, targetTable, source, columnList, updateList, batchSize, hints);
        } else {
            return new SqlReplace(pos, keywords, targetTable, source, columnList, batchSize, hints);
        }
    }

  @Override public SqlKind getKind() {
    return SqlKind.INSERT;
  }

  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(keywords, targetTable, source, columnList, updateList);
  }

  /**
   * Returns whether this is an UPSERT statement.
   *
   * <p>In SQL, this is represented using the {@code UPSERT} keyword rather than
   * {@code INSERT}; in the abstract syntax tree, an UPSERT is indicated by the
   * presence of a {@link SqlDmlKeyword#UPSERT} keyword.
   */
  public final boolean isUpsert() {
    return getModifierNode(SqlDmlKeyword.UPSERT) != null;
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      keywords = (SqlNodeList) operand;
      break;
    case 1:
      assert operand instanceof SqlIdentifier;
      targetTable = operand;
      break;
    case 2:
      source = operand;
      break;
    case 3:
      columnList = (SqlNodeList) operand;
      break;
    case 4:
      updateList = (SqlNodeList) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

    public SqlNodeList getKeywords() {
        return keywords;
    }

  /**
   * @return the identifier for the target table of the insertion
   */
  public SqlNode getTargetTable() {
    return targetTable;
  }

  /**
   * @return the source expression for the data to be inserted
   */
  public SqlNode getSource() {
    return source;
  }

  public void setSource(SqlNode source) {
    this.source = source;
  }

  /**
   * @return the list of target column names, or null for all columns in the
   * target table
   */
  public SqlNodeList getTargetColumnList() {
    return columnList;
  }

  public SqlNodeList getUpdateColumnList() {
    SqlNodeList colList = new SqlNodeList(SqlParserPos.ZERO);

    if (updateList != null) {
      for (SqlNode node : updateList) {
        if (!(node instanceof SqlCall)) {
          continue;
        }
        SqlCall call = (SqlCall) node;
        colList.add(call.getOperandList().get(0));
      }
    }

    return colList;
  }

  public void setUpdateSelect(SqlSelect sqlSelect) {
    this.updateSelect = sqlSelect;
  }

  public SqlSelect getUpdateSelect() {
    return updateSelect;
  }

  public SqlNodeList getUpdateList() {
    return updateList;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public final SqlNode getModifierNode(SqlDmlKeyword modifier) {
    for (SqlNode keyword : keywords) {
      SqlDmlKeyword keyword2 =
          ((SqlLiteral) keyword).symbolValue(SqlDmlKeyword.class);
      if (keyword2 == modifier) {
        return keyword;
      }
    }
    return null;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.startList(SqlWriter.FrameTypeEnum.SELECT);
    writer.sep(isUpsert() ? "UPSERT" : "INSERT");
    for (SqlNode keyword : keywords) {
        keyword.unparse(writer, 0, 0);
    }
    writer.sep("INTO");
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    targetTable.unparse(writer, opLeft, opRight);
    if (columnList != null) {
      columnList.unparse(writer, opLeft, opRight);
    }
    writer.newlineAndIndent();
    source.unparse(writer, 0, 0);

    if (updateList.size() > 0) {
        writer.sep("ON DUPLICATE KEY UPDATE");
        final SqlWriter.Frame frame = writer.startList("", "");
        final SqlDialect dialect = writer.getDialect();
        for (SqlNode node : updateList) {
            writer.sep(",");
            dialect.unparseCall(writer, (SqlCall) node, 0, 0);
        }
        writer.endList(frame);
    }
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateInsert(this);
  }

  @Override
  public SqlNodeList getHints() {
    return hints;
  }

  @Override
  public void setHints(SqlNodeList hints) {
    this.hints = hints;
  }

  @Override public SqlNode clone(SqlParserPos pos) {
      return create(this.getKind(), pos, this.keywords, this.targetTable,
          this.source, this.columnList, this.updateList, this.batchSize,
          this.hints);
  }


}

// End SqlInsert.java
