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
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * create table with select
 *
 * Created by yiminglc on 17/1/4.
 */
public class DSqlCreateTable extends SqlCall {

  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CREATE", SqlKind.CREATE_TABLE);
  SqlNode query;
  SqlIdentifier tableName;
  SqlNodeList columnList;

  public DSqlCreateTable(
      SqlParserPos pos,
      SqlNode query,
      SqlIdentifier tableName,
      SqlNodeList columnList) {
    super(pos);
    this.query = query;
    this.tableName = tableName;
    this.columnList = columnList;
  }


  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(tableName, query);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("TABLE");
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    tableName.unparse(writer, opLeft, opRight);
    if (columnList != null) {
      String columnString = "(";
      for (int i = 0; i < columnList.size(); i += 2) {
        columnString += columnList.get(i) + " " + columnList.get(i + 1);
        if (i != columnList.size() - 2) {
          columnString += ',';
        } else {
          columnString += ')';
        }
      }
      writer.print(columnString);
    }
    if (query != null) {
      query.unparse(writer, opLeft, opRight);
    }
  }

  public SqlNode getQuery() {
    return query;
  }

  public SqlIdentifier getTableName() {
    return tableName;
  }

  public SqlNodeList getColumnList() {
    return columnList;
  }
}

// End DSqlCreateTable.java
