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

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.sql.SqlAlterTable.ColumnOpt;
import org.apache.calcite.sql.SqlColumnDeclaration.ColumnFormat;
import org.apache.calcite.sql.SqlColumnDeclaration.ColumnNull;
import org.apache.calcite.sql.SqlColumnDeclaration.SpecialIndex;
import org.apache.calcite.sql.SqlColumnDeclaration.Storage;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Map;

/**
 * Utilities concerning {@link SqlNode} for DDL.
 */
public class SqlDdlNodes {
  private SqlDdlNodes() {}

  public static SqlCreateDatabase createDatabase(SqlParserPos pos, boolean ifNotExists, SqlIdentifier dbName,
                                                 String charSet, String collate, String locality, String partitionMode) {
    return new SqlCreateDatabase(pos, ifNotExists, dbName, charSet, collate, locality, partitionMode);
  }

  public static SqlDropDatabase dropDatabase(SqlParserPos pos, boolean ifExists, SqlIdentifier dbName) {
    return new SqlDropDatabase(pos, ifExists, dbName);
  }

  /** Creates a CREATE TABLE. */
  public static SqlCreateTable createTable(SqlParserPos pos, boolean replace,
                                           boolean ifNotExists, SqlNode name,
                                           SqlNode likeTableName, SqlNodeList columnList,
                                           SqlNode query, SqlNode dbpartitionBy, SqlNode dbpartitions,
                                           SqlNode tbpartitionBy, SqlNode tbpartitions, String sourceSql,
                                           boolean broadcast, SequenceBean sequence, SqlNode sqlPartition, SqlNode tableGroupName) {
        return new SqlCreateTable(pos,
            replace,
            ifNotExists,
            name,
            likeTableName,
            columnList,
            query, dbpartitionBy, dbpartitions, tbpartitionBy, tbpartitions, sourceSql, broadcast, sequence,
            sqlPartition, tableGroupName);
  }

  /** Creates a CREATE VIEW. */
  public static SqlCreateView createView(SqlParserPos pos, boolean replace,
                                         SqlIdentifier name, SqlNodeList columnList, SqlNode query) {
    return new SqlCreateView(pos, replace, name, columnList, query);
  }

  /** Creates a DROP TABLE. */
  public static SqlDropTable dropTable(SqlParserPos pos, boolean ifExists,
                                       SqlIdentifier name, boolean purge) {
    return new SqlDropTable(pos, ifExists, name, purge);
  }

  /** Truncate a TABLE. */
  public static SqlTruncateTable truncateTable(SqlParserPos pos, boolean ifExists,
                                       SqlIdentifier name ,boolean purge) {
    return new SqlTruncateTable(pos, ifExists, name, purge);
  }

  public static SqlDropIndex dropIndex(SqlIdentifier name , SqlIdentifier tableName , String sql, SqlParserPos pos) {
    return new SqlDropIndex(name, tableName , sql, pos);
  }

  public static SqlAlterTableDropIndex alterTabledropIndex(SqlIdentifier tableName , SqlIdentifier indexName , String sql, SqlParserPos pos) {
    return new SqlAlterTableDropIndex(tableName, indexName , sql, pos);
  }

  public static SqlCreateSequence createSequence(SqlCharStringLiteral name , SqlIdentifier tableName , String sql, SqlParserPos pos) {
    return new SqlCreateSequence(name, tableName , sql, pos);
  }

  public static SqlAlterSequence alterSequence(SqlCharStringLiteral name , SqlIdentifier tableName , String sql, SqlParserPos pos) {
    return new SqlAlterSequence(name, tableName , sql, pos);
  }

  public static SqlDropSequence dropSequence(SqlCharStringLiteral name , SqlIdentifier tableName , String sql, SqlParserPos pos) {
    return new SqlDropSequence(name, tableName , sql, pos);
  }

  public static SqlRenameSequence renameSequence(SqlCharStringLiteral name , SqlCharStringLiteral to,SqlIdentifier tableName , String sql, SqlParserPos pos) {
    return new SqlRenameSequence(name, to, tableName , sql, pos);
  }

  public static SqlRenameTable renameTable(SqlIdentifier to , SqlIdentifier tableName , String sql, SqlParserPos pos) {
    return new SqlRenameTable(to, tableName , sql, pos);
  }

  public static SqlAlterTable alterTable(SqlIdentifier tableName, Map<ColumnOpt, List<String>> columnOpts,
                                         String sql, SqlTableOptions tableOptions,
                                         List<SqlAlterSpecification> alters,
                                         SqlParserPos pos) {
    return new SqlAlterTable(tableName ,columnOpts, sql, tableOptions, alters, pos);
  }

  public static SqlAlterRule alterRule(SqlIdentifier tableName, String sql, SqlParserPos pos) {
    return new SqlAlterRule(tableName, sql, pos);
  }

  public static SqlAlterTableSetTableGroup alterTableSetTableGroup(SqlIdentifier tableName, String targetTableGroup,
                                                                   String sql, SqlParserPos pos) {
    return new SqlAlterTableSetTableGroup(tableName, targetTableGroup, sql, pos);
  }

  /** Creates a column declaration. */
    public static SqlNode column(SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, ColumnNull notNull,
                                 SqlLiteral defaultVal, SqlCall defaultExpr, boolean autoIncrement,
                                 SpecialIndex specialIndex, SqlLiteral comment, ColumnFormat columnFormat,
                                 Storage storage, SqlReferenceDefinition referenceDefinition,
                                 boolean onUpdateCurrentTimestamp, Type autoIncrementType, int unitCount,
                                 int unitIndex, int innerStep) {
        return new SqlColumnDeclaration(pos,
            name,
            dataType,
            notNull,
            defaultVal,
            defaultExpr,
            autoIncrement,
            specialIndex,
            comment,
            columnFormat,
            storage,
            referenceDefinition,
            onUpdateCurrentTimestamp,
            autoIncrementType,
            unitCount,
            unitIndex,
            innerStep);
    }

  /** Returns the schema in which to create an object. */
  static Pair<CalciteSchema, String> schema(CalcitePrepare.Context context,
                                            boolean mutable, SqlIdentifier id) {
    final String name;
    final List<String> path;
    if (id.isSimple()) {
      path = context.getDefaultSchemaPath();
      name = id.getSimple();
    } else {
      path = Util.skipLast(id.names);
      name = Util.last(id.names);
    }
    CalciteSchema schema = mutable ? context.getMutableRootSchema()
        : context.getRootSchema();
    for (String p : path) {
      schema = schema.getSubSchema(p, true);
    }
    return Pair.of(schema, name);
  }

  /** Wraps a query to rename its columns. Used by CREATE VIEW and CREATE
   * MATERIALIZED VIEW. */
  static SqlNode renameColumns(SqlNodeList columnList, SqlNode query) {
    if (columnList == null) {
      return query;
    }
    final SqlParserPos p = query.getParserPosition();
    final SqlNodeList selectList =
        new SqlNodeList(ImmutableList.<SqlNode>of(SqlIdentifier.star(p)), p);
    final SqlCall from =
        SqlStdOperatorTable.AS.createCall(p,
            ImmutableList.<SqlNode>builder()
                .add(query)
                .add(new SqlIdentifier("_", p))
                .addAll(columnList)
                .build());
    return new SqlSelect(p, null, selectList, from, null, null, null, null,
        null, null, null);
  }

}

// End SqlDdlNodes.java
