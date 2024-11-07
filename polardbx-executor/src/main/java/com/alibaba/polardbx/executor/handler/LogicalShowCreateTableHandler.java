/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLCurrentTimeExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.repo.mysql.common.ResultSetHelper;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author mengshi
 */
public class LogicalShowCreateTableHandler extends HandlerCommon {

    private final LogicalShowCreateTablesForPartitionDatabaseHandler partitionDatabaseHandler;
    private final LogicalShowCreateTablesForShardingDatabaseHandler shardingDatabaseHandler;

    public LogicalShowCreateTableHandler(IRepository repo) {
        super(repo);
        partitionDatabaseHandler = new LogicalShowCreateTablesForPartitionDatabaseHandler(repo);
        shardingDatabaseHandler = new LogicalShowCreateTablesForShardingDatabaseHandler(repo);

    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalShow show = (LogicalShow) logicalPlan;

        String schemaName = show.getSchemaName();

        if (schemaName == null) {
            schemaName = executionContext.getSchemaName();
        }

        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            return partitionDatabaseHandler.handle(logicalPlan, executionContext);
        } else {
            return shardingDatabaseHandler.handle(logicalPlan, executionContext);
        }
    }

    public static String reorgLogicalColumnOrder(String schemaName, String logicalTableName, String sql) {
        if (!ConfigDataMode.isPolarDbX()) {
            return sql;
        }

        // Always show columns in logical column order no matter what the mode.
        List<ColumnsRecord> logicalColumnsInOrder =
            ResultSetHelper.fetchLogicalColumnsInOrder(schemaName, logicalTableName);

        if (GeneralUtil.isEmpty(logicalColumnsInOrder)) {
            return sql;
        }

        List<SQLTableElement> newTableElements = new ArrayList<>();

        final MySqlCreateTableStatement createTableStmt =
            (MySqlCreateTableStatement) SQLUtils.parseStatementsWithDefaultFeatures(sql, JdbcConstants.MYSQL).get(0)
                .clone();

        for (SQLTableElement tableElement : createTableStmt.getTableElementList()) {
            if (tableElement instanceof MysqlForeignKey) {
                if (((MysqlForeignKey) tableElement).getName() != null) {
                    ((MysqlForeignKey) tableElement).setHasConstraint(true);
                }
            }
        }

        for (ColumnsRecord logicalColumn : logicalColumnsInOrder) {
            for (SQLTableElement tableElement : createTableStmt.getTableElementList()) {
                if (tableElement instanceof SQLColumnDefinition) {
                    String physicalColumnName =
                        SQLUtils.normalizeNoTrim(((SQLColumnDefinition) tableElement).getColumnName());
                    if (TStringUtil.equalsIgnoreCase(physicalColumnName, logicalColumn.columnName)) {
                        newTableElements.add(tableElement);
                        break;
                    }
                }
            }
        }

        for (SQLTableElement tableElement : createTableStmt.getTableElementList()) {
            if (!(tableElement instanceof SQLColumnDefinition)) {
                newTableElements.add(tableElement);
            }
        }

        createTableStmt.getTableElementList().clear();
        createTableStmt.getTableElementList().addAll(newTableElements);

        return createTableStmt.toString();
    }

    public static MySqlCreateTableStatement fetchShowCreateTableFromMetaDb(String schemaName, String tableName,
                                                                           ExecutionContext executionContext,
                                                                           TableMeta tableMeta) {
        final MySqlCreateTableStatement createTableStmt = new MySqlCreateTableStatement();
        createTableStmt.setTableName(tableName);
        // Always show columns in logical column order no matter what the mode.
        List<ColumnsRecord> logicalColumnsInOrder =
            ResultSetHelper.fetchLogicalColumnsInOrder(schemaName, tableName);

        List<String> primaryKeys = new ArrayList<>();
        for (ColumnsRecord logicalColumn : logicalColumnsInOrder) {
            SQLColumnDefinition sqlColumnDefinition = new SQLColumnDefinition();
            sqlColumnDefinition.setName(SqlIdentifier.surroundWithBacktick(logicalColumn.columnName));
            sqlColumnDefinition.setDbType(DbType.mysql);
            MySqlExprParser parser = new MySqlExprParser(ByteString.from(logicalColumn.columnType));
            sqlColumnDefinition.setDataType(parser.parseDataType());

            if (logicalColumn.columnDefault != null) {
                SQLExpr sqlExpr = SQLUtils.toDefaultSQLExpr(
                    sqlColumnDefinition.getDataType(), logicalColumn.columnDefault, logicalColumn.flag);
                if (sqlExpr != null) {
                    sqlColumnDefinition.setDefaultExpr(sqlExpr);
                }

                if (sqlExpr instanceof SQLCurrentTimeExpr) {
                    if (!StringUtils.isEmpty(logicalColumn.extra) && logicalColumn.extra.toUpperCase(Locale.ROOT)
                        .contains("ON UPDATE")) {
                        sqlColumnDefinition.setOnUpdate(sqlExpr);
                    }
                }
            }

            if (!StringUtils.isEmpty(logicalColumn.columnComment)) {
                sqlColumnDefinition.setComment(logicalColumn.columnComment);
            }

            if ("NO".equalsIgnoreCase(logicalColumn.isNullable)) {
                sqlColumnDefinition.addConstraint(new SQLNotNullConstraint());
            }

            if ("PRI".equalsIgnoreCase(logicalColumn.columnKey)) {
                primaryKeys.add(SqlIdentifier.surroundWithBacktick(logicalColumn.columnName));
            }
            boolean autoIncrement = TStringUtil.equalsIgnoreCase(logicalColumn.extra, "auto_increment");
            if (autoIncrement) {
                sqlColumnDefinition.setAutoIncrement(true);
            }
            createTableStmt.addColumn(sqlColumnDefinition);
        }

        //PRIMARY
        List<SQLSelectOrderByItem> colNames = primaryKeys.stream()
            .map(e -> new SQLSelectOrderByItem(new SQLIdentifierExpr(e)))
            .collect(Collectors.toList());
        MySqlPrimaryKey newPrimaryKey = new MySqlPrimaryKey();
        SQLIndexDefinition indexDefinition = newPrimaryKey.getIndexDefinition();
        indexDefinition.setKey(true);
        indexDefinition.setType("PRIMARY");
        indexDefinition.getColumns().addAll(colNames);
        createTableStmt.getTableElementList().add(newPrimaryKey);

        TablesRecord tablesRecord =
            ResultSetHelper.fetchLogicalTableRecord(schemaName, tableName);
        if (!StringUtils.isEmpty(tablesRecord.engine)) {
            createTableStmt.addOption("ENGINE", new SQLIdentifierExpr(tablesRecord.engine));
        }

        if (!StringUtils.isEmpty(tablesRecord.tableCollation)) {
            String tableCharacterSet = Optional.ofNullable(tablesRecord.tableCollation)
                .map(CollationName::getCharsetOf)
                .map(Enum::name)
                .orElse(CharsetName.DEFAULT_CHARACTER_SET);
            createTableStmt.addOption("CHARACTER SET", new SQLIdentifierExpr(tableCharacterSet));
            createTableStmt.addOption("COLLATE", new SQLIdentifierExpr(tablesRecord.tableCollation));
        }
        return createTableStmt;
    }

}
