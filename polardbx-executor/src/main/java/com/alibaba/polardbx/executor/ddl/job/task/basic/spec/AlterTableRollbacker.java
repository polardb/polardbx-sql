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

package com.alibaba.polardbx.executor.ddl.job.task.basic.spec;

import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropPrimaryKey;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRenameColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRenameIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLConstraint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableAlterColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableOption;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.ddl.job.meta.delegate.TableInfoManagerDelegate;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.clearspring.analytics.util.Lists;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class AlterTableRollbacker {

    public static boolean checkIfRollbackable(String origSql) {
        SQLStatement sqlStatement = FastsqlUtils.parseSql(origSql).get(0);
        if (sqlStatement instanceof SQLAlterTableStatement) {
            return checkIfRollbackable((SQLAlterTableStatement) sqlStatement);
        } else {
            return true;
        }
    }

    public static boolean checkIfRollbackable(SQLAlterTableStatement alterTableStmt) {
//        if (alterTableStmt.getPartition() != null) {
//            return false;
//        }

        // alter operations
        EnumSet<AlterTableOperation> alterOperations = EnumSet.noneOf(AlterTableOperation.class);
        if (alterTableStmt.getItems() != null && !alterTableStmt.getItems().isEmpty()) {
            for (SQLAlterTableItem alterItem : alterTableStmt.getItems()) {
                if (alterItem instanceof SQLAlterTableAddColumn) {
                    alterOperations.add(AlterTableOperation.ADD_COLUMN);
                } else if (alterItem instanceof SQLAlterTableAddIndex) {
                    alterOperations.add(AlterTableOperation.ADD_INDEX);
                } else if (alterItem instanceof SQLAlterTableAddConstraint) {
                    SQLConstraint constraint = ((SQLAlterTableAddConstraint) alterItem).getConstraint();
                    if (constraint instanceof MySqlPrimaryKey) {
                        alterOperations.add(AlterTableOperation.ADD_PRIMARY_KEY);
                    } else if (constraint instanceof MySqlUnique) {
                        alterOperations.add(AlterTableOperation.ADD_INDEX);
                    } else {
                        alterOperations.add(AlterTableOperation.OTHERS);
                    }
                } else if (alterItem instanceof SQLAlterTableRenameColumn) {
                    alterOperations.add(AlterTableOperation.RENAME_COLUMN);
                } else if (alterItem instanceof SQLAlterTableRenameIndex) {
                    alterOperations.add(AlterTableOperation.RENAME_INDEX);
                } else if (alterItem instanceof MySqlAlterTableAlterColumn) {
                    MySqlAlterTableAlterColumn alterColumn = (MySqlAlterTableAlterColumn) alterItem;
                    if (alterColumn.isDropDefault()) {
                        alterOperations.add(AlterTableOperation.ALTER_COLUMN_DROP_DEFAULT);
                    } else if (alterColumn.getDefaultExpr() != null) {
                        alterOperations.add(AlterTableOperation.ALTER_COLUMN_SET_DEFAULT);
                    } else {
                        alterOperations.add(AlterTableOperation.OTHERS);
                    }
                } else if (alterItem instanceof MySqlAlterTableOption) {
                    MySqlAlterTableOption alterTableOption = (MySqlAlterTableOption) alterItem;
                    if (TStringUtil.equalsIgnoreCase(alterTableOption.getName(), "ALGORITHM")) {
                        alterOperations.add(AlterTableOperation.ALGORITHM);
                    } else {
                        alterOperations.add(AlterTableOperation.OTHERS);
                    }
                } else {
                    alterOperations.add(AlterTableOperation.OTHERS);
                }
            }
        }

        // table options
        boolean hasNoTableOption =
            alterTableStmt.getTableOptions() == null || alterTableStmt.getTableOptions().isEmpty();

        return AlterTableOperation.areAllOperationsRollbackable(alterOperations) && hasNoTableOption;
    }

    public static List<SQLAlterTableItem> reverse(String schemaName, String tableName, SQLAlterTableItem alterItem) {
        if (alterItem instanceof SQLAlterTableAddColumn) {
            return reverse((SQLAlterTableAddColumn) alterItem);
        } else if (alterItem instanceof SQLAlterTableAddIndex) {
            return reverse((SQLAlterTableAddIndex) alterItem);
        } else if (alterItem instanceof SQLAlterTableAddConstraint) {
            return reverse((SQLAlterTableAddConstraint) alterItem);
        } else if (alterItem instanceof SQLAlterTableRenameColumn) {
            return reverse((SQLAlterTableRenameColumn) alterItem);
        } else if (alterItem instanceof SQLAlterTableRenameIndex) {
            return reverse((SQLAlterTableRenameIndex) alterItem);
        } else if (alterItem instanceof MySqlAlterTableAlterColumn) {
            return reverse(schemaName, tableName, (MySqlAlterTableAlterColumn) alterItem);
        } else if (alterItem instanceof MySqlAlterTableOption) {
            return reverse((MySqlAlterTableOption) alterItem);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNSUPPORTED,
                "the alter table operation '" + alterItem.getClass().getName() + "' can't be reversed for rollback");
        }
    }

    private static List<SQLAlterTableItem> reverse(SQLAlterTableAddColumn addColumn) {
        List<SQLColumnDefinition> colDefs = addColumn.getColumns();
        if (colDefs != null && !colDefs.isEmpty()) {
            List<SQLAlterTableItem> reversedItems = new ArrayList<>();
            for (SQLColumnDefinition colDef : colDefs) {
                SQLAlterTableDropColumnItem dropColumn = new SQLAlterTableDropColumnItem();
                dropColumn.addColumn(new SQLIdentifierExpr(colDef.getColumnName()));
                reversedItems.add(dropColumn);
            }
            return reversedItems;
        } else {
            return Lists.newArrayList();
        }
    }

    private static List<SQLAlterTableItem> reverse(SQLAlterTableAddIndex addIndex) {
        List<SQLAlterTableItem> reversedItems = new ArrayList<>();
        SQLAlterTableDropIndex dropIndex = new SQLAlterTableDropIndex();
        SQLName indexName = addIndex.getName();
        if (indexName == null) {
            // If user doesn't specify an index name, then the first column name
            // is used as the index name.
            indexName = (SQLIdentifierExpr) addIndex.getColumns().get(0).getExpr();
        }
        dropIndex.setIndexName(indexName);
        reversedItems.add(dropIndex);
        return reversedItems;
    }

    private static List<SQLAlterTableItem> reverse(SQLAlterTableAddConstraint addConstraint) {
        List<SQLAlterTableItem> reversedItems = new ArrayList<>();
        SQLConstraint constraint = addConstraint.getConstraint();
        if (constraint instanceof MySqlPrimaryKey) {
            SQLAlterTableDropPrimaryKey dropPrimaryKey = new SQLAlterTableDropPrimaryKey();
            reversedItems.add(dropPrimaryKey);
        } else if (constraint instanceof MySqlUnique) {
            SQLAlterTableDropIndex dropIndex = new SQLAlterTableDropIndex();
            SQLName indexName = constraint.getName();
            if (indexName == null) {
                // If user doesn't specify a unique index name, then the first column name
                // is used as the index name.
                indexName = (SQLIdentifierExpr) ((MySqlUnique) constraint).getColumns().get(0).getExpr();
            }
            dropIndex.setIndexName(indexName);
            reversedItems.add(dropIndex);
        }
        return reversedItems;
    }

    private static List<SQLAlterTableItem> reverse(SQLAlterTableRenameColumn renameColumn) {
        List<SQLAlterTableItem> reversedItems = new ArrayList<>();
        SQLAlterTableRenameColumn reversedRenameColumn = new SQLAlterTableRenameColumn();
        reversedRenameColumn.setColumn(renameColumn.getTo());
        reversedRenameColumn.setTo(renameColumn.getColumn());
        reversedItems.add(reversedRenameColumn);
        return reversedItems;
    }

    private static List<SQLAlterTableItem> reverse(SQLAlterTableRenameIndex renameIndex) {
        List<SQLAlterTableItem> reversedItems = new ArrayList<>();
        SQLAlterTableRenameIndex reversedRenameIndex = new SQLAlterTableRenameIndex();
        reversedRenameIndex.setName(renameIndex.getTo());
        reversedRenameIndex.setTo(renameIndex.getName());
        reversedItems.add(reversedRenameIndex);
        return reversedItems;
    }

    private static List<SQLAlterTableItem> reverse(String schemaName, String tableName,
                                                   MySqlAlterTableAlterColumn alterColumn) {
        List<SQLAlterTableItem> reversedItems = new ArrayList<>();
        MySqlAlterTableAlterColumn reversedAlterColumn = new MySqlAlterTableAlterColumn();

        SQLName columnNameNode = alterColumn.getColumn();
        String columnName =
            TStringUtil.replace(columnNameNode.getSimpleName(), DdlConstants.BACKTICK, DdlConstants.EMPTY_CONTENT);

        String origDefault = null;
        boolean isColumnNullable = false;

        boolean notFound = false;
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        if (tableMeta != null) {
            ColumnMeta columnMeta = tableMeta.getColumn(columnName);
            if (columnMeta != null) {
                origDefault = columnMeta.getField().getDefault();
                isColumnNullable = columnMeta.isNullable();
            } else {
                notFound = true;
            }
        } else {
            notFound = true;
        }

        if (notFound) {
            ColumnsRecord record = new TableInfoManagerDelegate<ColumnsRecord>(new TableInfoManager()) {
                @Override
                protected ColumnsRecord invoke() {
                    List<ColumnsRecord> records =
                        this.tableInfoManager.queryOneColumn(schemaName, tableName, columnName);
                    if (records != null && records.size() > 0) {
                        return records.get(0);
                    }
                    return null;
                }
            }.execute();

            if (record != null) {
                origDefault = record.columnDefault;
                isColumnNullable = TStringUtil.equalsIgnoreCase(record.isNullable, "YES");
                notFound = false;
            }
        }

        if (notFound) {
            // Not found original default value, so do nothing for safe.
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "not found original default value from meta");
        }

        if (origDefault != null) {
            reversedAlterColumn.setDropDefault(false);
            reversedAlterColumn.setDefaultExpr(genReversedDefault(origDefault));
        } else {
            if (isColumnNullable) {
                reversedAlterColumn.setDropDefault(false);
                reversedAlterColumn.setDefaultExpr(new SQLNullExpr());
            } else {
                reversedAlterColumn.setDropDefault(true);
                reversedAlterColumn.setDefaultExpr(null);
            }
        }

        reversedAlterColumn.setColumn(columnNameNode);

        reversedItems.add(reversedAlterColumn);
        return reversedItems;
    }

    private static SQLExpr genReversedDefault(String origDefault) {
        return new MySqlExprParser(ByteString.from(origDefault)).expr();
    }

    private static List<SQLAlterTableItem> reverse(MySqlAlterTableOption alterTableOption) {
        // Perhaps we should remove the 'algorithm' option for rollback for compatibility.
        final String algorithm = "ALGORITHM";
        if (TStringUtil.equalsIgnoreCase(algorithm, alterTableOption.getName())) {
            List<SQLAlterTableItem> reversedItems = new ArrayList<>();
            MySqlAlterTableOption reversedAlterTableOption =
                new MySqlAlterTableOption(algorithm, new SQLIdentifierExpr("DEFAULT"));
            reversedItems.add(reversedAlterTableOption);
            return reversedItems;
        } else {
            return Lists.newArrayList();
        }
    }

}
