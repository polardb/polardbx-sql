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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.ddl.Attribute;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableRenameColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.utils.MetaUtils;
import com.alibaba.polardbx.rule.TableRule;
import lombok.Getter;
import org.apache.calcite.sql.SqlAddColumn;
import org.apache.calcite.sql.SqlAddForeignKey;
import org.apache.calcite.sql.SqlAddFullTextIndex;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddPrimaryKey;
import org.apache.calcite.sql.SqlAddSpatialIndex;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterColumnDefaultVal;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableDropIndex;
import org.apache.calcite.sql.SqlAlterTableRenameIndex;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlChangeColumn;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlConvertToCharacterSet;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDropColumn;
import org.apache.calcite.sql.SqlDropForeignKey;
import org.apache.calcite.sql.SqlDropPrimaryKey;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlModifyColumn;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter.unwrapLocalIndexName;

/**
 * @author mengshi
 */
@Getter
@TaskName(name = "AlterTableValidateTask")
public class AlterTableValidateTask extends BaseValidateTask {

    private final String stmt;

    private transient TableRule tableRule;

    private transient PartitionInfo partitionInfo;

    private transient TableMeta tableMeta;

    private Boolean pushDownMultipleStatement;

    private String tableName;
    private Long tableVersion;
    private TableGroupConfig tableGroupConfig;

    @JSONCreator
    public AlterTableValidateTask(String schemaName, String tableName, String stmt, Long tableVersion,
                                  Boolean pushDownMultipleStatement, TableGroupConfig tableGroupConfig) {
        super(schemaName);
        this.tableName = tableName;
        this.stmt = stmt;
        this.tableVersion = tableVersion;
        this.pushDownMultipleStatement = pushDownMultipleStatement;
        this.tableGroupConfig = TableGroupConfig.copyWithoutTables(tableGroupConfig);
    }

    public AlterTableValidateTask(String schemaName, String tableName, String stmt, Long tableVersion,
                                  TableGroupConfig tableGroupConfig) {
        super(schemaName);
        this.tableName = tableName;
        this.stmt = stmt;
        this.tableVersion = tableVersion;
        this.pushDownMultipleStatement = false;
        this.tableGroupConfig = TableGroupConfig.copyWithoutTables(tableGroupConfig);
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        TableValidator.validateTableExistence(schemaName, tableName, executionContext);
        GsiValidator.validateAllowDdlOnTable(schemaName, tableName, executionContext);

        this.tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);

        if (this.tableMeta == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, schemaName, tableName);
        }

        if (tableMeta.getVersion() < tableVersion.longValue()) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_META_TOO_OLD, schemaName, tableName);
        }
        if (OptimizerContext.getContext(schemaName).getRuleManager() != null) {
            this.tableRule = OptimizerContext.getContext(schemaName).getRuleManager().getTddlRule().getTable(tableName);
        } else {
            this.tableRule = null;
        }

        if (OptimizerContext.getContext(schemaName).getPartitionInfoManager() != null) {
            this.partitionInfo =
                OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
        } else {
            this.partitionInfo = null;
        }

        SqlAlterTable sqlAlterTable = (SqlAlterTable) new FastsqlParser().parse(stmt, executionContext).get(0);

        final boolean checkForeignKey = executionContext.foreignKeyChecks();
        Map<String, ForeignKeyData> referencedColumns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (ForeignKeyData data : tableMeta.getReferencedForeignKeys().values()) {
            for (String refColumn : data.refColumns) {
                referencedColumns.put(refColumn, data);
            }
        }
        Map<String, ForeignKeyData> referencingColumns = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (ForeignKeyData data : tableMeta.getForeignKeys().values()) {
            for (String refColumn : data.columns) {
                referencingColumns.put(refColumn, data);
            }
        }
        Map<String, SqlDataTypeSpec.DrdsTypeName> columnsBeforeDdlType = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tableMeta.getAllColumns().forEach(c -> columnsBeforeDdlType.put(c.getName(),
            SqlDataTypeSpec.DrdsTypeName.from(c.getDataType().getStringSqlType().toUpperCase())));
        Set<String> constraints = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        constraints.addAll(
            tableMeta.getForeignKeys().values().stream().map(c -> c.constraint).collect(Collectors.toList()));

        Set<String> columns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> columnsBeforeDdl = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        columns.addAll(tableMeta.getAllColumns().stream().map(c -> c.getName()).collect(Collectors.toList()));
        columnsBeforeDdl.addAll(tableMeta.getAllColumns().stream().map(c -> c.getName()).collect(Collectors.toList()));
        // We need manually add this column since it is not in getAllColumns
        if (tableMeta.getColumnMultiWriteTargetColumnMeta() != null) {
            columns.add(tableMeta.getColumnMultiWriteTargetColumnMeta().getName());
        }

        Set<String> indexes = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> indexesBeforeDdl = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexes.addAll(
            tableMeta.getAllIndexes().stream().map(i -> i.getPhysicalIndexName()).collect(Collectors.toList()));
        indexesBeforeDdl.addAll(
            tableMeta.getAllIndexes().stream().map(i -> i.getPhysicalIndexName()).collect(Collectors.toList()));

        GsiMetaManager.GsiMetaBean gsiMetaBean =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);

        boolean existsPrimary = tableMeta.getPrimaryIndex() != null;
        if (sqlAlterTable.getAlters().size() > 1 && pushDownMultipleStatement) {
            validateMultipleStatement(tableMeta, sqlAlterTable);
            return;
        }

        for (SqlAlterSpecification alterItem : sqlAlterTable.getAlters()) {
            switch (alterItem.getKind()) {
            case ADD_COLUMN:
                checkColumnNotExists(columns, ((SqlAddColumn) alterItem).getColName().getLastName());
                checkWithCci(executionContext, alterItem.getKind());
                if (((SqlAddColumn) alterItem).getAfterColumn() != null) {
                    checkColumnExists(columns, ((SqlAddColumn) alterItem).getAfterColumn().getLastName());
                }
                checkAddGeneratedColumnOnFk((SqlAddColumn) alterItem);

                columns.add(((SqlAddColumn) alterItem).getColName().getLastName());
                break;

            case DROP_COLUMN:
                String columnName = ((SqlDropColumn) alterItem).getColName().getLastName();
                checkColumnExists(columnsBeforeDdl, columnName);
                checkModifyShardingKey(columnName);
                checkWithCci(executionContext, alterItem.getKind());
                if (checkForeignKey) {
                    checkFkDropColumn(referencedColumns, referencingColumns, columnName);
                }
                if (existsPrimary) {
                    if (tableMeta.getPrimaryIndex().getKeyColumn(columnName) != null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_DROP_PRIMARY_KEY);
                    }
                }
                columns.remove(columnName);
                break;

            case MODIFY_COLUMN:
                checkColumnExists(columnsBeforeDdl, ((SqlModifyColumn) alterItem).getColName().getLastName());
                checkWithCci(executionContext, alterItem.getKind());
                if (((SqlModifyColumn) alterItem).getAfterColumn() != null) {
                    checkColumnExists(columns, ((SqlModifyColumn) alterItem).getAfterColumn().getLastName());
                }
                checkFkModifyColumn(referencedColumns, referencingColumns, columnsBeforeDdlType,
                    (SqlModifyColumn) alterItem);

                break;

            case ALTER_COLUMN_DEFAULT_VAL:
                checkColumnExists(columnsBeforeDdl,
                    ((SqlAlterColumnDefaultVal) alterItem).getColumnName().getLastName());
                checkWithCci(executionContext, alterItem.getKind());
                break;

            case CHANGE_COLUMN:
                checkColumnExists(columnsBeforeDdl, ((SqlChangeColumn) alterItem).getOldName().getLastName());
                checkWithCci(executionContext, alterItem.getKind());
                checkLBAC(((SqlChangeColumn) alterItem).getOldName().getLastName());
                columns.remove(((SqlChangeColumn) alterItem).getOldName().getLastName());
                checkColumnNotExists(columns, ((SqlChangeColumn) alterItem).getNewName().getLastName());
                if (((SqlChangeColumn) alterItem).getAfterColumn() != null) {
                    checkColumnExists(columns, ((SqlChangeColumn) alterItem).getAfterColumn().getLastName());
                }
                checkFkChangeColumn(referencedColumns, referencingColumns, columnsBeforeDdlType,
                    (SqlChangeColumn) alterItem);
                if (!referencedColumns.isEmpty()) {
                    checkNotNull((SqlChangeColumn) alterItem, referencedColumns);
                }
                if (!referencingColumns.isEmpty()) {
                    checkNotNull((SqlChangeColumn) alterItem, referencingColumns);
                }

                columns.add(((SqlChangeColumn) alterItem).getNewName().getLastName());
                break;

            case ADD_INDEX:
                for (SqlIndexColumnName column : ((SqlAddIndex) alterItem).getIndexDef().getColumns()) {
                    checkColumnExists(columns, column.getColumnName().getLastName());
                }

                if (((SqlAddIndex) alterItem).getIndexName() != null) {
                    checkIndexNotExists(indexes, ((SqlAddIndex) alterItem).getIndexName().getLastName());
                    indexes.add(((SqlAddIndex) alterItem).getIndexName().getLastName());
                }
                break;

            case ADD_FULL_TEXT_INDEX:
                for (SqlIndexColumnName column : ((SqlAddFullTextIndex) alterItem).getIndexDef().getColumns()) {
                    checkColumnExists(columns, column.getColumnName().getLastName());
                }
                if (((SqlAddFullTextIndex) alterItem).getIndexName() != null) {
                    checkIndexNotExists(indexes, ((SqlAddFullTextIndex) alterItem).getIndexName().getLastName());
                    indexes.add(((SqlAddFullTextIndex) alterItem).getIndexName().getLastName());
                }
                break;

            case ADD_UNIQUE_INDEX:
                for (SqlIndexColumnName column : ((SqlAddUniqueIndex) alterItem).getIndexDef().getColumns()) {
                    checkColumnExists(columns, column.getColumnName().getLastName());
                }
                if (((SqlAddUniqueIndex) alterItem).getIndexName() != null) {
                    checkIndexNotExists(indexes, ((SqlAddUniqueIndex) alterItem).getIndexName().getLastName());
                    indexes.add(((SqlAddUniqueIndex) alterItem).getIndexName().getLastName());
                }
                break;

            case ADD_SPATIAL_INDEX:
                for (SqlIndexColumnName column : ((SqlAddSpatialIndex) alterItem).getIndexDef().getColumns()) {
                    checkColumnExists(columns, column.getColumnName().getLastName());
                }
                if (((SqlAddSpatialIndex) alterItem).getIndexName() != null) {
                    checkIndexNotExists(indexes, ((SqlAddSpatialIndex) alterItem).getIndexName().getLastName());
                    indexes.add(((SqlAddSpatialIndex) alterItem).getIndexName().getLastName());
                }
                break;

            case ALTER_RENAME_INDEX:
                final SqlAlterTableRenameIndex renameIndex = (SqlAlterTableRenameIndex) alterItem;
                if (null != renameIndex.getOriginIndexName() && tableMeta.withCci(
                    renameIndex.getOriginIndexName().getLastName())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "Do not support rename cci "
                        + renameIndex.getOriginIndexName().getLastName());
                }

                checkIndexExists(indexes, ((SqlAlterTableRenameIndex) alterItem).getIndexName().getLastName());
                indexes.remove(((SqlAlterTableRenameIndex) alterItem).getIndexName().getLastName());
                checkIndexNotExists(indexes, ((SqlAlterTableRenameIndex) alterItem).getNewIndexName().getLastName());
                indexes.add(((SqlAlterTableRenameIndex) alterItem).getNewIndexName().getLastName());
                break;

            case ADD_PRIMARY_KEY:
                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_PRIMARY_KEY);
                } else {
                    for (SqlIndexColumnName column : ((SqlAddPrimaryKey) alterItem).getColumns()) {
                        checkColumnExists(columns, column.getColumnName().getLastName());
                    }
                    checkPrimaryKeyNotExists(existsPrimary);
                    existsPrimary = true;
                }
                break;

            case DROP_PRIMARY_KEY:
                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DROP_PRIMARY_KEY);
                } else {
                    checkPrimaryKeExists(existsPrimary);
                    existsPrimary = false;
                }
                break;

            case DROP_INDEX:
                String indexName = ((SqlAlterTableDropIndex) alterItem).getIndexName().getLastName();
                checkDropLocalIndex(indexName, gsiMetaBean);
                checkIndexExists(indexesBeforeDdl, indexName);
                indexes.remove(((SqlAlterTableDropIndex) alterItem).getIndexName().getLastName());
                break;
            case ADD_FOREIGN_KEY:
                if (((SqlAddForeignKey) alterItem).getConstraint() != null) {
                    checkFkConstraintsExists(constraints, ((SqlAddForeignKey) alterItem).getConstraint().getLastName());
                }
                break;
            case DROP_FOREIGN_KEY:
                checkFkConstraintsNotExists(constraints, ((SqlDropForeignKey) alterItem).getConstraint().getLastName());
                break;
            case CONVERT_TO_CHARACTER_SET:
                if (checkForeignKey) {
                    checkFkCharset(((SqlConvertToCharacterSet) alterItem).getCharset());
                }
                break;
            }
        }

        if (columns.size() == 1 && columns.contains(
            TddlConstants.IMPLICIT_COL_NAME)) { // no columns without implicit primary key
            throw new TddlRuntimeException(ErrorCode.ERR_DROP_ALL_COLUMNS);
        }
        if (tableGroupConfig != null) {
            TableValidator.validateTableGroupChange(schemaName, tableGroupConfig);
        }

        checkRenameColumn();
    }

    private void checkRenameColumn() {
        SQLAlterTableStatement alterTableStatement = (SQLAlterTableStatement) FastsqlUtils.parseSql(stmt).get(0);
        for (SQLAlterTableItem item : alterTableStatement.getItems()) {
            if (item instanceof SQLAlterTableRenameColumn) {
                checkLBAC(((SQLAlterTableRenameColumn) item).getColumn().getSimpleName());
            }
        }
    }

    private void checkLBAC(String columnName) {
        if (LBACSecurityManager.getInstance().getColumnLabel(schemaName, tableName, columnName) != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_LBAC, "the column exist security label");
        }
    }

    // refer: sql_table.cc#mysql_prepare_alter_table
    private void validateMultipleStatement(TableMeta tableMeta, SqlAlterTable sqlAlterTable) {
        boolean existsPrimary = tableMeta.getPrimaryIndex() != null;
        List<SqlAlterSpecification> alterItems = new LinkedList<>(sqlAlterTable.getAlters());
        // oldColumnMetas:  all the column of original table
        // oldIndexes: all the index of original table
        // newIndexes: all the index of new table
        // newColumns: all the column of new table
        List<ColumnMeta> oldColumnMetas = tableMeta.getAllColumns();
        List<IndexMeta> oldIndexes = tableMeta.getSecondaryIndexes();
        List<String> newIndexes = new ArrayList<>(oldIndexes.size() + alterItems.size());
        List<SqlIndexDefinition> newIndexeDefs = new ArrayList<>(newIndexes.size());
        LinkedList<String> newColumns = new LinkedList<>();
        List<Pair<String, String>> afterColumns = new ArrayList<>();
        Iterator<SqlAlterSpecification> iter;

        for (ColumnMeta field : oldColumnMetas) {
            String fieldName = field.getName();
            List<SqlAlterSpecification> alterItemsForThisField = findAlterItems(alterItems, fieldName);
            // If there are no add, drop or any other alter action, then store new columns into newColumns
            if (alterItemsForThisField.isEmpty()) {
                newColumns.add(fieldName);
            } else {
                for (SqlAlterSpecification alterItem : alterItemsForThisField) {
                    switch (alterItem.getKind()) {
                    case DROP_COLUMN:
                        if (existsPrimary) {
                            if (tableMeta.getPrimaryIndex().getKeyColumn(fieldName) != null) {
                                // Not supported in Mutiple Statement DDL.
                                throw new TddlRuntimeException(ErrorCode.ERR_DROP_PRIMARY_KEY);
                            }
                        }
                        alterItems.remove(alterItem);
                        break;
                    case MODIFY_COLUMN:
                        if (((SqlModifyColumn) alterItem).getAfterColumn() != null) {
                            afterColumns.add(
                                Pair.of(fieldName, ((SqlModifyColumn) alterItem).getAfterColumn().getLastName()));
                        }
                        newColumns.add(fieldName);
                        break;
                    case ALTER_COLUMN_DEFAULT_VAL:
                        newColumns.add(fieldName);
                        alterItems.remove(alterItem);
                        break;
                    case CHANGE_COLUMN:
                        String newFieldName = ((SqlChangeColumn) alterItem).getNewName().getLastName();
                        if (((SqlChangeColumn) alterItem).getAfterColumn() != null) {
                            afterColumns.add(
                                Pair.of(newFieldName, ((SqlChangeColumn) alterItem).getAfterColumn().getLastName()));
                        }
                        newColumns.add(newFieldName);
                        break;
                    }
                }
            }
        }

        iter = alterItems.iterator();
        while (iter.hasNext()) {
            SqlAlterSpecification alterItem = iter.next();
            switch (alterItem.getKind()) {
            case MODIFY_COLUMN:
                if (((SqlModifyColumn) alterItem).getAfterColumn() != null) {
                    String afterName = ((SqlModifyColumn) alterItem).getAfterColumn().getLastName();
                    if (!newColumns.contains(afterName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_COLUMN, afterName, tableName);
                    }
                }
                iter.remove();
                break;
            case CHANGE_COLUMN:
                if (((SqlChangeColumn) alterItem).getAfterColumn() != null) {
                    String afterName = ((SqlChangeColumn) alterItem).getAfterColumn().getLastName();
                    if (!newColumns.contains(afterName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_COLUMN, afterName, tableName);
                    }
                }
                iter.remove();
                break;
            case ADD_COLUMN:
                // put the new add columns in the end.
                // which would not be adjusted.
                if (((SqlAddColumn) alterItem).getAfterColumn() != null) {
                    String afterName = ((SqlAddColumn) alterItem).getAfterColumn().getLastName();
                    if (!newColumns.contains(afterName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_COLUMN, afterName, tableName);
                    }
                    afterColumns.add(Pair.of(((SqlAddColumn) alterItem).getColName().getLastName(),
                        afterName));
                }
                newColumns.add(((SqlAddColumn) alterItem).getColName().getLastName());
                iter.remove();
                break;
            }
        }
        //Reorder by add and change after relationShip
        for (Pair<String, String> afterColumn : afterColumns) {
            String columnName = afterColumn.getKey();
            String afterColumnName = afterColumn.getValue();
            int index = newColumns.indexOf(columnName);
            if (index == -1) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_COLUMN, columnName, tableName);
            }
            newColumns.remove(index);
            index = newColumns.indexOf(afterColumnName);
            if (index == -1) {
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_COLUMN, afterColumnName, tableName);
            }
            newColumns.add(index + 1, columnName);
        }

        String columnName;
        iter = alterItems.iterator();
        while (iter.hasNext()) {
            SqlAlterSpecification alterItem = iter.next();
            switch (alterItem.getKind()) {
            case DROP_COLUMN:
                columnName = ((SqlDropColumn) alterItem).getColName().getLastName();
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_COLUMN, columnName, tableName);
            case MODIFY_COLUMN:
                columnName = ((SqlModifyColumn) alterItem).getColName().getLastName();
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_COLUMN, columnName, tableName);
            case ALTER_COLUMN_DEFAULT_VAL:
                columnName = ((SqlAlterColumnDefaultVal) alterItem).getColumnName().getLastName();
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_COLUMN, columnName, tableName);
            case CHANGE_COLUMN:
                columnName = ((SqlChangeColumn) alterItem).getOldName().getLastName();
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_COLUMN, columnName, tableName);
            }
        }
        validateNewColumns(newColumns);
        for (IndexMeta indexMeta : oldIndexes) {
            boolean drop = false;
            iter = alterItems.iterator();
            while (iter.hasNext()) {
                SqlAlterSpecification alterItem = iter.next();
                switch (alterItem.getKind()) {
                case DROP_INDEX:
                    if (indexMeta.getPhysicalIndexName()
                        .equalsIgnoreCase(((SqlAlterTableDropIndex) alterItem).getIndexName().getLastName())) {
                        drop = true;
                        iter.remove();
                    }
                    break;
                case ALTER_RENAME_INDEX:
                    if (indexMeta.getPhysicalIndexName()
                        .equalsIgnoreCase(((SqlAlterTableRenameIndex) alterItem).getOriginIndexName().getLastName())) {
                        drop = true;
                        newIndexes.add(((SqlAlterTableRenameIndex) alterItem).getNewIndexName().getLastName());
                        iter.remove();
                    }
                    break;
                }
            }
            if (!drop) {
                newIndexes.add(indexMeta.getPhysicalIndexName());
            }
        }

        iter = alterItems.iterator();
        while (iter.hasNext()) {
            SqlAlterSpecification alterItem = iter.next();
            switch (alterItem.getKind()) {
            case DROP_INDEX:
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_KEY,
                    ((SqlAlterTableDropIndex) alterItem).getIndexName().getLastName());
            case ADD_INDEX:
                if (((SqlAddIndex) alterItem).getIndexName() != null) {
                    newIndexes.add(((SqlAddIndex) alterItem).getIndexName().getLastName());
                }
                newIndexeDefs.add(((SqlAddIndex) alterItem).getIndexDef());
                iter.remove();
                break;
            case ADD_FULL_TEXT_INDEX:
                if (((SqlAddFullTextIndex) alterItem).getIndexName() != null) {
                    newIndexes.add(((SqlAddFullTextIndex) alterItem).getIndexName().getLastName());
                }
                newIndexeDefs.add(((SqlAddFullTextIndex) alterItem).getIndexDef());
                iter.remove();
                break;
            case ADD_UNIQUE_INDEX:
                if (((SqlAddUniqueIndex) alterItem).getIndexName() != null) {
                    newIndexes.add(((SqlAddUniqueIndex) alterItem).getIndexName().getLastName());
                }
                newIndexeDefs.add(((SqlAddUniqueIndex) alterItem).getIndexDef());
                iter.remove();
                break;
            case ADD_SPATIAL_INDEX:
                if (((SqlAddSpatialIndex) alterItem).getIndexName() != null) {
                    newIndexes.add(((SqlAddSpatialIndex) alterItem).getIndexName().getLastName());
                }
                newIndexeDefs.add(((SqlAddSpatialIndex) alterItem).getIndexDef());
                iter.remove();
                break;
            case ALTER_RENAME_INDEX:
                throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_KEY,
                    ((SqlAlterTableRenameIndex) alterItem).getIndexName().getLastName());
            case ADD_PRIMARY_KEY:
                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_PRIMARY_KEY);
                } else {
                    Set newColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                    newColumnSet.addAll(newColumns);
                    for (SqlIndexColumnName column : ((SqlAddPrimaryKey) alterItem).getColumns()) {
                        checkColumnExists(newColumnSet, column.getColumnName().getLastName());
                    }
                    checkPrimaryKeyNotExists(existsPrimary);
                    existsPrimary = true;
                }
                break;
            case DROP_PRIMARY_KEY:
                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DROP_PRIMARY_KEY);
                } else {
                    checkPrimaryKeExists(existsPrimary);
                    existsPrimary = false;
                }
                break;
            }
        }
        validateNewIndexes(newIndexes);
    }

    private void validateNewColumns(List<String> newColumns) {
        if (newColumns.size() == 1 && newColumns.contains(TddlConstants.IMPLICIT_COL_NAME)
            || newColumns.isEmpty()) { // no columns without implicit primary key
            throw new TddlRuntimeException(ErrorCode.ERR_DROP_ALL_COLUMNS);
        }
        Set<String> newColumnSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (String name : newColumns) {
            if (newColumnSet.contains(name)) {
                throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATE_COLUMN, name);
            }
            newColumnSet.add(name);
        }
    }

    private void validateNewIndexes(List<String> newIndexes) {
        Set<String> newIndexSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (String name : newIndexes) {
            if (newIndexSet.contains(name)) {
                throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATE_KEY, name);
            }
            newIndexSet.add(name);
        }
    }

    private void validateColumns(List<String> newColumns) {
        Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (String name : newColumns) {
            checkColumnNotExists(names, name);
            names.add(name);
        }
    }

    private List<SqlAlterSpecification> findAlterItems(List<SqlAlterSpecification> alterItems, String name) {
        List<SqlAlterSpecification> matched = new ArrayList<>();
        String columnName;
        for (SqlAlterSpecification alterItem : alterItems) {
            switch (alterItem.getKind()) {
            case DROP_COLUMN:
                columnName = ((SqlDropColumn) alterItem).getColName().getLastName();
                if (name.equalsIgnoreCase(columnName)) {
                    matched.add(alterItem);
                }
                continue;
            case MODIFY_COLUMN:
                columnName = ((SqlModifyColumn) alterItem).getColName().getLastName();
                if (name.equalsIgnoreCase(columnName)) {
                    matched.add(alterItem);
                }
                continue;
            case ALTER_COLUMN_DEFAULT_VAL:
                columnName = ((SqlAlterColumnDefaultVal) alterItem).getColumnName().getLastName();
                if (name.equalsIgnoreCase(columnName)) {
                    matched.add(alterItem);
                }
                continue;
            case CHANGE_COLUMN:
                columnName = ((SqlChangeColumn) alterItem).getOldName().getLastName();
                if (name.equalsIgnoreCase(columnName)) {
                    matched.add(alterItem);
                }
                continue;
            }
        }
        return matched;
    }

    private void checkDropLocalIndex(String indexName, GsiMetaManager.GsiMetaBean gsiMetaBean) {
        // 默认主键拆分的表，不允许删除默认生成的 local index
        if (tableMeta.isAutoPartition() && !gsiMetaBean.isGsi(indexName) && tableMeta.getGsiTableMetaBean() != null) {

            String logicalGsiName = unwrapLocalIndexName(indexName);
            final String wrapped = tableMeta.getGsiTableMetaBean().indexMap.keySet().stream()
                .filter(idx -> TddlSqlToRelConverter.unwrapGsiName(idx).equalsIgnoreCase(logicalGsiName)).findFirst()
                .orElse(null);

            if (wrapped != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_AUTO_PARTITION_TABLE,
                    "it is not allowed to drop the default local index generated by gsi");
            }
        }
    }

    public void checkModifyShardingKey(String column) {
        if (this.tableRule != null) {// for sharding
            for (String shardColumn : tableRule.getShardColumns()) {
                if (column.equalsIgnoreCase(shardColumn)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ALTER_SHARDING_KEY);
                }
            }
        } else if (this.partitionInfo != null) {// for partition
            for (String shardColumn : this.partitionInfo.getPartitionColumns()) {
                if (column.equalsIgnoreCase(shardColumn)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ALTER_SHARDING_KEY);
                }
            }
        }
    }

    private void checkPrimaryKeExists(boolean existsPrimary) {
        if (!existsPrimary) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_KEY, "PRIMARY");
        }
    }

    private void checkPrimaryKeyNotExists(boolean existsPrimary) {
        if (existsPrimary) {
            throw new TddlRuntimeException(ErrorCode.ERR_MULTIPLE_PRIMARY_KEY);
        }
    }

    private void checkIndexExists(Set<String> indexes, String indexName) {
        if (!indexes.contains(indexName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_KEY, indexName);
        }
    }

    private void checkIndexNotExists(Set<String> indexes, String indexName) {
        if (indexes.contains(indexName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATE_KEY, indexName);
        }
    }

    private void checkColumnExists(Set<String> columns, String columnName) {
        if (!columns.contains(columnName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_COLUMN, columnName, tableName);
        }
    }

    private void checkColumnNotExists(Set<String> columns, String columnName) {
        if (columns.contains(columnName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATE_COLUMN, columnName);
        }
    }

    private void checkFkModifyColumn(Map<String, ForeignKeyData> referencedColumns,
                                     Map<String, ForeignKeyData> referencingColumns,
                                     Map<String, SqlDataTypeSpec.DrdsTypeName> columnsBeforeDdlType,
                                     SqlModifyColumn alterItem) {
        String columnName = alterItem.getColName().getLastName();
        if (referencedColumns.containsKey(columnName)) {
            checkColumnType(alterItem, columnsBeforeDdlType, columnName, referencedColumns, true);
            checkNotNull(alterItem, referencedColumns);
        }
        if (referencingColumns.containsKey(columnName)) {
            checkColumnType(alterItem, columnsBeforeDdlType, columnName, referencingColumns, false);
            checkNotNull(alterItem, referencingColumns);
        }
    }

    private void checkFkChangeColumn(Map<String, ForeignKeyData> referencedColumns,
                                     Map<String, ForeignKeyData> referencingColumns,
                                     Map<String, SqlDataTypeSpec.DrdsTypeName> columnsBeforeDdlType,
                                     SqlChangeColumn alterItem) {
        String columnName = alterItem.getOldName().getLastName();
        if (referencedColumns.containsKey(columnName)) {
            checkColumnType(alterItem, columnsBeforeDdlType, columnName, referencedColumns, true);
        }
        if (referencingColumns.containsKey(columnName)) {
            checkColumnType(alterItem, columnsBeforeDdlType, columnName, referencingColumns, false);
        }
    }

    private void checkColumnType(SqlModifyColumn alterItem,
                                 Map<String, SqlDataTypeSpec.DrdsTypeName> columnsBeforeDdlType, String columnName,
                                 Map<String, ForeignKeyData> columns, boolean referenced) {
        SqlDataTypeSpec.DrdsTypeName columnType = SqlDataTypeSpec.DrdsTypeName.from(
            alterItem.getColDef().getDataType().getTypeName().getLastName().toUpperCase());
        if (!columnType.equals(columnsBeforeDdlType.get(columnName))) {
            int columnIndex = referenced ? columns.get(columnName).refColumns.indexOf(columnName) :
                columns.get(columnName).columns.indexOf(columnName);
            String referencingColumnName = columns.get(columnName).refColumns.get(columnIndex);
            String referencedColumnName = columns.get(columnName).columns.get(columnIndex);
            if (referenced) {
                throw new TddlRuntimeException(ErrorCode.ERR_CHANGE_COLUMN_FK_CONSTRAINT, referencingColumnName,
                    schemaName, tableName, referencedColumnName, columns.get(columnName).schema,
                    columns.get(columnName).tableName, columns.get(columnName).constraint);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_CHANGE_COLUMN_FK_CONSTRAINT,
                    columns.get(columnName).refSchema, columns.get(columnName).refTableName, referencingColumnName,
                    schemaName, tableName, referencedColumnName, columns.get(columnName).constraint);
            }
        }
    }

    private void checkNotNull(SqlModifyColumn alterItem, Map<String, ForeignKeyData> columns) {
        boolean onSetNull =
            columns.get(alterItem.getColName().getSimple()).onUpdate.equals(ForeignKeyData.ReferenceOptionType.SET_NULL)
                || columns.get(alterItem.getColName().getSimple()).onDelete.equals(
                ForeignKeyData.ReferenceOptionType.SET_NULL);
        boolean isNotNull = alterItem.getColDef().getNotNull() == SqlColumnDeclaration.ColumnNull.NOTNULL;
        boolean isPrimary = alterItem.getColDef().getSpecialIndex() != null && alterItem.getColDef().getSpecialIndex()
            .equals(SqlColumnDeclaration.SpecialIndex.PRIMARY);
        if (onSetNull && (isNotNull || isPrimary)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT,
                "Foreign key columns can not be NULL when option is SET NULL");
        }
    }

    private void checkNotNull(SqlChangeColumn alterItem, Map<String, ForeignKeyData> columns) {
        boolean onSetNull = columns.get(alterItem.getOldName().getSimple()) != null && (
            columns.get(alterItem.getOldName().getSimple()).onUpdate.equals(ForeignKeyData.ReferenceOptionType.SET_NULL)
                || columns.get(alterItem.getOldName().getSimple()).onDelete.equals(
                ForeignKeyData.ReferenceOptionType.SET_NULL));
        boolean isNotNull = alterItem.getColDef().getNotNull() == SqlColumnDeclaration.ColumnNull.NOTNULL;
        boolean isPrimary = alterItem.getColDef().getSpecialIndex() != null && alterItem.getColDef().getSpecialIndex()
            .equals(SqlColumnDeclaration.SpecialIndex.PRIMARY);
        if (onSetNull && (isNotNull || isPrimary)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_CONSTRAINT,
                "Foreign key columns can not be NULL when option is SET NULL");
        }
    }

    private void checkColumnType(SqlChangeColumn alterItem,
                                 Map<String, SqlDataTypeSpec.DrdsTypeName> columnsBeforeDdlType, String columnName,
                                 Map<String, ForeignKeyData> columns, boolean referenced) {
        SqlDataTypeSpec.DrdsTypeName columnType = SqlDataTypeSpec.DrdsTypeName.from(
            alterItem.getColDef().getDataType().getTypeName().getLastName().toUpperCase());
        if (!columnType.equals(columnsBeforeDdlType.get(columnName))) {
            int columnIndex = referenced ? columns.get(columnName).refColumns.indexOf(columnName) :
                columns.get(columnName).columns.indexOf(columnName);
            String referencingColumnName = columns.get(columnName).refColumns.get(columnIndex);
            String referencedColumnName = columns.get(columnName).columns.get(columnIndex);
            if (referenced) {
                throw new TddlRuntimeException(ErrorCode.ERR_CHANGE_COLUMN_FK_CONSTRAINT, referencingColumnName,
                    schemaName, tableName, referencedColumnName, columns.get(columnName).schema,
                    columns.get(columnName).tableName, columns.get(columnName).constraint);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_CHANGE_COLUMN_FK_CONSTRAINT,
                    columns.get(columnName).refSchema, columns.get(columnName).refTableName, referencingColumnName,
                    schemaName, tableName, referencedColumnName, columns.get(columnName).constraint);
            }
        }
    }

    private void checkFkDropColumn(Map<String, ForeignKeyData> referencedColumns,
                                   Map<String, ForeignKeyData> referencingColumns, String columnName) {
        if (referencedColumns.containsKey(columnName) || referencingColumns.containsKey(columnName)) {
            ForeignKeyData data = null;
            if (referencedColumns.containsKey(columnName)) {
                data = referencedColumns.get(columnName);
                throw new TddlRuntimeException(ErrorCode.ERR_DROP_COLUMN_FK_CONSTRAINT, columnName, data.schema,
                    data.tableName, data.constraint);
            } else {
                data = referencingColumns.get(columnName);
                throw new TddlRuntimeException(ErrorCode.ERR_DROP_COLUMN_FK_CONSTRAINT, columnName, data.schema,
                    data.tableName, data.constraint);
            }

        }
    }

    private void checkFkConstraintsExists(Set<String> constraints, String constraintName) {
        if (constraints.contains(constraintName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATE_NAME_FK_CONSTRAINT, constraintName);
        }
    }

    private void checkFkConstraintsNotExists(Set<String> constraints, String constraintName) {
        if (!constraints.contains(constraintName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DROP_FK_CONSTRAINT, constraintName);
        }
    }

    private void checkFkCharset(String charset) {
        if (!charset.equalsIgnoreCase(tableMeta.getDefaultCharset())) {
            if (!tableMeta.getForeignKeys().isEmpty() || !tableMeta.getReferencedForeignKeys().isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_FK_CONVERT_TO_CHARSET, charset, schemaName, tableName);
            }
        }
    }

    private void checkAddGeneratedColumnOnFk(SqlAddColumn addColumn) {
        SqlColumnDeclaration col = addColumn.getColDef();
        Set<String> generatedReferencedColumns = new TreeSet<>(String::compareToIgnoreCase);
        if (col.isGeneratedAlways()) {
            generatedReferencedColumns.add(col.toString());
            SqlCall expr = col.getGeneratedAlwaysExpr();
            GeneratedColumnUtil.validateGeneratedColumnExpr(expr);
            generatedReferencedColumns.addAll(GeneratedColumnUtil.getReferencedColumns(expr));
        } else {
            return;
        }
        for (ForeignKeyData data : tableMeta.getForeignKeys().values()) {
            for (String column : data.columns) {
                if (generatedReferencedColumns.contains(column)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ADD_FK_GENERATED_COLUMN, column);
                }
            }
        }
    }

    private void checkWithCci(ExecutionContext executionContext, SqlKind sqlKind) {
        boolean forbidDdlWithCci = executionContext.getParamManager().getBoolean(ConnectionParams.FORBID_DDL_WITH_CCI);
        if (forbidDdlWithCci && tableMeta.withCci()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_WITH_CCI, sqlKind.name());
        }
    }

    @Override
    protected String remark() {
        return "|logicalTableName: " + tableName;
    }
}
