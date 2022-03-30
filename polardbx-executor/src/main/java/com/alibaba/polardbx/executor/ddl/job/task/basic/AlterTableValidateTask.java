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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.rule.TableRule;
import lombok.Getter;
import org.apache.calcite.sql.SqlAddColumn;
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
import org.apache.calcite.sql.SqlChangeColumn;
import org.apache.calcite.sql.SqlDropColumn;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlModifyColumn;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

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

    private String tableName;
    private Long tableVersion;
    private TableGroupConfig tableGroupConfig;

    @JSONCreator
    public AlterTableValidateTask(String schemaName, String tableName, String stmt, Long tableVersion, TableGroupConfig tableGroupConfig) {
        super(schemaName);
        this.tableName = tableName;
        this.stmt = stmt;
        this.tableVersion = tableVersion;
        this.tableGroupConfig = tableGroupConfig;
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
            this.tableRule =
                OptimizerContext.getContext(schemaName).getRuleManager().getTddlRule().getTable(tableName);
        } else {
            this.tableRule = null;
        }

        if (OptimizerContext.getContext(schemaName).getPartitionInfoManager() != null) {
            this.partitionInfo =
                OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
        } else {
            this.partitionInfo = null;
        }

        SqlAlterTable sqlAlterTable = (SqlAlterTable) new FastsqlParser()
            .parse(stmt, executionContext)
            .get(0);
        boolean allowAlterShardingKey =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_ALTER_SHARD_KEY);
        if (!allowAlterShardingKey) {
            for (Map.Entry<SqlAlterTable.ColumnOpt, List<String>> entry : sqlAlterTable.getColumnOpts().entrySet()) {
                final List<String> value = entry.getValue();
                for (int i = 0; i < value.size(); i++) {
                    final String s = value.get(i);
                    checkModifyShardingKey(s);
                }
            }
        }

        Set<String> columns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> columnsBeforeDdl = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        columns.addAll(tableMeta.getAllColumns().stream().map(c -> c.getName()).collect(Collectors.toList()));
        columnsBeforeDdl.addAll(tableMeta.getAllColumns().stream().map(c -> c.getName()).collect(Collectors.toList()));

        Set<String> indexes = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> indexesBeforeDdl = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexes.addAll(tableMeta.getIndexes().stream().map(i -> i.getPhysicalIndexName()).collect(Collectors.toList()));
        indexesBeforeDdl
            .addAll(tableMeta.getIndexes().stream().map(i -> i.getPhysicalIndexName()).collect(Collectors.toList()));

        boolean existsPrimary = tableMeta.getPrimaryIndex() != null;

        for (SqlAlterSpecification alterItem : sqlAlterTable.getAlters()) {
            switch (alterItem.getKind()) {
            case ADD_COLUMN:
                checkColumnNotExists(columns, ((SqlAddColumn) alterItem).getColName().getLastName());
                if (((SqlAddColumn) alterItem).getAfterColumn() != null) {
                    checkColumnExists(columns, ((SqlAddColumn) alterItem).getAfterColumn().getLastName());
                }

                columns.add(((SqlAddColumn) alterItem).getColName().getLastName());
                break;

            case DROP_COLUMN:
                String columnName = ((SqlDropColumn) alterItem).getColName().getLastName();
                checkColumnExists(columnsBeforeDdl, columnName);
                if (existsPrimary) {
                    if (tableMeta.getPrimaryIndex().getKeyColumn(columnName) != null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_DROP_PRIMARY_KEY);
                    }
                }
                columns.remove(columnName);
                break;

            case MODIFY_COLUMN:
                checkColumnExists(columnsBeforeDdl, ((SqlModifyColumn) alterItem).getColName().getLastName());
                if (((SqlModifyColumn) alterItem).getAfterColumn() != null) {
                    checkColumnExists(columns, ((SqlModifyColumn) alterItem).getAfterColumn().getLastName());
                }
                break;

            case ALTER_COLUMN_DEFAULT_VAL:
                checkColumnExists(columnsBeforeDdl,
                    ((SqlAlterColumnDefaultVal) alterItem).getColumnName().getLastName());
                break;

            case CHANGE_COLUMN:
                checkColumnExists(columnsBeforeDdl, ((SqlChangeColumn) alterItem).getOldName().getLastName());
                columns.remove(((SqlChangeColumn) alterItem).getOldName().getLastName());
                checkColumnNotExists(columns, ((SqlChangeColumn) alterItem).getNewName().getLastName());
                if (((SqlChangeColumn) alterItem).getAfterColumn() != null) {
                    checkColumnExists(columns, ((SqlChangeColumn) alterItem).getAfterColumn().getLastName());
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
                checkIndexExists(indexesBeforeDdl, ((SqlAlterTableDropIndex) alterItem).getIndexName().getLastName());
                indexes.remove(((SqlAlterTableDropIndex) alterItem).getIndexName().getLastName());
                break;
            }
        }

        if (columns.size() == 1 && columns
            .contains(TddlConstants.IMPLICIT_COL_NAME)) { // no columns without implicit primary key
            throw new TddlRuntimeException(ErrorCode.ERR_DROP_ALL_COLUMNS);
        }
        if (tableGroupConfig!=null) {
            TableValidator.validateTableGroupChange(schemaName, tableGroupConfig);
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

    @Override
    protected String remark() {
        return "|logicalTableName: " + tableName;
    }
}
