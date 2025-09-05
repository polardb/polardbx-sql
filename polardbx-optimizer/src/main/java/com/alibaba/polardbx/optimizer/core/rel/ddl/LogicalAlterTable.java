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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.SQLMode;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.ddl.Attribute;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.DefaultExprUtil;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiTableMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiUtils;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.PreparedDataUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RepartitionPrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.AlterGlobalIndexVisibilityPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.AlterTableWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateIndexWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropIndexWithGsiPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.RenameGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;
import com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils;
import com.alibaba.polardbx.optimizer.utils.MetaUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAddColumn;
import org.apache.calcite.sql.SqlAddForeignKey;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAddPrimaryKey;
import org.apache.calcite.sql.SqlAddUniqueIndex;
import org.apache.calcite.sql.SqlAlterColumnDefaultVal;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableAlterIndex;
import org.apache.calcite.sql.SqlAlterTableAsOfTimeStamp;
import org.apache.calcite.sql.SqlAlterTableDropFile;
import org.apache.calcite.sql.SqlAlterTableDropIndex;
import org.apache.calcite.sql.SqlAlterTableExchangePartition;
import org.apache.calcite.sql.SqlAlterTableModifyTtlOptions;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlAlterTablePurgeBeforeTimeStamp;
import org.apache.calcite.sql.SqlAlterTableRemoveLocalPartition;
import org.apache.calcite.sql.SqlAlterTableRemoveTtlOptions;
import org.apache.calcite.sql.SqlAlterTableRenameIndex;
import org.apache.calcite.sql.SqlAlterTableRepartitionLocalPartition;
import org.apache.calcite.sql.SqlBinaryStringLiteral;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlChangeColumn;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlColumnDeclaration.SpecialIndex;
import org.apache.calcite.sql.SqlConvertToCharacterSet;
import org.apache.calcite.sql.SqlDropColumn;
import org.apache.calcite.sql.SqlDropForeignKey;
import org.apache.calcite.sql.SqlDropPrimaryKey;
import org.apache.calcite.sql.SqlEnableKeys;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlModifyColumn;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlReferenceOption;
import org.apache.calcite.sql.SqlTableOptions;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.AUTO_LOCAL_INDEX_PREFIX;
import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;
import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;
import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.EMPTY_CONTENT;
import static com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter.unwrapGsiName;
import static com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils.PARTITION_FK_SUB_JOB;
import static org.apache.calcite.sql.SqlCreateTable.buildUnifyIndexName;

public class LogicalAlterTable extends LogicalTableOperation {

    public final static Collection<AlterColumnSpecification> ALTER_COLUMN_RENAME = ImmutableList.of(
        AlterColumnSpecification.AlterColumnName);
    public final static Collection<AlterColumnSpecification> ALTER_COLUMN_NAME_OR_TYPE = ImmutableList.of(
        AlterColumnSpecification.AlterColumnName,
        AlterColumnSpecification.AlterColumnType);
    public final static Collection<AlterColumnSpecification> ALTER_COLUMN_DEFAULT = ImmutableList.of(
        AlterColumnSpecification.AlterColumnDefault);
    public final static Collection<AlterColumnSpecification> ALTER_COLUMN_REORDER = ImmutableList.of(
        AlterColumnSpecification.AlterColumnDefault);
    private final SqlAlterTable sqlAlterTable;
    // Use list for multiple alters.
    private final List<Set<AlterColumnSpecification>> alterColumnSpecificationSets = new ArrayList<>();

    // TODO there are duplications over these two PrepareData
    private AlterTablePreparedData alterTablePreparedData;
    private AlterTableWithGsiPreparedData alterTableWithGsiPreparedData;

    private AlterTablePreparedData alterTableWithFileStorePreparedData;

    private RepartitionPrepareData repartitionPrepareData;
    private List<CreateGlobalIndexPreparedData> createGlobalIndexesPreparedData;

    private boolean rewrittenAlterSql = false;

    public LogicalAlterTable(AlterTable alterTable) {
        super(alterTable);
        this.sqlAlterTable = (SqlAlterTable) relDdl.sqlNode;
    }

    public static LogicalAlterTable create(AlterTable alterTable) {
        return new LogicalAlterTable(alterTable);
    }

    public static List<String> getAlteredColumns(SqlAlterTable alterTable, SqlAlterTable.ColumnOpt columnOpt) {
        Map<SqlAlterTable.ColumnOpt, List<String>> columnOpts = alterTable.getColumnOpts();
        if (columnOpts != null && columnOpts.size() > 0) {
            return columnOpts.get(columnOpt);
        }
        return null;
    }

    public SqlAlterTable getSqlAlterTable() {
        return this.sqlAlterTable;
    }

    public boolean isRepartition() {
        return sqlAlterTable != null && sqlAlterTable instanceof SqlAlterTablePartitionKey;
    }

    public boolean isAlterIndexVisibility() {
        return sqlAlterTable != null
            && sqlAlterTable.isAlterIndexVisibility();
    }

    public boolean isExchangePartition() {
        return sqlAlterTable != null && sqlAlterTable.isExchangePartition();
    }

    public boolean isAllocateLocalPartition() {
        return sqlAlterTable != null && sqlAlterTable.isAllocateLocalPartition();
    }

    public boolean isExpireLocalPartition() {
        return sqlAlterTable != null && sqlAlterTable.isExpireLocalPartition();
    }

    public boolean isCleanupExpiredData() {
        return sqlAlterTable != null && sqlAlterTable.isCleanupExpiredData();
    }

    public boolean isModifyTtlOptions() {
        if (sqlAlterTable != null) {
            List<SqlAlterSpecification> sqlAlterSpecifications = sqlAlterTable.getAlters();
            if (!sqlAlterSpecifications.isEmpty() && sqlAlterSpecifications.size() == 1) {
                SqlAlterSpecification alterSpec = sqlAlterSpecifications.get(0);
                if (alterSpec instanceof SqlAlterTableModifyTtlOptions) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isRemoveTtlOptions() {
        if (sqlAlterTable != null) {
            List<SqlAlterSpecification> sqlAlterSpecifications = sqlAlterTable.getAlters();
            if (!sqlAlterSpecifications.isEmpty() && sqlAlterSpecifications.size() == 1) {
                SqlAlterSpecification alterSpec = sqlAlterSpecifications.get(0);
                if (alterSpec instanceof SqlAlterTableRemoveTtlOptions) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isDropFile() {
        return sqlAlterTable != null && sqlAlterTable.isDropFile();
    }

    public boolean isAlterEngine() {
        return sqlAlterTable != null && sqlAlterTable.getTableOptions() != null
            && sqlAlterTable.getTableOptions().getEngine() != null;
    }

    public boolean isAlterAsOfTimeStamp() {
        return sqlAlterTable instanceof SqlAlterTableAsOfTimeStamp;
    }

    public boolean isAlterPurgeBeforeTimeStamp() {
        return sqlAlterTable instanceof SqlAlterTablePurgeBeforeTimeStamp;
    }

    public boolean isRepartitionLocalPartition() {
        return sqlAlterTable != null && sqlAlterTable instanceof SqlAlterTableRepartitionLocalPartition;
    }

    public boolean isRemoveLocalPartition() {
        return sqlAlterTable != null && sqlAlterTable instanceof SqlAlterTableRemoveLocalPartition;
    }

    public boolean isCreateGsi() {
        return sqlAlterTable.createGsi();
    }

    public boolean isCreateClusteredIndex() {
        return sqlAlterTable.createClusteredIndex();
    }

    public boolean isCreateCci() {
        return sqlAlterTable.createCci();
    }

    public boolean isAddIndex() {
        return sqlAlterTable.addIndex();
    }

    public boolean isDropIndex() {
        return sqlAlterTable.dropIndex();
    }

    public boolean isDropGsi() {
        return alterTableWithGsiPreparedData != null &&
            alterTableWithGsiPreparedData.getDropIndexWithGsiPreparedData() != null &&
            alterTableWithGsiPreparedData.getDropIndexWithGsiPreparedData().getGlobalIndexPreparedData() != null &&
            !alterTableWithGsiPreparedData.getDropIndexWithGsiPreparedData().getGlobalIndexPreparedData().isColumnar();
    }

    public boolean isDropCci() {
        return alterTableWithGsiPreparedData != null &&
            alterTableWithGsiPreparedData.getDropIndexWithGsiPreparedData() != null &&
            alterTableWithGsiPreparedData.getDropIndexWithGsiPreparedData().getGlobalIndexPreparedData() != null &&
            alterTableWithGsiPreparedData.getDropIndexWithGsiPreparedData().getGlobalIndexPreparedData().isColumnar();
    }

    public boolean isAlterTableRenameGsi() {
        return alterTableWithGsiPreparedData != null
            && alterTableWithGsiPreparedData.getRenameGlobalIndexPreparedData() != null;
    }

    public boolean isTruncatePartition() {
        return sqlAlterTable.isTruncatePartition();
    }

    public boolean isAutoPartitionTable() {
        final String tableName = sqlAlterTable.getOriginTableName().getLastName();
        final TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        return tableMeta.isAutoPartition();
    }

    public AlterTablePreparedData getAlterTablePreparedData() {
        return alterTablePreparedData;
    }

    public AlterTableWithGsiPreparedData getAlterTableWithGsiPreparedData() {
        return alterTableWithGsiPreparedData;
    }

    public AlterTablePreparedData getAlterTableWithFileStorePreparedData() {
        return alterTableWithFileStorePreparedData;
    }

    private AlterTableWithGsiPreparedData getOrNewAlterTableWithGsiPreparedData() {
        if (this.alterTableWithGsiPreparedData == null) {
            this.alterTableWithGsiPreparedData = new AlterTableWithGsiPreparedData();
        }
        return this.alterTableWithGsiPreparedData;
    }

    public boolean isWithGsi() {
        return alterTableWithGsiPreparedData != null && alterTableWithGsiPreparedData.hasGsi();
    }

    public boolean isAlterColumnAlterDefault() {
        return !alterColumnSpecificationSets.isEmpty() &&
            alterColumnSpecificationSets.get(0).stream().anyMatch(ALTER_COLUMN_DEFAULT::contains);
    }

    public boolean isAlterColumnRename() {
        return !alterColumnSpecificationSets.isEmpty() &&
            alterColumnSpecificationSets.get(0).stream().anyMatch(ALTER_COLUMN_RENAME::contains);
    }

//    public boolean isTruncatePartition() {
//        return sqlAlterTable.isTruncatePartition();
//    }

    @Override
    public boolean isSupportedByFileStorage() {
        final String tableName = sqlAlterTable.getOriginTableName().getLastName();
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTableWithNull(tableName);
        // forbid ddl on old oss table
        if (tableMeta.isOldFileStorage()) {
            return isAlterAsOfTimeStamp() || isAlterPurgeBeforeTimeStamp() || isAlterEngine() || isExchangePartition()
                || isDropFile();
        }
        // forbid ddl on ttl table
        CheckOSSArchiveUtil.checkTTLSource(schemaName, tableName);
        if (supportedCommonByFileStorage()) {
            return true;
        }
        return checkAlterForFileStorage() && validateModifyFileStorage();
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        final String tableName = sqlAlterTable.getOriginTableName().getLastName();
        // 在 TTL 表上执行后，不需要联动到 OSS 表的 DDL
        if (skipCheckForFileStorage()) {
            return true;
        }

        if (CheckOSSArchiveUtil.withoutOldFileStorage(schemaName, tableName)) {
            if (supportedCommonByFileStorage()) {
                return true;
            }
            return checkAlterForFileStorage() && validateModifyFileStorage();
        }
        return supportedCommonByFileStorage();
    }

    public boolean isRewrittenAlterSql() {
        return rewrittenAlterSql;
    }

    public void setRewrittenAlterSql(boolean rewrittenAlterSql) {
        this.rewrittenAlterSql = rewrittenAlterSql;
    }

    public boolean supportedCommonByFileStorage() {
        return isAlterAsOfTimeStamp() || isAlterPurgeBeforeTimeStamp() || isAlterEngine() || isExchangePartition()
            || isDropFile() || isAllocateLocalPartition() || isExpireLocalPartition();
    }

    /**
     * Check if ddl od ttl-tbl/local-part-tbl should be skipped by the archive-table
     */
    private boolean skipCheckForFileStorage() {

        /**
         * Comh here, that means the schemaName.tableName
         * must be a ttl-table with binding archive table
         */
        if (!sqlAlterTable.getAlters().isEmpty()) {
            SqlAlterSpecification alterModifyTtlDefAst = sqlAlterTable.getAlters().get(0);
            if (alterModifyTtlDefAst instanceof SqlAlterTableModifyTtlOptions) {
                if (sqlAlterTable.getAlters().size() > 1) {
                    throw new NotSupportException("Not support multi alter on modify ttl definition");
                }
                return true;
            }
        }

        return isRepartitionLocalPartition();
    }

    private boolean checkAlterForFileStorage() {
        // Repartition local partition and remove local partition do not have alters, special judge is needed
        if (isRepartitionLocalPartition() || isRemoveLocalPartition()) {
            return false;
        }
        // 'alter default character set' is not parsed as alter
        for (SqlAlterSpecification alterItem : sqlAlterTable.getAlters()) {
            if (!alterItem.supportFileStorage()) {
                return false;
            }
        }
        return true;
    }

    /**
     * validate whether modify/change column is supported
     *
     * @return true if supported
     */
    private boolean validateModifyFileStorage() {
        // don't support omc
        if (sqlAlterTable.getTableOptions() != null) {
            SqlIdentifier algorithm = sqlAlterTable.getTableOptions().getAlgorithm();
            if ((algorithm != null && algorithm.getSimple().equalsIgnoreCase(Attribute.ALTER_TABLE_ALGORITHM_OMC))) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Do not support online modify column on archive table");
            }
        }

        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        MetaUtils.TableColumns tableColumns = MetaUtils.TableColumns.build(tableMeta);

        for (SqlAlterSpecification alterItem : sqlAlterTable.getAlters()) {
            SqlKind alterType = alterItem.getKind();
            if (alterType != SqlKind.MODIFY_COLUMN && alterType != SqlKind.CHANGE_COLUMN) {
                continue;
            }

            String columnName;
            SqlColumnDeclaration columnDeclaration;

            // validate 'after' and 'rename'
            if (alterType == SqlKind.MODIFY_COLUMN) {
                SqlModifyColumn modifyColumn = (SqlModifyColumn) alterItem;
                columnName = modifyColumn.getColName().getLastName();
                columnDeclaration = modifyColumn.getColDef();

                if (modifyColumn.getAfterColumn() != null) {
                    String afterColumn = modifyColumn.getAfterColumn().getLastName();
                    if (afterColumn.equalsIgnoreCase(columnName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Do not support insert after the same column");
                    }
                }
            } else {
                SqlChangeColumn changeColumn = (SqlChangeColumn) alterItem;
                columnName = changeColumn.getOldName().getLastName();
                columnDeclaration = changeColumn.getColDef();

                if (changeColumn.getAfterColumn() != null) {
                    String afterColumn = changeColumn.getAfterColumn().getLastName();
                    if (afterColumn.equalsIgnoreCase(columnName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Do not support insert after the same column");
                    }
                }
                String newColumnName = changeColumn.getNewName().getLastName();
                if (tableMeta.getColumnIgnoreCase(newColumnName) != null && !newColumnName.equalsIgnoreCase(
                    columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                        String.format("Duplicate column name `%s`", newColumnName));
                }
            }

            // pk and sk
            if (tableColumns.isPrimaryKey(columnName) || tableColumns.isShardingKey(columnName)
                || tableColumns.isGsiShardingKey(columnName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Do not support modify column on primary key or sharding key with file storage table");
            }

            final ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(columnName);
            if (columnMeta == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Modify unknown column '" + columnName + "'");
            }

            final RelDataType sourceDataType = columnMeta.getField().getRelType();
            final RelDataType targetDataType = columnDeclaration.getDataType()
                .deriveType(getCluster().getTypeFactory(), columnDeclaration.getDataType().getNullable());

            if (TableColumnUtils.isUnsupportedType(
                columnDeclaration.getDataType().getTypeName().toString())) {
                return false;
            }

            if (!TableColumnUtils.canConvertBetweenTypeFileStorage(sourceDataType, targetDataType,
                columnDeclaration.getDataType())) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    String.format("Converting between type %s and %s is not supported", sourceDataType.getSqlTypeName(),
                        targetDataType.getSqlTypeName()));
            }
        }

        return true;
    }

    public List<CreateGlobalIndexPreparedData> getCreateGlobalIndexesPreparedData() {
        return createGlobalIndexesPreparedData;
    }

    public void setCreateGlobalIndexesPreparedData(List<CreateGlobalIndexPreparedData> createGsiPreparedDataList) {
        this.createGlobalIndexesPreparedData = createGsiPreparedDataList;
    }

    public boolean isAddLogicalForeignKeyOnly() {
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        Set<String> indexes = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        indexes.addAll(
            tableMeta.getAllIndexes().stream().map(i -> i.getPhysicalIndexName()).collect(Collectors.toList()));
        if (GeneralUtil.isNotEmpty(alterTablePreparedData.getAddedForeignKeys())) {
            ForeignKeyData fk = alterTablePreparedData.getAddedForeignKeys().get(0);
            if (indexes.contains(fk.constraint)) {
                return true;
            }
        }
        return false;
    }

    public boolean isAddGeneratedColumn() {
        // We have made sure that adding generated column does not mix with other alters
        return GeneralUtil.isNotEmpty(sqlAlterTable.getAlters()) && sqlAlterTable.getAlters()
            .stream()
            .allMatch(alter -> alter instanceof SqlAddColumn && ((SqlAddColumn) alter).getColDef()
                .isGeneratedAlwaysLogical());
    }

    public boolean isDropGeneratedColumn() {
        final String tableName = sqlAlterTable.getOriginTableName().getLastName();
        final TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        Set<String> generatedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        generatedColumns.addAll(tableMeta.getLogicalGeneratedColumnNames());

        // We have made sure that droping generated column does not mix with other alters
        return GeneralUtil.isNotEmpty(sqlAlterTable.getAlters()) && sqlAlterTable.getAlters()
            .stream()
            .allMatch(alter -> {
                if (alter instanceof SqlDropColumn) {
                    String colName = ((SqlDropColumn) alter).getColName().getLastName();
                    return generatedColumns.contains(colName);
                } else {
                    return false;
                }
            });
    }

    public void prepareData() {
        // NOTE that there is only one GSI operation is allowed along with a ALTER TABLE specification,
        // i.e. if an ALTER TABLE with a specification that is an explicit or implicit GSI operation,
        // then no any other operation is allowed in this ALTER TABLE statement.

        // Currently only one alter operation is allowed in online alter operation.

        if (sqlAlterTable.createGsi()) {
            prepareCreateData();
        } else {
            if (sqlAlterTable.dropIndex()) {
                prepareDropData();
            } else if (sqlAlterTable.renameIndex()) {
                prepareRenameData();
            } else if (sqlAlterTable.isAlterIndexVisibility()) {
                prepareAlterIndexVisibilityData();
            } else {
                prepareAlterGsiData();
            }

            prepareAlterData();

            // oss
            prepareAlterFileStore();
        }
    }

    public RepartitionPrepareData getRepartitionPrepareData() {
        return repartitionPrepareData;
    }

    public void prepareLocalIndexData() {
        if (repartitionPrepareData == null) {
            repartitionPrepareData = new RepartitionPrepareData();
        }
        Map<String, GsiMetaManager.GsiIndexMetaBean> gsiInfo = new HashMap<>();
        repartitionPrepareData.setGsiInfo(gsiInfo);

        final GsiMetaManager.GsiMetaBean gsiMetaBean =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);
        GsiMetaManager.GsiTableMetaBean tableMeta = gsiMetaBean.getTableMeta().get(tableName);
        if (tableMeta == null) {
            return;
        }

        for (Map.Entry<String, GsiMetaManager.GsiIndexMetaBean> indexEntry : tableMeta.indexMap.entrySet()) {
            final String indexName = indexEntry.getKey();
            final GsiMetaManager.GsiIndexMetaBean indexDetail = indexEntry.getValue();

            if (indexDetail.columnarIndex) {
                continue;
            }

            if (indexDetail.indexStatus != IndexStatus.PUBLIC) {
                throw new TddlRuntimeException(ErrorCode.ERR_REPARTITION_TABLE_WITH_GSI,
                    "can not alter table repartition when gsi table is not public");
            }

            gsiInfo.put(TddlConstants.AUTO_LOCAL_INDEX_PREFIX + indexName, indexDetail);
        }
    }

    /**
     * GSI table prepare data when the table rule of primary table has changed
     * <p>
     * if GSI table contain the new partition keys of primary table, then do nothing, otherwise
     * add columns and backfill data
     */
    public void prepareRepartitionData(TableRule targetTableRule) {
        if (repartitionPrepareData == null) {
            repartitionPrepareData = new RepartitionPrepareData();
        }
        Map<String, List<String>> backfillIndexs = new TreeMap<>();
        List<String> dropIndexes = new ArrayList<>();

        final SqlAddIndex sqlAddIndex = (SqlAddIndex) sqlAlterTable.getAlters().get(0);
        final SqlIndexDefinition indexDef = sqlAddIndex.getIndexDef();
        repartitionPrepareData.setPrimaryTableDefinition(indexDef.getPrimaryTableDefinition());

        repartitionPrepareData.setBackFilledIndexes(backfillIndexs);
        repartitionPrepareData.setDroppedIndexes(dropIndexes);

        final GsiMetaBean gsiMetaBean =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);
        GsiTableMetaBean tableMeta = gsiMetaBean.getTableMeta().get(tableName);
        if (tableMeta == null) {
            return;
        }

        for (Map.Entry<String, GsiIndexMetaBean> indexEntry : tableMeta.indexMap.entrySet()) {
            final String indexName = indexEntry.getKey();
            final GsiIndexMetaBean indexDetail = indexEntry.getValue();
            List<String> columns = new ArrayList<>();
            Set<String> backfillColumns = new HashSet<>();

            // Ignore GSI which is not public.
            if (indexDetail.indexStatus != IndexStatus.PUBLIC) {
                throw new TddlRuntimeException(ErrorCode.ERR_REPARTITION_TABLE_WITH_GSI,
                    "can not alter table repartition when gsi table is not public");
            }

            for (GsiMetaManager.GsiIndexColumnMetaBean indexColumn : indexDetail.indexColumns) {
                columns.add(indexColumn.columnName);
            }
            for (GsiMetaManager.GsiIndexColumnMetaBean coveringColumn : indexDetail.coveringColumns) {
                columns.add(coveringColumn.columnName);
            }

            for (String dbPartitionKey : targetTableRule.getDbPartitionKeys()) {
                if (columns.stream().noneMatch(dbPartitionKey::equalsIgnoreCase)) {
                    backfillColumns.add(dbPartitionKey);
                }
            }
            for (String tbPartitionKey : targetTableRule.getTbPartitionKeys()) {
                if (columns.stream().noneMatch(tbPartitionKey::equalsIgnoreCase)) {
                    backfillColumns.add(tbPartitionKey);
                }
            }

            if (!backfillColumns.isEmpty()) {
                backfillIndexs.put(indexName, new ArrayList<>(backfillColumns));
            }

            TableRule indexTableRule = OptimizerContext.getContext(schemaName).getRuleManager().getTableRule(indexName);
            boolean dropIndex = PlannerUtils.tableRuleIsIdentical(targetTableRule, indexTableRule)
                && ListUtils.isEqualList(indexTableRule.getDbPartitionKeys(), targetTableRule.getDbPartitionKeys())
                && ListUtils.isEqualList(indexTableRule.getTbPartitionKeys(), targetTableRule.getTbPartitionKeys());

            PartitionInfo indexPartitionInfo = OptimizerContext.getContext(schemaName).getPartitionInfoManager()
                .getPartitionInfo(indexName);

            if (indexDetail.columnarIndex) {
                // CCI which need to be recreated
                genAddCciSql(indexName, indexDetail, indexPartitionInfo);
                genDropCciSql(indexName, indexDetail, indexPartitionInfo);
            } else if (dropIndex || (GeneralUtil.isEmpty(targetTableRule.getDbShardRules()) &&
                GeneralUtil.isEmpty(targetTableRule.getTbShardRules()))
                || targetTableRule.isBroadcast()) {
                // GSI which need to be dropped
                dropIndexes.add(indexName);
            }
        }
    }

    public void prepareForeignKeyData(TableMeta tableMeta, SqlAlterTablePartitionKey ast) {
        if (repartitionPrepareData == null) {
            repartitionPrepareData = new RepartitionPrepareData();
        }

        Set<ForeignKeyData> addFks = new HashSet<>();
        Set<ForeignKeyData> removeFks = new HashSet<>();

        addFks.addAll(tableMeta.getForeignKeys().values());
        addFks.addAll(tableMeta.getReferencedForeignKeys().values());
        removeFks.addAll(tableMeta.getForeignKeys().values());
        removeFks.addAll(tableMeta.getReferencedForeignKeys().values());
        repartitionPrepareData.getModifyForeignKeys().addAll(tableMeta.getForeignKeys().values());

        genAddForeignKeySql(addFks);
        genDropForeignKeySql(removeFks);
    }

    private void genAddForeignKeySql(Set<ForeignKeyData> foreignKeys) {
        String sql;
        String rollbackSql;
        for (ForeignKeyData data : foreignKeys) {
            sql = String.format("ALTER TABLE `%s`.`%s` ADD ",
                data.schema, data.tableName) + data.toString() + PARTITION_FK_SUB_JOB;
            rollbackSql = String.format("ALTER TABLE `%s`.`%s` DROP FOREIGN KEY `%s`",
                data.schema, data.tableName, data.constraint) + PARTITION_FK_SUB_JOB;
            repartitionPrepareData.getAddForeignKeySql().add(new Pair<>(sql, rollbackSql));
        }
    }

    private void genDropForeignKeySql(Set<ForeignKeyData> foreignKeys) {
        String sql;
        String rollbackSql;
        for (ForeignKeyData data : foreignKeys) {
            sql = String.format("ALTER TABLE `%s`.`%s` DROP FOREIGN KEY `%s`",
                data.schema, data.tableName, data.constraint) + PARTITION_FK_SUB_JOB;
            rollbackSql = String.format("ALTER TABLE `%s`.`%s` ADD ",
                data.schema, data.tableName) + data.toString() + PARTITION_FK_SUB_JOB;
            repartitionPrepareData.getDropForeignKeySql().add(new Pair<>(sql, rollbackSql));
            Set<String> tables = repartitionPrepareData.getForeignKeyChildTable()
                .computeIfAbsent(data.schema, x -> new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER));
            tables.add(data.tableName);
        }
    }

    private void genAddCciSql(String indexName, GsiMetaManager.GsiIndexMetaBean indexDetail,
                              PartitionInfo indexPartitionInfo) {
        indexName = TddlSqlToRelConverter.unwrapGsiName(indexName);
        StringBuilder sql = new StringBuilder();
        String rollbackSql = "";
        sql.append(String.format("CREATE CLUSTERED COLUMNAR INDEX `%s` ON `%s` (",
            indexName, indexDetail.tableName));
        for (int i = 0; i < indexDetail.indexColumns.size(); i++) {
            if (i == 0) {
                sql.append("`").append(indexDetail.indexColumns.get(i).columnName).append("`");
            } else {
                sql.append(", `").append(indexDetail.indexColumns.get(i).columnName).append("`");
            }
        }
        sql.append(")");
        sql.append(indexPartitionInfo.getPartitionBy().toString());

        rollbackSql = String.format("DROP INDEX `%s` ON `%s`", indexName, indexDetail.tableName);
        repartitionPrepareData.getAddCciSql().add(new Pair<>(sql.toString(), rollbackSql));
    }

    private void genDropCciSql(String indexName, GsiMetaManager.GsiIndexMetaBean indexDetail,
                               PartitionInfo indexPartitionInfo) {
        indexName = TddlSqlToRelConverter.unwrapGsiName(indexName);
        String sql = "";
        StringBuilder rollbackSql = new StringBuilder();
        sql = String.format("DROP INDEX `%s` ON `%s`", indexName, indexDetail.tableName);

        rollbackSql.append(String.format("CREATE CLUSTERED COLUMNAR INDEX `%s` ON `%s` (",
            indexName, indexDetail.tableName));
        for (int i = 0; i < indexDetail.indexColumns.size(); i++) {
            if (i == 0) {
                rollbackSql.append("`").append(indexDetail.indexColumns.get(i).columnName).append("`");
            } else {
                rollbackSql.append(", `").append(indexDetail.indexColumns.get(i).columnName).append("`");
            }
        }
        rollbackSql.append(")");
        rollbackSql.append(indexPartitionInfo.getPartitionBy().toString());
        repartitionPrepareData.getDropCciSql().add(new Pair<>(sql, rollbackSql.toString()));
    }

    private void prepareCreateData() {
        alterTableWithGsiPreparedData = new AlterTableWithGsiPreparedData();
        final SqlAddIndex sqlAddIndex = (SqlAddIndex) sqlAlterTable.getAlters().get(0);
        final String indexName = sqlAddIndex.getIndexName().getLastName();
        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        TableMeta tableMeta = sm.getTable(tableName);
        CreateIndexWithGsiPreparedData createIndexWithGsiPreparedData = new CreateIndexWithGsiPreparedData();
        createIndexWithGsiPreparedData.setGlobalIndexPreparedData(prepareCreateGsiData(indexName, sqlAddIndex));

        if (isAutoPartitionTable() && !sqlAddIndex.isColumnarIndex()) {
            CreateLocalIndexPreparedData localIndexPreparedData =
                prepareCreateLocalIndexData(tableName, indexName, isCreateClusteredIndex(), true);
            createIndexWithGsiPreparedData.addLocalIndexPreparedData(localIndexPreparedData);
            localIndexPreparedData.setTableVersion(tableMeta.getVersion());

            addLocalIndexOnClusteredTable(createIndexWithGsiPreparedData, indexName, true);
        }

        alterTableWithGsiPreparedData.setCreateIndexWithGsiPreparedData(createIndexWithGsiPreparedData);
    }

    private CreateGlobalIndexPreparedData prepareCreateGsiData(String indexTableName, SqlAddIndex sqlAddIndex) {
        final OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);

        final SqlIndexDefinition indexDef = sqlAddIndex.getIndexDef();

        final TableMeta primaryTableMeta = optimizerContext.getLatestSchemaManager().getTable(tableName);
        final TableRule primaryTableRule = optimizerContext.getRuleManager().getTableRule(tableName);

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        boolean isBroadCast;
        Map<SqlNode, RexNode> partBoundExprInfo = null;
        PartitionInfo primaryPartitionInfo = null;
        String locality = "";
        String sourceSql = null;
        if (isNewPartDb) {
            primaryPartitionInfo = optimizerContext.getPartitionInfoManager().getPartitionInfo(tableName);
            isBroadCast = primaryPartitionInfo.isBroadcastTable();
            partBoundExprInfo = ((AlterTable) (this.relDdl)).getAllRexExprInfo();
            if (!sqlAddIndex.isColumnarIndex()) {
                // Do not set locality for cci
                locality = primaryPartitionInfo.getLocality().toString();
            }
            sourceSql = ((SqlAlterTable) ((this.relDdl)).getSqlNode()).getOriginalSql();
        } else {
            isBroadCast = primaryTableRule.isBroadcast();
        }

        boolean isUnique = sqlAddIndex instanceof SqlAddUniqueIndex;
        boolean isClustered = indexDef.isClustered();
        boolean isColumnar = indexDef.isColumnar();

        final LocalPartitionDefinitionInfo localPartitionDefinitionInfo =
            primaryTableMeta.getLocalPartitionDefinitionInfo();
        if (localPartitionDefinitionInfo != null) {
            localPartitionDefinitionInfo.setId(null);
            localPartitionDefinitionInfo.setTableName(indexTableName);
        }

        CreateGlobalIndexPreparedData preparedData =
            prepareCreateGlobalIndexData(tableName, indexDef.getPrimaryTableDefinition(), indexTableName,
                primaryTableMeta, false, false, false, indexDef.getDbPartitionBy(),
                indexDef.getDbPartitions(), indexDef.getTbPartitionBy(), indexDef.getTbPartitions(),
                indexDef.getPartitioning(), localPartitionDefinitionInfo, isUnique, isClustered, isColumnar,
                indexDef.getTableGroupName(), indexDef.isWithImplicitTableGroup(), null,
                locality, partBoundExprInfo, sourceSql);
        if (isNewPartDb) {
            preparedData.setPrimaryPartitionInfo(primaryPartitionInfo);
            preparedData.setOldPrimaryKeys(
                primaryTableMeta.getPrimaryKey().stream().map(ColumnMeta::getName).collect(Collectors.toList()));
        } else {
            preparedData.setPrimaryTableRule(primaryTableRule);
        }
        preparedData.setIndexDefinition(indexDef);
        preparedData.setTableVersion(primaryTableMeta.getVersion());
        preparedData.setVisible(indexDef.isVisible());

        CreateTablePreparedData indexTablePreparedData = preparedData.getIndexTablePreparedData();
        indexTablePreparedData.setGsi(true);

        if (indexDef.isSingle()) {
            indexTablePreparedData.setSharding(false);
        } else if (indexDef.isBroadcast()) {
            indexTablePreparedData.setBroadcast(true);
        } else {
            indexTablePreparedData.setSharding(true);
        }

        if (sqlAddIndex instanceof SqlAddUniqueIndex) {
            preparedData.setUnique(true);
        }

        if (indexDef.getOptions() != null) {
            final String indexComment = indexDef.getOptions()
                .stream()
                .filter(option -> null != option.getComment())
                .findFirst()
                .map(option -> RelUtils.stringValue(option.getComment()))
                .orElse("");
            preparedData.setIndexComment(indexComment);
        }

        if (indexDef.getIndexType() != null) {
            preparedData.setIndexType(null == indexDef.getIndexType() ? null : indexDef.getIndexType().name());
        }

        if (indexDef != null) {
            preparedData.setSingle(indexDef.isSingle());
            preparedData.setBroadcast(indexDef.isBroadcast());
        }

        if (isNewPartDb) {
            JoinGroupInfoRecord record = JoinGroupUtils.getJoinGroupInfoByTable(schemaName, tableName, null);
            if (record != null) {
                SqlIdentifier joinGroup = new SqlIdentifier(record.joinGroupName, SqlParserPos.ZERO);
                preparedData.setJoinGroupName(joinGroup);
                if (preparedData.getIndexTablePreparedData() != null) {
                    preparedData.getIndexTablePreparedData().setJoinGroupName(joinGroup);
                }
            }
        }

        if (preparedData.isWithImplicitTableGroup()) {
            TableGroupInfoManager tableGroupInfoManager =
                OptimizerContext.getContext(preparedData.getSchemaName()).getTableGroupInfoManager();
            String tableGroupName = preparedData.getTableGroupName() == null ? null :
                ((SqlIdentifier) preparedData.getTableGroupName()).getLastName();
            assert tableGroupName != null;
            if (alterTableWithGsiPreparedData != null) {
                if (tableGroupInfoManager.getTableGroupConfigByName(tableGroupName) == null) {
                    alterTableWithGsiPreparedData.getRelatedTableGroupInfo().put(tableGroupName, true);
                    preparedData.getRelatedTableGroupInfo().put(tableGroupName, true);
                } else {
                    alterTableWithGsiPreparedData.getRelatedTableGroupInfo().put(tableGroupName, false);
                    preparedData.getRelatedTableGroupInfo().put(tableGroupName, false);
                }
            } else {
                if (tableGroupInfoManager.getTableGroupConfigByName(tableGroupName) == null) {
                    preparedData.getRelatedTableGroupInfo().put(tableGroupName, true);
                } else {
                    preparedData.getRelatedTableGroupInfo().put(tableGroupName, false);
                }
            }
        }

        return preparedData;
    }

    public void prepareModifySk(TableMeta newTableMeta) {
        createGlobalIndexesPreparedData = new ArrayList<>();

        final List<SqlAddIndex> sqlAddIndexes =
            sqlAlterTable.getSkAlters().stream().map(e -> (SqlAddIndex) e).collect(Collectors.toList());

        for (SqlAddIndex sqlAddIndex : sqlAddIndexes) {
            final String indexName = sqlAddIndex.getIndexName().getLastName();
            CreateGlobalIndexPreparedData createGlobalIndexPreparedData = prepareCreateGsiData(indexName, sqlAddIndex);
            createGlobalIndexPreparedData.getIndexTablePreparedData().setTableMeta(newTableMeta);
            if (!createGlobalIndexPreparedData.getIndexTablePreparedData().isWithImplicitTableGroup()) {
                createGlobalIndexPreparedData.getIndexTablePreparedData().setSourceSql(null);
            }
            if (CollectionUtils.isEmpty(alterTablePreparedData.getAddedPrimaryKeyColumns())) {
                createGlobalIndexPreparedData.setOldPrimaryKeys(null);
            }
            createGlobalIndexesPreparedData.add(createGlobalIndexPreparedData);
        }
    }

    private void prepareDropData() {
        final SqlAlterTableDropIndex dropIndex = (SqlAlterTableDropIndex) sqlAlterTable.getAlters().get(0);
        final String indexTableName = dropIndex.getIndexName().getLastName();
        final String originalIndexName = dropIndex.getOriginIndexName().getLastName();

        final GsiMetaBean gsiMetaBean =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);
        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta tableMeta = sm.getTable(tableName);

        if (gsiMetaBean.isGsiOrCci(indexTableName)) {
            alterTableWithGsiPreparedData = new AlterTableWithGsiPreparedData();

            DropIndexWithGsiPreparedData dropIndexWithGsiPreparedData = new DropIndexWithGsiPreparedData();
            dropIndexWithGsiPreparedData
                .setGlobalIndexPreparedData(prepareDropGlobalIndexData(tableName, indexTableName, false));
            dropIndexWithGsiPreparedData.getGlobalIndexPreparedData().setTableVersion(tableMeta.getVersion());
            dropIndexWithGsiPreparedData.getGlobalIndexPreparedData().setOriginalIndexName(originalIndexName);
            if (isAutoPartitionTable() && gsiMetaBean.isGsi(indexTableName)) {
                // drop implicit local index
                Set<String> indexes = tableMeta.getLocalIndexNames();

                // primary table local index
                if (indexes.contains(AUTO_LOCAL_INDEX_PREFIX + unwrapGsiName(indexTableName))) {
                    dropIndexWithGsiPreparedData.addLocalIndexPreparedData(
                        prepareDropLocalIndexData(tableName, indexTableName, isCreateClusteredIndex(), true));
                }

                // drop local index (generated by gsi) on clustered table
                dropLocalIndexOnClusteredTable(dropIndexWithGsiPreparedData, gsiMetaBean, indexTableName);
            }

            alterTableWithGsiPreparedData.setDropIndexWithGsiPreparedData(dropIndexWithGsiPreparedData);
        } else {
            if (tableMeta.withGsi()) {
                boolean clusteredExists = false;
                for (SqlAlterSpecification alterItem : sqlAlterTable.getAlters()) {
                    if (alterItem.getKind() == SqlKind.DROP_INDEX) {
                        clusteredExists = true;
                    }
                }
                if (clusteredExists) {
                    // clustered index need add column or index.
                    if (sqlAlterTable.getAlters().size() > 1) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Do not support multi ALTER statements on table with clustered index");
                    }

                    alterTableWithGsiPreparedData = new AlterTableWithGsiPreparedData();

                    final GsiTableMetaBean gsiTableMetaBean = tableMeta.getGsiTableMetaBean();
                    for (Map.Entry<String, GsiIndexMetaBean> indexEntry : gsiTableMetaBean.indexMap.entrySet()) {
                        if (indexEntry.getValue().clusteredIndex) {
                            final String clusteredIndexTableName = indexEntry.getKey();
                            TableMeta gsiTableMeta = sm.getTable(clusteredIndexTableName);

                            if (null != sqlAlterTable.getTableOptions()
                                && GeneralUtil.isNotEmpty(sqlAlterTable.getTableOptions().getUnion())) {
                                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                    "Do not support set table option UNION to table with clustered index");
                            }

                            if (sqlAlterTable.getAlters().get(0).getKind() == SqlKind.DROP_INDEX) {
                                // Special dealing.
                                final SqlAlterTableDropIndex dropClusteredIndex =
                                    (SqlAlterTableDropIndex) sqlAlterTable.getAlters().get(0);
                                if (!PreparedDataUtil.indexExistence(schemaName, clusteredIndexTableName,
                                    dropClusteredIndex.getIndexName().getLastName())) {
                                    continue; // Ignore this clustered index.
                                } else if (gsiTableMeta.isLastShardIndex(
                                    dropClusteredIndex.getIndexName().getLastName())) {
                                    RenameLocalIndexPreparedData preparedData = new RenameLocalIndexPreparedData();
                                    String oldName = dropClusteredIndex.getIndexName().getLastName();
                                    Set<String> orderedIndexColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
                                    orderedIndexColumnNames.addAll(
                                        gsiTableMeta.getPartitionInfo().getActualPartitionColumnsNotReorder());
                                    final String suffix = buildUnifyIndexName(orderedIndexColumnNames, 45);
                                    final String newName = TddlConstants.AUTO_SHARD_KEY_PREFIX + suffix;
                                    preparedData.setSchemaName(schemaName);
                                    preparedData.setTableName(clusteredIndexTableName);
                                    preparedData.setOrgIndexName(oldName);
                                    preparedData.setNewIndexName(newName);
                                    alterTableWithGsiPreparedData.setRenameLocalIndexPreparedData(preparedData);
                                    continue;
                                }
                            }
                            AlterTablePreparedData alterTablePreparedData =
                                prepareAlterTableData(clusteredIndexTableName);
                            alterTablePreparedData.setTableVersion(gsiTableMeta.getVersion());
                            alterTableWithGsiPreparedData.addAlterGlobalIndexPreparedData(alterTablePreparedData);
                        }
                    }
                }
            }
        }
    }

    private void addLocalIndexOnClusteredTable(CreateIndexWithGsiPreparedData createIndexWithGsiPreparedData,
                                               String indexName,
                                               boolean onGsi) {
        final GsiMetaBean gsiMetaBean =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);

        if (gsiMetaBean.withGsi(tableName)) {
            // Local indexes on clustered GSIs.
            final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
            for (Map.Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                if (gsiEntry.getValue().clusteredIndex && !gsiEntry.getValue().columnarIndex) {
                    final String clusteredTableName = gsiEntry.getKey();
                    createIndexWithGsiPreparedData
                        .addLocalIndexPreparedData(
                            prepareCreateLocalIndexData(clusteredTableName, indexName, true, onGsi));
                }
            }
        }
    }

    private void dropLocalIndexOnClusteredTable(DropIndexWithGsiPreparedData dropIndexWithGsiPreparedData,
                                                GsiMetaBean gsiMetaBean,
                                                String indexTableName) {
        if (gsiMetaBean.withGsi(tableName)) {
            // Drop generated local index on clustered.
            final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
            for (Map.Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                if (gsiEntry.getValue().clusteredIndex && !gsiEntry.getKey().equalsIgnoreCase(indexTableName) &&
                    !gsiEntry.getValue().columnarIndex) {
                    // Add all clustered index except which is dropping.
                    final String clusteredTableName = gsiEntry.getKey();
                    TableMeta tableMeta =
                        OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(clusteredTableName);
                    Set<String> indexes = tableMeta.getLocalIndexNames();

                    if (!dropIndexWithGsiPreparedData.hasLocalIndexOnClustered(clusteredTableName)) {
                        String targetLocalIndexName = AUTO_LOCAL_INDEX_PREFIX + unwrapGsiName(indexTableName);

                        if (indexes.contains(targetLocalIndexName)) {
                            if (tableMeta.isLastShardIndex(targetLocalIndexName)) {
                                RenameLocalIndexPreparedData preparedData = new RenameLocalIndexPreparedData();
                                Set<String> orderedIndexColumnNames =
                                    new LinkedHashSet<>(
                                        tableMeta.getPartitionInfo().getActualPartitionColumnsNotReorder());
                                final String suffix = buildUnifyIndexName(orderedIndexColumnNames, 45);
                                final String newName = TddlConstants.AUTO_SHARD_KEY_PREFIX + suffix;
                                preparedData.setSchemaName(schemaName);
                                preparedData.setTableName(clusteredTableName);
                                preparedData.setOrgIndexName(targetLocalIndexName);
                                preparedData.setNewIndexName(newName);
                                alterTableWithGsiPreparedData.setRenameLocalIndexPreparedData(preparedData);
                                continue;
                            }
                            DropLocalIndexPreparedData dropLocalIndexPreparedData =
                                prepareDropLocalIndexData(clusteredTableName, null, true, true);
                            dropIndexWithGsiPreparedData.addLocalIndexPreparedData(dropLocalIndexPreparedData);
                        }
                    }
                }
            }
        }
    }

    private void modifyColumnOnClusteredTable(AlterTableWithGsiPreparedData preparedData,
                                              GsiMetaBean gsiMetaBean) {
        if (!gsiMetaBean.withGsi(tableName)) {
            return;
        }
        final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
        for (Map.Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
            if (gsiEntry.getValue().clusteredIndex && !gsiEntry.getValue().columnarIndex) {
                preparedData.addAlterClusterIndex(prepareAlterTableData(gsiEntry.getKey()));
            }
        }
    }

    private void prepareRenameData() {
        final SqlAlterTableRenameIndex renameIndex = (SqlAlterTableRenameIndex) sqlAlterTable.getAlters().get(0);
        final String indexTableName = renameIndex.getIndexName().getLastName();
        final String newIndexName = renameIndex.getNewIndexNameStr();

        final GsiMetaBean gsiMetaBean =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);

        if (gsiMetaBean.isGsi(indexTableName)) {
            alterTableWithGsiPreparedData = new AlterTableWithGsiPreparedData();
            alterTableWithGsiPreparedData
                .setRenameGlobalIndexPreparedData(prepareRenameGsiData(indexTableName, newIndexName));
        }
    }

    private void prepareAlterIndexVisibilityData() {
        final SqlAlterTableAlterIndex alterIndexVisibility = (SqlAlterTableAlterIndex) sqlAlterTable.getAlters().get(0);
        final String primaryTableName = alterIndexVisibility.getTableName().getLastName();
        final String indexTableName = alterIndexVisibility.getIndexName().getLastName();
        final String visibility = alterIndexVisibility.getVisibility();

        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final GsiMetaBean gsiMetaBean = sm.getGsi(tableName, IndexStatus.ALL);

        if (gsiMetaBean.isGsi(indexTableName) || gsiMetaBean.isColumnar(indexTableName)) {
            alterTableWithGsiPreparedData = new AlterTableWithGsiPreparedData();
            AlterGlobalIndexVisibilityPreparedData preparedData = new AlterGlobalIndexVisibilityPreparedData();
            preparedData.setPrimaryTableName(primaryTableName);
            preparedData.setIndexTableName(indexTableName);
            preparedData.setVisibility(visibility);
            preparedData.setColumnar(gsiMetaBean.isColumnar(indexTableName));
            TableMeta gsiTableMeta = sm.getTable(indexTableName);
            preparedData.setTableVersion(gsiTableMeta.getVersion());
            preparedData.setSchemaName(schemaName);
            alterTableWithGsiPreparedData.setGlobalIndexVisibilityPreparedData(preparedData);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                "only global index's visibility can be altered");
        }
    }

    private RenameGlobalIndexPreparedData prepareRenameGsiData(String indexTableName, String newIndexTableName) {
        RenameGlobalIndexPreparedData preparedData = new RenameGlobalIndexPreparedData();

        RenameTablePreparedData renameTablePreparedData = new RenameTablePreparedData();

        renameTablePreparedData.setSchemaName(schemaName);
        renameTablePreparedData.setTableName(indexTableName);
        renameTablePreparedData.setNewTableName(newIndexTableName);

        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        TableMeta gsiTableMeta = sm.getTable(indexTableName);
        renameTablePreparedData.setTableVersion(gsiTableMeta.getVersion());

        if (gsiTableMeta.isGsi()) {
            String primaryTableName = gsiTableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            TableMeta primaryTableMeta = sm.getTable(primaryTableName);

            final Set<String> localIndexNames = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            primaryTableMeta.getSecondaryIndexes().forEach(meta -> localIndexNames.add(meta.getPhysicalIndexName()));

            String unwrapName = TddlSqlToRelConverter.unwrapGsiName(indexTableName);
            String newUnwrapName = TddlSqlToRelConverter.unwrapGsiName(newIndexTableName);

            // rename local index
            if (localIndexNames.contains(AUTO_LOCAL_INDEX_PREFIX + unwrapName)) {
                RenameLocalIndexPreparedData renameLocalIndexPreparedData = new RenameLocalIndexPreparedData();

                renameLocalIndexPreparedData.setSchemaName(schemaName);
                renameLocalIndexPreparedData.setOrgIndexName(AUTO_LOCAL_INDEX_PREFIX + unwrapName);
                renameLocalIndexPreparedData.setNewIndexName(AUTO_LOCAL_INDEX_PREFIX + newUnwrapName);

                preparedData.setRenameLocalIndexPreparedData(renameLocalIndexPreparedData);
            }
        }

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(indexTableName);
        preparedData.setIndexTablePreparedData(renameTablePreparedData);

        return preparedData;
    }

    private void prepareAlterTableOnlineModifyColumnData() {
        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        TableMeta tableMeta = sm.getTable(this.tableName);

        AlterTablePreparedData preparedData = new AlterTablePreparedData();
        preparedData.setTableVersion(tableMeta.getVersion());
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);

        List<String> modifiedColumns = getAlteredColumns(sqlAlterTable, SqlAlterTable.ColumnOpt.MODIFY);
        List<Pair<String, String>> changedColumns = new ArrayList<>();
        List<String> updatedColumns = new ArrayList<>(GeneralUtil.emptyIfNull(modifiedColumns));

        String columnName = null;

        boolean isChange = false;
        boolean isTextOrBlob = false;
        boolean isUnique = false;
        String uniqueIndexName = null;

        for (SqlAlterSpecification alterItem : GeneralUtil.emptyIfNull(sqlAlterTable.getAlters())) {
            if (alterItem instanceof SqlChangeColumn) {
                SqlChangeColumn changeColumn = (SqlChangeColumn) alterItem;
                columnName = changeColumn.getOldName().getLastName();
                changedColumns.add(
                    Pair.of(changeColumn.getNewName().getLastName(), changeColumn.getOldName().getLastName()));
                preparedData.setModifyColumnName(changeColumn.getOldName().getLastName());
                isChange = true;
                preparedData.setNewColumnNullable(changeColumn.getColDef().getNotNull() == null
                    || changeColumn.getColDef().getNotNull() == SqlColumnDeclaration.ColumnNull.NULL
                    || changeColumn.getColDef().getDefaultVal() != null);
                isTextOrBlob =
                    changeColumn.getColDef().getDataType().toString().contains("BLOB") || changeColumn.getColDef()
                        .getDataType().toString().contains("TEXT");
                isUnique = changeColumn.getColDef().getSpecialIndex() == SpecialIndex.UNIQUE;
                if (isUnique) {
                    uniqueIndexName = changeColumn.getNewName().getLastName();
                }
            } else {
                final SqlModifyColumn modifyColumn = (SqlModifyColumn) alterItem;
                columnName = modifyColumn.getColName().getLastName();
                updatedColumns.add(columnName);
                preparedData.setModifyColumnName(columnName);

                String tmpColumnName = TableColumnUtils.generateTemporaryName(columnName);
                if (tableMeta.getColumnIgnoreCase(tmpColumnName) != null) {
                    // In case we create a duplicated tmpColumnName, just throw exception and let user retry
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Generate temporary column name failed, please try again");
                }
                preparedData.setTmpColumnName(tmpColumnName);
                preparedData.setNewColumnNullable(modifyColumn.getColDef().getNotNull() == null
                    || modifyColumn.getColDef().getNotNull() == SqlColumnDeclaration.ColumnNull.NULL
                    || modifyColumn.getColDef().getDefaultVal() != null);
                isTextOrBlob =
                    modifyColumn.getColDef().getDataType().toString().contains("BLOB") || modifyColumn.getColDef()
                        .getDataType().toString().contains("TEXT");
                isUnique = modifyColumn.getColDef().getSpecialIndex() == SpecialIndex.UNIQUE;
                if (isUnique) {
                    uniqueIndexName = modifyColumn.getColName().getLastName();
                }
            }
        }

        String checkerColumnName = TableColumnUtils.generateTemporaryName(columnName + "_checker");
        if (tableMeta.getColumnIgnoreCase(checkerColumnName) != null || checkerColumnName.equalsIgnoreCase(
            preparedData.getTmpColumnName())) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "Generate temporary column name failed, please try again");
        }
        preparedData.setCheckerColumnNames(ImmutableList.of(checkerColumnName));

        String finalColumnName = columnName;
        final PlannerContext context = (PlannerContext) this.getCluster().getPlanner().getContext();
        final ParamManager paramManager = context.getParamManager();

        MetaUtils.TableColumns tableColumns = MetaUtils.TableColumns.build(tableMeta);
        if (tableMeta.withGsi() && !tableColumns.isShardingKey(finalColumnName)
            && !tableColumns.isGsiShardingKey(finalColumnName)) {
            // modify sharding key can use omc (with gsi case), and only modify char/varchar(n) --> char/varchar(m) m > n
            for (GsiIndexMetaBean indexMeta : tableMeta.getGsiTableMetaBean().indexMap.values()) {
                if (!indexMeta.columnarIndex &&
                    (indexMeta.clusteredIndex || indexMeta.coveringColumns.stream()
                        .anyMatch(cm -> cm.columnName.equalsIgnoreCase(finalColumnName)))) {
                    if (!paramManager.getBoolean(ConnectionParams.ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI)
                        && !paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Change column name or type included in GSI is not recommended");
                    }
                }
            }
        }

        preparedData.setDroppedColumns(Collections.emptyList());
        preparedData.setAddedColumns(Collections.emptyList());
        preparedData.setUpdatedColumns(updatedColumns);
        preparedData.setChangedColumns(changedColumns);

        preparedData.setDroppedIndexes(Collections.emptyList());
        preparedData.setAddedIndexes(Collections.emptyList());
        preparedData.setRenamedIndexes(Collections.emptyList());
        preparedData.setPrimaryKeyDropped(false);
        preparedData.setAddedPrimaryKeyColumns(Collections.emptyList());

        final ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(preparedData.getModifyColumnName());
        final RelDataType sourceDataType = columnMeta.getField().getRelType();
        preparedData.setOldColumnNullable(
            sourceDataType.isNullable() || StringUtils.isNotEmpty(columnMeta.getField().getDefault()));

        preparedData.setTableComment(null);
        if (isChange) {
            preparedData.setOnlineChangeColumn(true);
        } else {
            preparedData.setOnlineModifyColumn(true);
        }

        String modifyColumnName = preparedData.getModifyColumnName();

        Map<String, Map<String, String>> localIndexNewNameMap = new HashMap<>();
        Map<String, Map<String, String>> localIndexTmpNameMap = new HashMap<>();
        Map<String, Map<String, IndexMeta>> localIndexMeta = new HashMap<>();
        Map<String, String> newUniqueIndexNameMap = new HashMap<>();

        // Primary local index
        Map<String, String> newNameMap = new HashMap<>();
        Map<String, String> tmpNameMap = new HashMap<>();
        Map<String, IndexMeta> indexMetaMap = new HashMap<>();

        Set<String> indexNameSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        genLocalIndexInfo(tableName, tableMeta, modifyColumnName, newNameMap, tmpNameMap, indexMetaMap, isUnique,
            uniqueIndexName, newUniqueIndexNameMap, indexNameSet);

        if (!newNameMap.isEmpty()) {
            localIndexNewNameMap.put(tableName, newNameMap);
            localIndexTmpNameMap.put(tableName, tmpNameMap);
            localIndexMeta.put(tableName, indexMetaMap);
        }

        // GSI local index
        List<String> coveringGsi = new ArrayList<>();

        if (alterTableWithGsiPreparedData != null
            && alterTableWithGsiPreparedData.getGlobalIndexPreparedData() != null) {
            for (AlterTablePreparedData gsiTablePreparedData : alterTableWithGsiPreparedData
                .getGlobalIndexPreparedData()) {
                coveringGsi.add(gsiTablePreparedData.getTableName());
            }
        }
        if (alterTableWithGsiPreparedData != null
            && alterTableWithGsiPreparedData.getClusteredIndexPrepareData() != null) {
            for (AlterTablePreparedData gsiTablePreparedData : alterTableWithGsiPreparedData
                .getClusteredIndexPrepareData()) {
                coveringGsi.add(gsiTablePreparedData.getTableName());
            }
        }

        for (String gsiName : coveringGsi) {
            newNameMap = new HashMap<>();
            tmpNameMap = new HashMap<>();
            indexMetaMap = new HashMap<>();
            TableMeta gsiTableMeta = sm.getTable(gsiName);
            genLocalIndexInfo(gsiName, gsiTableMeta, modifyColumnName, newNameMap, tmpNameMap, indexMetaMap, isUnique,
                uniqueIndexName, newUniqueIndexNameMap, indexNameSet);

            if (!newNameMap.isEmpty()) {
                localIndexNewNameMap.put(gsiName, newNameMap);
                localIndexTmpNameMap.put(gsiName, tmpNameMap);
                localIndexMeta.put(gsiName, indexMetaMap);
            }
        }

        preparedData.setLocalIndexNewNameMap(localIndexNewNameMap);
        preparedData.setLocalIndexTmpNameMap(localIndexTmpNameMap);
        preparedData.setLocalIndexMeta(localIndexMeta);
        preparedData.setNewUniqueIndexNameMap(newUniqueIndexNameMap);

        for (String indexName : indexNameSet) {
            if (tableMeta.withGsi() && tableMeta.hasGsiIgnoreCase(indexName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "Duplicated index name " + indexName);
            }
        }

        boolean localIndexWithoutLength =
            localIndexMeta.values().stream().anyMatch(mp -> mp.values().stream().anyMatch(
                im -> im.getKeyColumnsExt().stream().anyMatch(
                    icm -> icm.getColumnMeta().getName().equalsIgnoreCase(finalColumnName) && icm.getSubPart() == 0)));

        if (localIndexWithoutLength && isTextOrBlob) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                String.format("BLOB/TEXT column '%s' used in key specification without a key length", columnName));
        }

        preparedData.setUseChecker(paramManager.getBoolean(ConnectionParams.COL_CHECK_AFTER_BACK_FILL));
        preparedData.setUseSimpleChecker(paramManager.getBoolean(ConnectionParams.COL_USE_SIMPLE_CHECKER));
        preparedData.setSkipBackfill(paramManager.getBoolean(ConnectionParams.COL_SKIP_BACK_FILL));
        alterTablePreparedData = preparedData;
    }

    private void genLocalIndexInfo(String tableName, TableMeta tableMeta, String modifyColumnName,
                                   Map<String, String> newNameMap, Map<String, String> tmpNameMap,
                                   Map<String, IndexMeta> indexMetaMap, boolean isUnique, String uniqueIndexName,
                                   Map<String, String> newUniqueIndexNameMap,
                                   Set<String> outIndexNameSet) {
        Set<String> tmpIndexNameSet = new HashSet<>();
        List<IndexMeta> indexMetaList = tableMeta.getSecondaryIndexes();
        List<String> indexNames = new ArrayList<>();
        for (IndexMeta indexMeta : indexMetaList) {
            String indexName = indexMeta.getPhysicalIndexName();
            indexNames.add(indexName);
            List<String> columnNames =
                indexMeta.getKeyColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList());
            if (columnNames.stream().noneMatch(cn -> cn.equalsIgnoreCase(modifyColumnName))) {
                continue;
            }

            String newIndexName = TableColumnUtils.generateTemporaryName(indexName);
            if (tmpIndexNameSet.contains(newIndexName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Generate temporary index name failed, please try again");
            }
            tmpIndexNameSet.add(newIndexName);

            String tmpIndexName = TableColumnUtils.generateTemporaryName(indexName);
            if (tmpIndexNameSet.contains(tmpIndexName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Generate temporary index name failed, please try again");
            }
            tmpIndexNameSet.add(tmpIndexName);

            indexMetaMap.put(indexName, indexMeta);
            newNameMap.put(indexName, newIndexName);
            tmpNameMap.put(indexName, tmpIndexName);
        }

        for (String newIndexName : newNameMap.values()) {
            if (newNameMap.containsKey(newIndexName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Generate temporary index name failed, please try again");
            }
        }

        for (String tmpIndexName : tmpNameMap.values()) {
            if (tmpNameMap.containsKey(tmpIndexName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Generate temporary index name failed, please try again");
            }
        }

        if (isUnique) {
            String indexName = TableColumnUtils.generateNewUniqueIndexName(uniqueIndexName, indexNames);
            newUniqueIndexNameMap.put(tableName, indexName);
            outIndexNameSet.add(indexName);
        }
    }

    private void prepareAlterFileStore() {
        // don't perform ddl on oss table when
        if (!CheckOSSArchiveUtil.withoutOldFileStorage(schemaName, tableName)) {
            return;
        }
        Optional<Pair<String, String>> archive = CheckOSSArchiveUtil.getArchive(schemaName, tableName);

        // generate basic alter table actions
        archive.ifPresent(
            x -> {
                alterTableWithFileStorePreparedData =
                    prepareAlterTableData(x.getKey(), x.getValue(), x.getValue(), false);
            });
    }

    private void prepareAlterData() {
        // generate basic alter table actions
        alterTablePreparedData = prepareAlterTableData(tableName);
        alterClusterIndexData();
    }

    /**
     * Apply primary table alters to cluster-index
     */
    private void alterClusterIndexData() {
        final GsiMetaBean gsiMetaBean =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);
        if (!alterTablePreparedData.getAddedIndexes().isEmpty()) {
            // FIXME(moyi) new CreateIndexWithGsiPreparedData in constructor instead of here
            CreateIndexWithGsiPreparedData addIndex =
                this.getOrNewAlterTableWithGsiPreparedData().getOrNewCreateIndexWithGsi();
            for (String indexName : alterTablePreparedData.getAddedIndexes()) {
                addLocalIndexOnClusteredTable(addIndex, indexName, false);
            }
        }

        if (alterTablePreparedData.hasColumnModify()) {
            AlterTableWithGsiPreparedData preparedData = this.getOrNewAlterTableWithGsiPreparedData();
            modifyColumnOnClusteredTable(preparedData, gsiMetaBean);
        }
    }

    public AlterTablePreparedData prepareAlterTableData(String tableName) {
        return prepareAlterTableData(this.schemaName, tableName, this.tableName, false);
    }

    private AlterTablePreparedData prepareAlterTableDataWithRebuild(String tableName) {
        return prepareAlterTableData(this.schemaName, tableName, this.tableName, true);
    }

    private AlterTablePreparedData prepareAlterTableData(String schemaName, String tableName, String primaryName,
                                                         boolean rebuild) {
        AlterTablePreparedData preparedData = new AlterTablePreparedData();
        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();

        SqlIdentifier algorithm =
            sqlAlterTable.getTableOptions() == null ? null : sqlAlterTable.getTableOptions().getAlgorithm();
        if (algorithm != null && algorithm.getSimple().equalsIgnoreCase(Attribute.ALTER_TABLE_ALGORITHM_OMC_INDEX)) {
            preparedData.setOnlineModifyColumnIndexTask(true);
            sqlAlterTable.setSourceSql(
                sqlAlterTable.getSourceSql().replace(Attribute.ALTER_TABLE_ALGORITHM_OMC_INDEX, "INPLACE"));
            sqlAlterTable.getTableOptions().setAlgorithm(new SqlIdentifier("INPLACE", SqlParserPos.ZERO));
        }

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        TableMeta tableMeta = sm.getTable(primaryName);
        TableMeta currentTableMeta = sm.getTable(tableName);
        preparedData.setTableVersion(tableMeta.getVersion());

        if (currentTableMeta.isColumnar()) {
            preparedData.setColumnar(true);
        }

        List<String> droppedColumns = getAlteredColumns(sqlAlterTable, SqlAlterTable.ColumnOpt.DROP);
        List<String> addedColumns = getAlteredColumns(sqlAlterTable, SqlAlterTable.ColumnOpt.ADD);
        Set<String> backfillColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        List<Pair<String, String>> changedColumns = new ArrayList<>();
        List<String> updatedColumns = new ArrayList<>();
        List<String> alterDefaultColumns = new ArrayList<>();
        List<String> alterDefaultNewColumns = new ArrayList<>();
        Map<String, String> isNullableMap = new HashMap<>();

        // Add column for gsi with current_timestamp needs backfill
        if (CollectionUtils.isNotEmpty(addedColumns)) {
            GsiMetaBean gsiMeta = sm.getGsi(primaryName, IndexStatus.ALL);
            if (gsiMeta != null && gsiMeta.isGsi(tableName)) {
                backfillColumns.addAll(PreparedDataUtil.findNeedBackfillColumns(tableMeta, sqlAlterTable));
            }
        }

        boolean primaryKeyDropped = false;
        boolean hasTimestampColumnDefault = false;

        final PlannerContext context = (PlannerContext) this.getCluster().getPlanner().getContext();
        final ParamManager paramManager = context.getParamManager();

        // We have to use the first column name as reference to confirm the physical index name
        // because Alter Table allows to add an index without specifying an index name.
        List<String> addedIndexes = new ArrayList<>();
        List<String> addedIndexesWithoutNames = new ArrayList<>();
        List<String> referencedTables = new ArrayList<>();
        List<ForeignKeyData> addedForeignKeys = new ArrayList<>();
        List<String> droppedForeignKeys = new ArrayList<>();

        List<String> droppedIndexes = new ArrayList<>();
        List<Pair<String, String>> renamedIndexes = new ArrayList<>();
        List<String> addedPrimaryKeyColumns = new ArrayList<>();
        List<Pair<String, String>> columnAfterAnother = new ArrayList<>();
        List<String> dropFiles = new ArrayList<>();

        Map<String, String> specialDefaultValues = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, Long> specialDefaultValueFlags = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        Map<String, Set<String>> allReferencedColumns =
            GeneratedColumnUtil.getAllLogicalReferencedColumnsByGen(tableMeta);
        Set<String> generatedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        generatedColumns.addAll(tableMeta.getLogicalGeneratedColumnNames());
        generatedColumns.addAll(tableMeta.getGeneratedColumnNames());

        List<String> allTableColumns =
            tableMeta.getAllColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList());
        Map<String, Set<String>> genColRefs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        MetaUtils.TableColumns tableColumns = MetaUtils.TableColumns.build(tableMeta);
        Set<String> currentShardingKey;
        if (currentTableMeta.isGsi()) {
            currentShardingKey = tableColumns.gsiShardingKeys.get(tableName);
        } else {
            currentShardingKey = tableColumns.shardingKeys;
        }

        if (sqlAlterTable.getAlters().size() > 1 && checkIfSupportMultipleStatementInAlterTableStmt(schemaName,
            tableName,
            primaryName)) {
            preparedData.setPushDownMultipleStatement(true);
        } else {
            preparedData.setPushDownMultipleStatement(false);
        }
        for (SqlAlterSpecification alterItem : GeneralUtil.emptyIfNull(sqlAlterTable.getAlters())) {
            if (alterItem instanceof SqlChangeColumn) {
                SqlChangeColumn changeColumn = (SqlChangeColumn) alterItem;
                String newColumnName = changeColumn.getNewName().getLastName();
                String oldColumnName = changeColumn.getOldName().getLastName();

                for (Map.Entry<String, Set<String>> entry : allReferencedColumns.entrySet()) {
                    if (entry.getValue().contains(oldColumnName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Can not change column [%s] referenced by a generated column [%s].",
                                oldColumnName, entry.getKey()));
                    }
                }

                if (changeColumn.getColDef().isGeneratedAlways()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Column [%s] can not be changed to a generated column", oldColumnName));
                }

                if (generatedColumns.contains(oldColumnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Can not change generated column [%s]", oldColumnName));
                }

                final Set<AlterColumnSpecification> specificationSet =
                    getAlterColumnSpecification(tableMeta, changeColumn);

                // check sharding key
                if (currentShardingKey.contains(oldColumnName)) {
                    if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                        // drds mode can not modify column type
                        // if not modify column type, then using physical ddl or omc
                        if (specificationSet.stream().anyMatch(ALTER_COLUMN_NAME_OR_TYPE::contains)
                            && !paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support online modify sharding key on drds mode database");
                        }
                    } else if (specificationSet.stream().anyMatch(ALTER_COLUMN_NAME_OR_TYPE::contains)) {
                        // can not modify sharding key directly, should repartition
                        if (!rebuild) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support modify the column type of partition key");
                        }

                        if (!StringUtils.equalsIgnoreCase(newColumnName, oldColumnName)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support change partition key with new column name");
                        }

                        if (specificationSet.stream().anyMatch(ALTER_COLUMN_DEFAULT::contains)) {
                            alterDefaultColumns.add(oldColumnName);
                        }

                        if (!currentTableMeta.getPartitionInfo().isSingleTable()
                            && !currentTableMeta.getPartitionInfo().isBroadcastTable()) {
                            preparedData.setKeepPartitionKeyRange(false);
                            preparedData.setNeedRepartition(true);
                        }
                        changedColumns.add(Pair.of(newColumnName, oldColumnName));
                        continue;
                    }
                }

                if ((tableMeta.withGsi() && tableColumns.existsInGsi(oldColumnName) && specificationSet.stream()
                    .anyMatch(ALTER_COLUMN_DEFAULT::contains)) || rebuild) {
                    alterDefaultColumns.add(oldColumnName);
                    alterDefaultNewColumns.add(newColumnName);
                }

                changedColumns.add(Pair.of(newColumnName, oldColumnName));
                // For time zone conversion
                hasTimestampColumnDefault |= isTimestampColumnWithDefault(changeColumn.getColDef());
                // Need to change logical column order.
                if (changeColumn.isFirst()) {
                    columnAfterAnother.add(new Pair<>(newColumnName, EMPTY_CONTENT));
                }
                if (changeColumn.getAfterColumn() != null) {
                    String afterColumnName = changeColumn.getAfterColumn().getLastName();
                    columnAfterAnother.add(new Pair<>(newColumnName, afterColumnName));
                }
                if (changeColumn.getColDef() != null &&
                    changeColumn.getColDef().getSpecialIndex() != null) {
                    String specialKeyName = changeColumn.getColDef().getSpecialIndex().name();
                    if (TStringUtil.equalsIgnoreCase("PRIMARY", specialKeyName)) {
                        addedPrimaryKeyColumns.add(newColumnName);
                    } else if (TStringUtil.equalsIgnoreCase("UNIQUE", specialKeyName)) {
                        addedIndexesWithoutNames.add(newColumnName);
                    }
                }
                // Check binary default values
                if (changeColumn.getColDef().getDefaultVal() instanceof SqlBinaryStringLiteral) {
                    String hexValue =
                        ((SqlBinaryStringLiteral) changeColumn.getColDef().getDefaultVal()).getBitString()
                            .toHexString();
                    specialDefaultValues.put(newColumnName, hexValue);
                    specialDefaultValueFlags.put(newColumnName, ColumnsRecord.FLAG_BINARY_DEFAULT);
                }
                // check default expr
                if (changeColumn.getColDef().getDefaultExpr() != null && InstanceVersion.isMYSQL80()) {
                    if (!DefaultExprUtil.supportDataType(changeColumn.getColDef())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Do not support data type of column `%s` with default expression.",
                                newColumnName));
                    }
                    SqlCall expr = changeColumn.getColDef().getDefaultExpr();
                    DefaultExprUtil.validateColumnExpr(expr);
                    specialDefaultValues.put(newColumnName, expr.toString());
                    specialDefaultValueFlags.put(newColumnName, ColumnsRecord.FLAG_DEFAULT_EXPR);
                }
            } else if (alterItem instanceof SqlAlterColumnDefaultVal) {
                SqlAlterColumnDefaultVal alterColumnDefaultVal = (SqlAlterColumnDefaultVal) alterItem;
                String columnName = alterColumnDefaultVal.getColumnName().getLastName();

                for (Map.Entry<String, Set<String>> entry : allReferencedColumns.entrySet()) {
                    if (entry.getValue().contains(columnName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Can not change column [%s] referenced by a generated column [%s].",
                                columnName, entry.getKey()));
                    }
                }

                if (generatedColumns.contains(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Can not change generated column [%s]", columnName));
                }

                updatedColumns.add(columnName);
                alterDefaultColumns.add(columnName);
                // Will check if this is a timestamp column with default value later.
                hasTimestampColumnDefault = true;
                // Check binary default values
                if (alterColumnDefaultVal.getDefaultVal() instanceof SqlBinaryStringLiteral) {
                    String hexValue =
                        ((SqlBinaryStringLiteral) alterColumnDefaultVal.getDefaultVal()).getBitString().toHexString();
                    specialDefaultValues.put(columnName, hexValue);
                    specialDefaultValueFlags.put(columnName, ColumnsRecord.FLAG_BINARY_DEFAULT);
                }
                // check default expr
                if (alterColumnDefaultVal.getDefaultExpr() != null && InstanceVersion.isMYSQL80()) {
                    SqlCall expr = alterColumnDefaultVal.getDefaultExpr();
                    DefaultExprUtil.validateColumnExpr(expr);
                    specialDefaultValues.put(columnName, expr.toString());
                    specialDefaultValueFlags.put(columnName, ColumnsRecord.FLAG_DEFAULT_EXPR);
                }
            } else if (alterItem instanceof SqlAddForeignKey) {
                if (sqlAlterTable.getAlters().size() > 1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support multi ALTER statements when adding foreign key");
                }

                ForeignKeyData foreignKeyData = new ForeignKeyData();
                foreignKeyData.constraint = ((SqlAddForeignKey) alterItem).getConstraint() != null ?
                    SQLUtils.normalize(((SqlAddForeignKey) alterItem).getConstraint().getLastName()) : null;
                foreignKeyData.indexName = ((SqlAddForeignKey) alterItem).getIndexName() != null ?
                    SQLUtils.normalize(((SqlAddForeignKey) alterItem).getIndexName().getLastName()) : null;
                foreignKeyData.columns = ((SqlAddForeignKey) alterItem).getIndexDef().getColumns().stream()
                    .map(c -> c.getColumnNameStr().toLowerCase()).collect(Collectors.toList());
                foreignKeyData.refSchema = ((SqlAddForeignKey) alterItem).getSchemaName();
                foreignKeyData.refTableName =
                    SQLUtils.normalize(
                        ((SqlAddForeignKey) alterItem).getReferenceDefinition().getTableName().getLastName()
                            .toLowerCase());
                foreignKeyData.refColumns =
                    ((SqlAddForeignKey) alterItem).getReferenceDefinition().getColumns().stream()
                        .map(c -> c.getLastName().toLowerCase()).collect(Collectors.toList());
                foreignKeyData.setPushDown(((SqlAddForeignKey) alterItem).isPushDown());
                if (((SqlAddForeignKey) alterItem).getReferenceDefinition().getreferenceOptions() != null) {
                    for (SqlReferenceOption option : ((SqlAddForeignKey) alterItem).getReferenceDefinition()
                        .getreferenceOptions()) {
                        if (option.getOnType() == SqlReferenceOption.OnType.ON_UPDATE) {
                            foreignKeyData.onUpdate =
                                option.convertReferenceOptionType(option.getReferenceOptionType());
                        }
                        if (option.getOnType() == SqlReferenceOption.OnType.ON_DELETE) {
                            foreignKeyData.onDelete =
                                option.convertReferenceOptionType(option.getReferenceOptionType());
                        }
                    }
                }

                if (foreignKeyData.isPushDown() && foreignKeyData.constraint == null) {
                    foreignKeyData.constraint = ForeignKeyUtils.getForeignKeyConstraintName(schemaName, tableName);
                    ((SqlAddForeignKey) alterItem).setConstraint(
                        new SqlIdentifier(SQLUtils.normalizeNoTrim(foreignKeyData.constraint),
                            SqlParserPos.ZERO));

                }

                if (((SqlAddForeignKey) alterItem).isPushDown()) {
                    // Physical FK.
                    referencedTables.add(foreignKeyData.refTableName); // Add to param generate.
                    addedForeignKeys.add(foreignKeyData);
                } else {
                    // Logical FK.
                    addedForeignKeys.add(foreignKeyData);
                }
            } else if (alterItem instanceof SqlDropForeignKey) {
                if (sqlAlterTable.getAlters().size() > 1) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support multi ALTER statements when dropping foreign key");
                }
                SqlDropForeignKey dropForeignKey = (SqlDropForeignKey) alterItem;
                droppedForeignKeys.add(dropForeignKey.getIndexName().getLastName());
            } else if (alterItem instanceof SqlAddIndex) {
                SqlAddIndex addIndex = (SqlAddIndex) alterItem;
                if (addIndex.getIndexName() != null) {
                    addedIndexes.add(addIndex.getIndexName().getLastName());
                } else {
                    String firstColumnName = addIndex.getIndexDef().getColumns().get(0).getColumnNameStr();
                    // If user doesn't specify an index name, we use the first column name as reference.
                    addedIndexesWithoutNames.add(firstColumnName);
                }
            } else if (alterItem instanceof SqlAlterTableDropIndex) {
                SqlAlterTableDropIndex dropIndex = (SqlAlterTableDropIndex) alterItem;
                droppedIndexes.add(dropIndex.getIndexName().getLastName());
            } else if (alterItem instanceof SqlAlterTableDropFile) {
                SqlAlterTableDropFile sqlAlterTableDropFile = (SqlAlterTableDropFile) alterItem;

                // remove duplicated file names
                sqlAlterTableDropFile.getFileNames().stream().map(sqlIdentifier -> sqlIdentifier.getLastName())
                    .distinct().forEach(dropFiles::add);

            } else if (alterItem instanceof SqlAlterTableRenameIndex) {
                SqlAlterTableRenameIndex renameIndex = (SqlAlterTableRenameIndex) alterItem;
                renamedIndexes.add(
                    Pair.of(renameIndex.getNewIndexName().getLastName(), renameIndex.getIndexName().getLastName()));
            } else if (alterItem instanceof SqlModifyColumn) {
                final SqlModifyColumn modifyColumn = (SqlModifyColumn) alterItem;
                String columnName = modifyColumn.getColName().getLastName();

                for (Map.Entry<String, Set<String>> entry : allReferencedColumns.entrySet()) {
                    if (entry.getValue().contains(columnName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Can not modify column [%s] referenced by a generated column [%s].",
                                columnName, entry.getKey()));
                    }
                }

                if (modifyColumn.getColDef().isGeneratedAlways()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Column [%s] can not be modified to a generated column", columnName));
                }

                if (generatedColumns.contains(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        String.format("Can not modify generated column [%s]", columnName));
                }

                final Set<AlterColumnSpecification> specificationSet =
                    getAlterColumnSpecification(tableMeta, modifyColumn);

                // check sharding key
                if (currentShardingKey.contains(columnName)) {
                    if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                        // drds mode can not modify column type
                        if (specificationSet.stream().anyMatch(ALTER_COLUMN_NAME_OR_TYPE::contains)
                            && !paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support online modify sharding key on drds mode database");
                        }
                    } else if (specificationSet.stream().anyMatch(ALTER_COLUMN_NAME_OR_TYPE::contains)) {
                        // can not modify sharding key directly
                        // 1. change partition route
                        // 2. do not change partition route, but change column type, such as changing varchar length.
                        //    if the column is primary key, can not use OMC, than using repartition instead
                        if (!rebuild) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Do not support modify the column type of partition key");
                        }

                        if (specificationSet.stream().anyMatch(ALTER_COLUMN_DEFAULT::contains)) {
                            alterDefaultColumns.add(columnName);
                        }

                        if (!currentTableMeta.getPartitionInfo().isSingleTable()
                            && !currentTableMeta.getPartitionInfo().isBroadcastTable()) {
                            preparedData.setKeepPartitionKeyRange(false);
                            preparedData.setNeedRepartition(true);
                        }
                        updatedColumns.add(columnName);
                        continue;
                    }
                }

                if ((tableMeta.withGsi() && tableColumns.existsInGsi(columnName) && specificationSet.stream()
                    .anyMatch(ALTER_COLUMN_DEFAULT::contains)) || rebuild) {
                    alterDefaultColumns.add(columnName);
                }

                updatedColumns.add(columnName);
                // For time zone conversion
                hasTimestampColumnDefault |= isTimestampColumnWithDefault(modifyColumn.getColDef());
                // Need to change logical column order.
                if (modifyColumn.isFirst()) {
                    columnAfterAnother.add(new Pair<>(columnName, EMPTY_CONTENT));
                }
                if (modifyColumn.getAfterColumn() != null) {
                    String afterColumnName = modifyColumn.getAfterColumn().getLastName();
                    columnAfterAnother.add(new Pair<>(columnName, afterColumnName));
                }
                if (modifyColumn.getColDef() != null &&
                    modifyColumn.getColDef().getSpecialIndex() != null) {
                    String specialKeyName = modifyColumn.getColDef().getSpecialIndex().name();
                    if (TStringUtil.equalsIgnoreCase("PRIMARY", specialKeyName)) {
                        addedPrimaryKeyColumns.add(columnName);
                    } else if (TStringUtil.equalsIgnoreCase("UNIQUE", specialKeyName)) {
                        addedIndexesWithoutNames.add(columnName);
                    }
                }
                // Check binary default values
                if (modifyColumn.getColDef().getDefaultVal() instanceof SqlBinaryStringLiteral) {
                    String hexValue =
                        ((SqlBinaryStringLiteral) modifyColumn.getColDef().getDefaultVal()).getBitString()
                            .toHexString();
                    specialDefaultValues.put(columnName, hexValue);
                    specialDefaultValueFlags.put(columnName, ColumnsRecord.FLAG_BINARY_DEFAULT);
                }
                // check default expr
                if (modifyColumn.getColDef().getDefaultExpr() != null && InstanceVersion.isMYSQL80()) {
                    if (!DefaultExprUtil.supportDataType(modifyColumn.getColDef())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Do not support data type of column `%s` with default expression.",
                                columnName));
                    }
                    SqlCall expr = modifyColumn.getColDef().getDefaultExpr();
                    DefaultExprUtil.validateColumnExpr(expr);
                    specialDefaultValues.put(columnName, expr.toString());
                    specialDefaultValueFlags.put(columnName, ColumnsRecord.FLAG_DEFAULT_EXPR);
                }
            } else if (alterItem instanceof SqlDropPrimaryKey) {
                primaryKeyDropped = true;
                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    if (tableColumns.isPrimaryKey(IMPLICIT_COL_NAME)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "drop primary key is not supported yet");
                    } else if (sqlAlterTable.getAlters().size() != 2
                        || !(sqlAlterTable.getAlters().get(0) instanceof SqlDropPrimaryKey)
                        || !(sqlAlterTable.getAlters().get(1) instanceof SqlAddPrimaryKey)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_DROP_PRIMARY_KEY,
                            "can only modify the primary key with alter table drop primary key, add primary key");
                    }
                }
                droppedIndexes.add("PRIMARY");
            } else if (alterItem instanceof SqlAddPrimaryKey) {
                SqlAddPrimaryKey addPrimaryKey = (SqlAddPrimaryKey) alterItem;
                for (SqlIndexColumnName indexColumnName : addPrimaryKey.getColumns()) {
                    addedPrimaryKeyColumns.add(indexColumnName.getColumnNameStr());
                    ColumnMeta cm = tableMeta.getColumnIgnoreCase(indexColumnName.getColumnNameStr());
                    if (cm != null && cm.isGeneratedColumn()) {
                        if (!paramManager.getBoolean(ConnectionParams.ENABLE_UNIQUE_KEY_ON_GEN_COL)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "create unique index on VIRTUAL/STORED generated column is not enabled");
                        }
                    }
                }
                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    preparedData.setNeedRepartition(true);
                    preparedData.setKeepPartitionKeyRange(true);

                    if (tableColumns.isPrimaryKey(IMPLICIT_COL_NAME)) {
                        if (tableMeta.isAutoPartition()) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "add primary key is not supported on the table which has implicit primary key ");
                        }

                        if (sqlAlterTable.getAlters().size() != 1) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "can only modify the primary key with alter table add primary key");
                        }
                        preparedData.setNeedDropImplicitKey(true);
                    } else if (sqlAlterTable.getAlters().size() != 2
                        || !(sqlAlterTable.getAlters().get(0) instanceof SqlDropPrimaryKey)
                        || !(sqlAlterTable.getAlters().get(1) instanceof SqlAddPrimaryKey)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ADD_PRIMARY_KEY,
                            "Can only modify the primary key with alter table drop primary key, add primary key(column)");
                    }
                }
            } else if (alterItem instanceof SqlAddColumn) {
                SqlAddColumn addColumn = (SqlAddColumn) alterItem;
                String newColumnName = addColumn.getColName().getLastName();
                // For time zone conversion
                hasTimestampColumnDefault |= isTimestampColumnWithDefault(addColumn.getColDef());
                // Need to change logical column order.
                int insertIndex = allTableColumns.size();
                if (addColumn.isFirst()) {
                    columnAfterAnother.add(new Pair<>(newColumnName, EMPTY_CONTENT));
                    insertIndex = 0;
                }
                if (addColumn.getAfterColumn() != null) {
                    String afterColumnName = addColumn.getAfterColumn().getLastName();
                    if (!TStringUtil.equalsIgnoreCase(newColumnName, afterColumnName)) {
                        columnAfterAnother.add(new Pair<>(newColumnName, afterColumnName));
                    }
                    for (int i = 0; i < allTableColumns.size(); i++) {
                        if (allTableColumns.get(i).equalsIgnoreCase(addColumn.getAfterColumn().getLastName())) {
                            insertIndex = i + 1;
                            break;
                        }
                    }
                }
                allTableColumns.add(insertIndex, newColumnName);

                final SqlColumnDeclaration.ColumnNull notNull = addColumn.getColDef().getNotNull();
                isNullableMap.put(newColumnName,
                    (null == notNull || SqlColumnDeclaration.ColumnNull.NULL == notNull) ? "YES" : "");
                // Primary or unique key with column added
                SpecialIndex specialIndex = addColumn.getColDef().getSpecialIndex();
                if (specialIndex == SpecialIndex.UNIQUE) {
                    addedIndexesWithoutNames.add(newColumnName);
                } else if (specialIndex == SpecialIndex.PRIMARY) {
                    addedPrimaryKeyColumns.add(newColumnName);
                }
                // Check binary default values
                if (addColumn.getColDef().getDefaultVal() instanceof SqlBinaryStringLiteral) {
                    String hexValue =
                        ((SqlBinaryStringLiteral) addColumn.getColDef().getDefaultVal()).getBitString()
                            .toHexString();
                    specialDefaultValues.put(newColumnName, hexValue);
                    specialDefaultValueFlags.put(newColumnName, ColumnsRecord.FLAG_BINARY_DEFAULT);
                }
                // check default expr
                if (addColumn.getColDef().getDefaultExpr() != null && InstanceVersion.isMYSQL80()) {
                    specialDefaultValues.put(newColumnName, addColumn.getColDef().getDefaultExpr().toString());
                    specialDefaultValueFlags.put(newColumnName, ColumnsRecord.FLAG_DEFAULT_EXPR);
                }
                // Check generated column
                if (addColumn.getColDef().isGeneratedAlwaysLogical()) {
                    if (addColumn.getColDef().isAutoIncrement()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Generated column [%s] can not be auto_increment column.", newColumnName));
                    }
                    if (addColumn.getColDef().isOnUpdateCurrentTimestamp()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Generated column [%s] can not be auto update column.", newColumnName));
                    }
                    if (!GeneratedColumnUtil.supportDataType(addColumn.getColDef())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Do not support data type of generated column [%s].", newColumnName));
                    }

                    //  validate column in expression exists
                    SqlCall expr = addColumn.getColDef().getGeneratedAlwaysExpr();
                    GeneratedColumnUtil.validateGeneratedColumnExpr(expr);
                    Set<String> referencedColumns = GeneratedColumnUtil.getReferencedColumns(expr);
                    genColRefs.put(newColumnName, referencedColumns);
                    specialDefaultValues.put(newColumnName, expr.toString());
                    specialDefaultValueFlags.put(newColumnName, ColumnsRecord.FLAG_LOGICAL_GENERATED_COLUMN);
                    backfillColumns.add(newColumnName);
                } else if (addColumn.getColDef().isGeneratedAlways()) {
                    SqlCall expr = addColumn.getColDef().getGeneratedAlwaysExpr();
                    GeneratedColumnUtil.validateGeneratedColumnExpr(expr);
                    specialDefaultValues.put(newColumnName, expr.toString());
                    specialDefaultValueFlags.put(newColumnName, ColumnsRecord.FLAG_GENERATED_COLUMN);
                } else if (addColumn.getColDef().getDefaultExpr() != null && InstanceVersion.isMYSQL80()) {
                    if (!DefaultExprUtil.supportDataType(addColumn.getColDef())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Do not support data type of column `%s` with default expression.",
                                newColumnName));
                    }
                    SqlCall expr = addColumn.getColDef().getDefaultExpr();
                    DefaultExprUtil.validateColumnExpr(expr);
                    specialDefaultValues.put(newColumnName, expr.toString());
                    specialDefaultValueFlags.put(newColumnName, ColumnsRecord.FLAG_DEFAULT_EXPR);
                }
            } else if (alterItem instanceof SqlDropColumn) {
                SqlDropColumn dropColumn = (SqlDropColumn) alterItem;
                String columnName = dropColumn.getColName().getLastName();

                for (Map.Entry<String, Set<String>> entry : allReferencedColumns.entrySet()) {
                    if (entry.getValue().contains(columnName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Can not drop column [%s] referenced by a generated column [%s].",
                                columnName, entry.getKey()));
                    }
                }
            } else if (alterItem instanceof SqlConvertToCharacterSet) {
                SqlConvertToCharacterSet convertCharset = (SqlConvertToCharacterSet) alterItem;
                preparedData.setCharset(convertCharset.getCharset());
                preparedData.setCollate(convertCharset.getCollate());
                updatedColumns.addAll(
                    currentTableMeta.getAllColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList()));
                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    boolean reHash = false;
                    if (CollectionUtils.isNotEmpty(tableColumns.shardingKeys)) {
                        reHash = tableColumns.shardingKeys.stream().anyMatch(
                            e -> tableMeta.getColumnIgnoreCase(e).getDataType().getSqlType() == Types.VARCHAR
                                || tableMeta.getColumnIgnoreCase(e).getDataType().getSqlType() == Types.CHAR);
                    }

                    if (CollectionUtils.isNotEmpty(tableColumns.gsiShardingKeys.values())) {
                        for (Set<String> cset : tableColumns.gsiShardingKeys.values()) {
                            reHash = reHash || cset.stream().anyMatch(
                                e -> tableMeta.getColumnIgnoreCase(e).getDataType().getSqlType() == Types.VARCHAR
                                    || tableMeta.getColumnIgnoreCase(e).getDataType().getSqlType() == Types.CHAR);
                        }
                    }

                    if (reHash) {
                        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNSUPPORTED,
                            "ALTER TABLE CONVERT TO CHARACTER SET is not supported yet.");
                    }
                }
            } else if (alterItem instanceof SqlEnableKeys) {
                SqlEnableKeys enableKeys = (SqlEnableKeys) alterItem;
                preparedData.setEnableKeys(enableKeys.getEnableType());
            } else if (alterItem instanceof SqlAlterTableExchangePartition) {
                // do nothing
            } else if (alterItem instanceof SqlAlterTableAlterIndex) {
                SqlAlterTableAlterIndex sqlAlterTableAlterIndex = (SqlAlterTableAlterIndex) alterItem;
                if (sqlAlterTableAlterIndex.isAlterIndexVisibility()) {
                    List<Pair<String, String>> alterIndexVisibility = new ArrayList<>();
                    alterIndexVisibility.add(new Pair<>(sqlAlterTableAlterIndex.getIndexName().getLastName(),
                        sqlAlterTableAlterIndex.getVisibility()));
                    preparedData.setIndexVisibility(alterIndexVisibility);
                }
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNSUPPORTED, "alter type: " + alterItem);
            }
        }

        String tableComment = null;
        String tableRowFormat = null;
        if (sqlAlterTable.getTableOptions() != null) {
            SqlTableOptions tableOptions = sqlAlterTable.getTableOptions();
            if (tableOptions.getComment() != null) {
                tableComment = tableOptions.getComment().toValue();
            }
            if (tableOptions.getRowFormat() != null) {
                tableRowFormat = tableOptions.getRowFormat().name();
            }
        }

        if (sqlAlterTable instanceof SqlAlterTableAsOfTimeStamp) {
            preparedData.setTimestamp(
                ((SqlAlterTableAsOfTimeStamp) sqlAlterTable).getTimestamp().getNlsString().getValue());
        } else if (sqlAlterTable instanceof SqlAlterTablePurgeBeforeTimeStamp) {
            preparedData.setTimestamp(
                ((SqlAlterTablePurgeBeforeTimeStamp) sqlAlterTable).getTimestamp().getNlsString().getValue());
        }

        // Validate add generated column
        int genColCount = 0;
        int otherCount = 0;
        for (SqlAlterSpecification alterItem : sqlAlterTable.getAlters()) {
            if (alterItem instanceof SqlAddColumn && ((SqlAddColumn) alterItem).getColDef()
                .isGeneratedAlwaysLogical()) {
                genColCount++;
            } else {
                otherCount++;
            }
        }

        if (genColCount != 0 && otherCount != 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "Do not support mix ADD GENERATED COLUMNS with other ALTER statements");
        }

        if (genColCount != 0 && sqlAlterTable.getTableOptions() != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "Do not support mix ADD GENERATED COLUMNS with other table options");
        }

        // Validate drop generated column
        genColCount = 0;
        otherCount = 0;
        for (SqlAlterSpecification alterItem : sqlAlterTable.getAlters()) {
            if (alterItem instanceof SqlDropColumn) {
                String colName = ((SqlDropColumn) alterItem).getColName().getLastName();
                ColumnMeta columnMeta = tableMeta.getColumn(colName);
                if (columnMeta != null && columnMeta.isLogicalGeneratedColumn()) {
                    genColCount++;
                } else {
                    otherCount++;
                }
            } else {
                otherCount++;
            }
        }

        if (genColCount != 0 && otherCount != 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "Do not support mix DROP GENERATED COLUMNS with other ALTER statements");
        }

        if (genColCount != 0 && sqlAlterTable.getTableOptions() != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "Do not support mix DROP GENERATED COLUMNS with other table options");
        }

        // For add generated columns
        if (specialDefaultValueFlags.values().stream()
            .anyMatch(l -> l == ColumnsRecord.FLAG_LOGICAL_GENERATED_COLUMN)) {
            // Check referenced column exists
            for (Map.Entry<String, Set<String>> entry : genColRefs.entrySet()) {
                for (String refCol : entry.getValue()) {
                    ColumnMeta refColMeta = tableMeta.getColumnIgnoreCase(refCol);
                    if (refColMeta == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Referenced column [%s] does not exist.", refCol));
                    }
                    // If it's added generated column, we have checked its data type before
                    if (!GeneratedColumnUtil.supportDataType(refColMeta)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Do not support data type of referenced column [%s].", refCol));
                    }
                    if (refColMeta.isAutoIncrement()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Referenced column [%s] can not be auto_increment column.", refCol));
                    }

                    if (refColMeta.isGeneratedColumn()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Referenced column [%s] can not be non-logical generated column.", refCol));
                    }

                    if (refColMeta.isAutoUpdateColumn()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            String.format("Referenced column [%s] can not be auto update column.", refCol));
                    }
                }
            }

            // Check cycle, only check newly added columns is enough, since it's impossible to have existing column
            // referring to added column
            GeneratedColumnUtil.getGeneratedColumnEvaluationOrder(genColRefs);
            preparedData.setForceCnEval(paramManager.getBoolean(ConnectionParams.GEN_COL_FORCE_CN_EVAL));
        }

        preparedData.setAlterDefaultColumns(alterDefaultColumns);
        preparedData.setAlterDefaultNewColumns(alterDefaultNewColumns);
        preparedData.setDroppedColumns(droppedColumns);
        preparedData.setAddedColumns(addedColumns);
        preparedData.setUpdatedColumns(updatedColumns);
        preparedData.setChangedColumns(changedColumns);
        preparedData.setTimestampColumnDefault(hasTimestampColumnDefault);
        preparedData.setSpecialDefaultValues(specialDefaultValues);
        preparedData.setSpecialDefaultValueFlags(specialDefaultValueFlags);
        preparedData.setDroppedIndexes(droppedIndexes);
        preparedData.setAddedIndexes(addedIndexes);
        preparedData.setAddedIndexesWithoutNames(addedIndexesWithoutNames);
        preparedData.setReferencedTables(referencedTables);
        preparedData.setAddedForeignKeys(addedForeignKeys);
        preparedData.setDroppedForeignKeys(droppedForeignKeys);
        preparedData.setRenamedIndexes(renamedIndexes);
        preparedData.setPrimaryKeyDropped(primaryKeyDropped);
        preparedData.setAddedPrimaryKeyColumns(addedPrimaryKeyColumns);
        preparedData.setColumnAfterAnother(columnAfterAnother);
        preparedData.setTableComment(tableComment);
        preparedData.setTableRowFormat(tableRowFormat);
        preparedData.setDropFiles(dropFiles);
        preparedData.setBackfillColumns(new ArrayList<>(backfillColumns));
        preparedData.setIsNullableMap(isNullableMap);
        return preparedData;
    }

    public static boolean hasSequence(String schemaName, String sourceTableName) {
        String seqName = AUTO_SEQ_PREFIX + sourceTableName;
        SequenceAttribute.Type existingSeqType =
            SequenceManagerProxy.getInstance().checkIfExists(schemaName, seqName);
        return existingSeqType != SequenceAttribute.Type.NA;
    }

    public boolean validateOnlineModify(ExecutionContext ec, boolean forceOmc) {
        final ParamManager paramManager = ec.getParamManager();
        if (!(paramManager.getBoolean(ConnectionParams.FORCE_USING_OMC) || forceOmc)) {
            if (sqlAlterTable.getTableOptions() == null) {
                return false;
            }

            SqlIdentifier algorithm = sqlAlterTable.getTableOptions().getAlgorithm();
            if (!(algorithm != null && algorithm.getSimple().equalsIgnoreCase(Attribute.ALTER_TABLE_ALGORITHM_OMC))) {
                return false;
            }
        }

        for (SqlAlterSpecification alterItem : sqlAlterTable.getAlters()) {
            SqlKind alterType = alterItem.getKind();
            if (alterType != SqlKind.MODIFY_COLUMN && alterType != SqlKind.CHANGE_COLUMN
                && alterType != SqlKind.ADD_COLUMN && alterType != SqlKind.DROP_COLUMN) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    "Online column modify only supports modify column or change column");
            }
        }

        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        MetaUtils.TableColumns tableColumns = MetaUtils.TableColumns.build(tableMeta);

        final Map<String, Set<String>> allReferencedColumns =
            GeneratedColumnUtil.getAllLogicalReferencedColumnsByGen(tableMeta);

        Set<String> generatedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        generatedColumns.addAll(tableMeta.getLogicalGeneratedColumnNames());
        generatedColumns.addAll(tableMeta.getGeneratedColumnNames());

        boolean needOmc = false;
        int i = -1;
        for (SqlAlterSpecification alterItem : sqlAlterTable.getAlters()) {
            i++;
            SqlKind alterType = alterItem.getKind();
            String columnName = null;
            SqlColumnDeclaration columnDeclaration = null;
            Set<AlterColumnSpecification> specificationSet = null;
            boolean changeWithSameName = true;

            if (alterType == SqlKind.MODIFY_COLUMN) {
                SqlModifyColumn modifyColumn = (SqlModifyColumn) alterItem;
                columnName = modifyColumn.getColName().getLastName();
                columnDeclaration = modifyColumn.getColDef();
                specificationSet = getAlterColumnSpecification(tableMeta, alterItem);

                for (Map.Entry<String, Set<String>> entry : allReferencedColumns.entrySet()) {
                    if (entry.getValue().contains(columnName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                            String.format("Can not modify column [%s] referenced by a generated column [%s].",
                                columnName, entry.getKey()));
                    }
                }

                if (modifyColumn.getColDef().isGeneratedAlwaysLogical()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        String.format("Column [%s] can not be modified to a generated column", columnName));
                }

                if (generatedColumns.contains(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        String.format("Can not modify generated column [%s]", columnName));
                }

                if (modifyColumn.getColDef().getSpecialIndex() != null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        "Do not support modify the column with unique key or primary key when using online modify column");
                }

                if (modifyColumn.getAfterColumn() != null) {
                    String afterColumn = modifyColumn.getAfterColumn().getLastName();
                    if (afterColumn.equalsIgnoreCase(columnName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                            "Do not support insert after the same column");
                    }
                }

                if (modifyColumn.getColDef().isAutoIncrement() && !hasSequence(schemaName, tableName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, String.format(
                        "Missing sequence for auto increment column. Please try to execute this command first: \"CREATE SEQUENCE `AUTO_SEQ_%s`\"",
                        tableName));
                }
            } else if (alterType == SqlKind.CHANGE_COLUMN) {
                SqlChangeColumn changeColumn = (SqlChangeColumn) alterItem;
                columnName = changeColumn.getOldName().getLastName();
                columnDeclaration = changeColumn.getColDef();
                specificationSet = getAlterColumnSpecification(tableMeta, alterItem);

                for (Map.Entry<String, Set<String>> entry : allReferencedColumns.entrySet()) {
                    if (entry.getValue().contains(columnName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                            String.format("Can not change column [%s] referenced by a generated column [%s].",
                                columnName, entry.getKey()));
                    }
                }

                if (changeColumn.getColDef().isGeneratedAlwaysLogical()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        String.format("Column [%s] can not be changed to a generated column", columnName));
                }

                if (changeColumn.getColDef().getSpecialIndex() != null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        "Do not support change the column with unique key or primary key when using online modify column");
                }

                if (generatedColumns.contains(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        String.format("Can not change generated column [%s]", columnName));
                }

                if (changeColumn.getAfterColumn() != null) {
                    String afterColumn = changeColumn.getAfterColumn().getLastName();
                    if (afterColumn.equalsIgnoreCase(columnName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                            "Do not support insert after the same column");
                    }
                }

                if (changeColumn.getColDef().isAutoIncrement() && !hasSequence(schemaName, tableName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, String.format(
                        "Missing sequence for auto increment column. Please try to execute this command first: \"CREATE SEQUENCE `AUTO_SEQ_%s`\"",
                        tableName));
                }

                String newColumnName = changeColumn.getNewName().getLastName();
                if (newColumnName.equalsIgnoreCase(columnName)) {
                    // Rewrite to modify
                    SqlModifyColumn modifyColumn =
                        new SqlModifyColumn(changeColumn.getOriginTableName(), changeColumn.getOldName(),
                            columnDeclaration,
                            changeColumn.isFirst(), changeColumn.getAfterColumn(), changeColumn.getSourceSql(),
                            SqlParserPos.ZERO);
                    sqlAlterTable.getAlters().set(i, modifyColumn);
                } else if (tableColumns.isPrimaryKey(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        "Online modify primary key name is not supported");
                } else {
                    changeWithSameName = false;
                }
            } else if (alterType == SqlKind.ADD_COLUMN) {
                SqlAddColumn sqlAddColumn = (SqlAddColumn) alterItem;
                columnName = sqlAddColumn.getColName().getLastName();
                if (tableMeta.getColumn(columnName) != null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        "Do not support add the column which is already existed when using online modify column");
                }
                if (sqlAddColumn.getColDef().getNotNull() != null && sqlAddColumn.getColDef().getDefaultVal() == null
                    && SQLMode.isStrictMode(ec.getSqlModeFlags())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        "Do not support add the column which is not null and have no default value when using online modify column");
                }
                if (sqlAddColumn.getColDef().getSpecialIndex() != null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        "Do not support add the column with unique key or primary key when using online modify column");
                }
                if (sqlAddColumn.getColDef().getGeneratedAlwaysExpr() != null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        "Do not support add the column with generated expr when using online modify column");
                }
                if (sqlAddColumn.getColDef().isAutoIncrement() && !hasSequence(schemaName, tableName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, String.format(
                        "Missing sequence for auto increment column. Please try to execute this command first: \"CREATE SEQUENCE `AUTO_SEQ_%s`\"",
                        tableName));
                }
                continue;
            } else if (alterType == SqlKind.DROP_COLUMN) {
                SqlDropColumn sqlDropColumn = (SqlDropColumn) alterItem;
                columnName = sqlDropColumn.getColName().getLastName();
                if (tableMeta.getColumn(columnName) == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        String.format("Can't DROP '%s'; check that column/key exists", columnName));
                }
                if (tableColumns.isPrimaryKey(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        String.format("Do not support drop primary key[%s])", columnName));
                }
                if (tableColumns.isShardingKey(columnName) || tableColumns.isGsiShardingKey(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        String.format("Do not support drop sharding key[%s])", columnName));
                }
                continue;
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    "Online modify column only supports modify / change column");
            }

            final ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(columnName);
            if (columnMeta == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    "Modify unknown column '" + columnName + "'");
            }

            if (tableColumns.isShardingKey(columnName) || tableColumns.isGsiShardingKey(columnName)) {
                if (!changeWithSameName) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        "Do not support change the column name of sharding key");
                }

                if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    if (!paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                            "Do not support change sharding key column type on drds mode database");
                    }
                }
            }

            final boolean forceTypeConversion = paramManager.getBoolean(ConnectionParams.OMC_FORCE_TYPE_CONVERSION);

            if (forceTypeConversion || specificationSet.stream().anyMatch(ALTER_COLUMN_NAME_OR_TYPE::contains)) {
                needOmc = true;
            }
        }

        if (!needOmc) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "It seems that you do not alter column type, try to turn off ALGORITHM=OMC for better practice");
        }

        return true;
    }

    public boolean autoConvertToOmc(ExecutionContext ec) {
        // hint
        final ParamManager paramManager = ec.getParamManager();
        if (!paramManager.getBoolean(ConnectionParams.ENABLE_AUTO_OMC)) {
            return false;
        }

        // omc usual check, such as auto increment, generate column, fulltext index, hint,,,
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        MetaUtils.TableColumns tableColumns = MetaUtils.TableColumns.build(tableMeta);

        boolean needOmc = false;
        for (SqlAlterSpecification alterItem : sqlAlterTable.getAlters()) {
            SqlKind alterType = alterItem.getKind();
            String columnName = null;
            SqlColumnDeclaration columnDeclaration = null;
            Set<AlterColumnSpecification> specificationSet = null;
            boolean changeWithSameName = true;

            if (alterType == SqlKind.MODIFY_COLUMN) {
                specificationSet = getAlterColumnSpecification(tableMeta, alterItem);
                SqlModifyColumn modifyColumn = (SqlModifyColumn) alterItem;
                columnName = modifyColumn.getColName().getLastName();
                columnDeclaration = modifyColumn.getColDef();
            } else if (alterType == SqlKind.CHANGE_COLUMN) {
                specificationSet = getAlterColumnSpecification(tableMeta, alterItem);
                SqlChangeColumn changeColumn = (SqlChangeColumn) alterItem;
                columnName = changeColumn.getOldName().getLastName();
                columnDeclaration = changeColumn.getColDef();

                String newColumnName = changeColumn.getNewName().getLastName();
                if (!newColumnName.equalsIgnoreCase(columnName)) {
                    changeWithSameName = false;
                }
            } else {
                continue;
            }

            final ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(columnName);
            if (columnMeta == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    "Modify unknown column '" + columnName + "'");
            }

            // modify partition key
            if (tableColumns.isShardingKey(columnName) || tableColumns.isGsiShardingKey(columnName)) {
                if (!changeWithSameName) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        "Do not support change the column name of sharding key");
                }

                if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    if (!paramManager.getBoolean(ConnectionParams.ENABLE_ALTER_SHARD_KEY)
                        && !paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                            "Do not support change sharding key column type on drds mode database");
                    }
                    return false;
                }

                if (specificationSet.stream().anyMatch(ALTER_COLUMN_NAME_OR_TYPE::contains)) {
                    if (!paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_MODIFY_SK)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                            "Do not support change the column type of partition key");
                    }
                    needOmc = true;
                }
            }

            if (tableMeta.withGsi() && tableColumns.existsInGsi(columnName) && specificationSet.stream()
                .anyMatch(ALTER_COLUMN_NAME_OR_TYPE::contains)) {
                needOmc = true;
            }
        }

        // todo:  ALGORITHM=copy

        if (needOmc) {
            validateOnlineModify(ec, true);
        }

        return needOmc;
    }

    private void validateAlters(final TableMeta tableMeta,
                                final MetaUtils.TableColumns tableColumns,
                                AtomicReference<String> columnNameOut,
                                AtomicReference<SqlKind> alterTypeOut,
                                AtomicBoolean gsiExistsOut,
                                AtomicBoolean clusterExistsOut) {
        String columnName = null;
        SqlKind alterType = null;
        boolean gsiExists = false;
        boolean clusteredExists = false;

        for (SqlAlterSpecification alterItem : sqlAlterTable.getAlters()) {
            if (!(alterItem.isA(SqlKind.CHECK_ALTER_WITH_GSI))) {
                continue;
            }

            final PlannerContext context = (PlannerContext) this.getCluster().getPlanner().getContext();
            final ParamManager paramManager = context.getParamManager();

            alterType = alterItem.getKind();
            switch (alterType) {
            case ADD_COLUMN:
                SqlAddColumn addColumn = (SqlAddColumn) alterItem;
                if (tableMeta.withClustered()) {
                    if (sqlAlterTable.getColumnOpts().size() > 1 ||
                        !sqlAlterTable.getColumnOpts().containsKey(SqlAlterTable.ColumnOpt.ADD) ||
                        sqlAlterTable.getColumnOpts().get(SqlAlterTable.ColumnOpt.ADD).size() > 1 ||
                        sqlAlterTable.getAlters().size() > 1) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Do not support mix ADD COLUMN with other ALTER statements when table contains CLUSTERED INDEX");
                    }
                    // Check duplicated column name for clustered index, because this may generate a compound job.
                    final String colName = addColumn.getColName().getLastName();
                    if (tableMeta.getColumnIgnoreCase(colName) != null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                            "Duplicate column name '" + colName + "' on `" + tableName + "`");
                    }
                    // Check in GSI table. This should never happen.
                    if (tableColumns.existsInGsi(colName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                            "Duplicate column name '" + colName + "' on GSI of `" + tableName + "`");
                    }
                    clusteredExists = true;
                }
                break;
            case ALTER_COLUMN_DEFAULT_VAL:
                SqlAlterColumnDefaultVal alterDefaultVal = (SqlAlterColumnDefaultVal) alterItem;
                columnName = alterDefaultVal.getColumnName().getLastName();

                if (tableColumns.existsInGsi(columnName)) {
                    gsiExists = true;
                }
                break;
            case CHANGE_COLUMN:
                SqlChangeColumn changeColumn = (SqlChangeColumn) alterItem;
                columnName = changeColumn.getOldName().getLastName();

                if (tableColumns.existsInGsi(columnName)) {
                    gsiExists = true;

                    // Allow some special case of modify column.
                    final Set<AlterColumnSpecification> specificationSet =
                        getAlterColumnSpecification(tableMeta, changeColumn);
                    alterColumnSpecificationSets.add(specificationSet);

                    if (specificationSet.stream().anyMatch(ALTER_COLUMN_NAME_OR_TYPE::contains)) {
                        if (tableColumns.isShardingKey(columnName)
                            || tableColumns.isGsiShardingKey(columnName)) {
                            if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                                    if (!paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                            "Do not support change sharding key column type on drds mode database");
                                    }
                                }
                                if (!paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                        "Do not support change column name or type on sharding key on table with GSI");
                                }
                            } else if (tableColumns.isPrimaryKey(columnName)) {
                            if (!paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                    "Do not support change column name or type on primary key or sharding key on table with GSI");
                            }
                        } else if (tableColumns.existsInGsiUniqueKey(columnName, false)) {
                            if (!paramManager
                                .getBoolean(ConnectionParams.ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI) &&
                                !paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                    "Change column included in UGSI is extremely dangerous which may corrupt the unique constraint");
                            }
                        } else if (!paramManager.getBoolean(ConnectionParams.ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI)
                            && !paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {

                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Change column name or type included in GSI is not recommended");
                        }
                    } // Alter default, comment and order, so just let it go.

                    // Change alter warning.
                    if (!paramManager.getBoolean(ConnectionParams.ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI) &&
                        !paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                        if (1 == specificationSet.size() &&
                            specificationSet.stream().anyMatch(ALTER_COLUMN_DEFAULT::contains)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "It seems that you only alter column default, try ALTER COLUMN SET/DROP DEFAULT(partly rollback supported) instead for better practice");
                        }
                    }
                }
                break;
            case DROP_COLUMN:
                SqlDropColumn dropColumn = (SqlDropColumn) alterItem;
                columnName = dropColumn.getColName().getLastName();

                if (tableColumns.existsInGsi(columnName)) {
                    gsiExists = true;

                    // PK can never modified.
                    if (tableColumns.isPrimaryKey(columnName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Do not support drop column included in primary key of table which has global secondary index");
                    }

                    // Drop column in local unique key and also in GSI is allowed in PolarDB-X by hint.
                    // Note this is **DANGER** because this operation may partly success and can't recover or rollback.
                    if ((!paramManager.getBoolean(ConnectionParams.ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI)) &&
                        tableColumns.existsInLocalUniqueKey(columnName, false)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Do not support drop column included in unique key of table which has global secondary index");
                    }
                }

                // Sharding key can never modified.
                if (tableColumns.isGsiShardingKey(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support drop sharding key of global secondary index");
                }

                // Drop column in GSI unique key is allowed in PolarDB-X by hint.
                // Note this is **DANGER** because this operation may partly success and can't recover or rollback.
                if ((!paramManager.getBoolean(ConnectionParams.ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI)) &&
                    tableColumns.existsInGsiUniqueKey(columnName, false)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support drop column included in unique key of global secondary index");
                }
                break;
            case MODIFY_COLUMN:
                SqlModifyColumn modifyColumn = (SqlModifyColumn) alterItem;
                columnName = modifyColumn.getColName().getLastName();

                if (tableColumns.existsInGsi(columnName)) {
                    gsiExists = true;

                    // Allow some special case of modify column.
                    final Set<AlterColumnSpecification> specificationSet =
                        getAlterColumnSpecification(tableMeta, modifyColumn);
                    alterColumnSpecificationSets.add(specificationSet);

                    if (specificationSet.stream().anyMatch(ALTER_COLUMN_NAME_OR_TYPE::contains)) {
                        if (
                            tableColumns.isShardingKey(columnName) ||
                            tableColumns.isGsiShardingKey(columnName)){
                                if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                            if (!paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                    "Do not support modify sharding key column type on drds mode database");
                                    }
                                }
                                if (!paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                        "Do not support change column name or type on sharding key on table with GSI");
                                }
                            } else if (tableColumns.isPrimaryKey(columnName)) {
                                if (!paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                        "Do not support change column name or type on primary key on table with GSI");
                            }
                        } else if (tableColumns.existsInGsiUniqueKey(columnName, false)) {
                            if (!paramManager
                                .getBoolean(ConnectionParams.ALLOW_DROP_OR_MODIFY_PART_UNIQUE_WITH_GSI) &&
                                !paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                    "Change column included in UGSI is extremely dangerous which may corrupt the unique constraint");
                            }
                        } else if (!paramManager.getBoolean(ConnectionParams.ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI)
                            && !paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY) ) {

                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Change column name or type included in GSI is not recommended");
                        }
                    } // Alter default, comment and order, so just let it go.

                    // Modify alter warning.
                    if (!paramManager.getBoolean(ConnectionParams.ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI) &&
                        !paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                        if (1 == specificationSet.size() &&
                            specificationSet.stream().anyMatch(ALTER_COLUMN_DEFAULT::contains)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "It seems that you only alter column default, try ALTER COLUMN SET/DROP DEFAULT(partly rollback supported) instead for better practice");
                        }
                    }
                }
                break;
            case ADD_INDEX:
            case ADD_UNIQUE_INDEX:
            case ADD_FULL_TEXT_INDEX:
            case ADD_SPATIAL_INDEX:
            case ADD_FOREIGN_KEY:
                final SqlAddIndex addIndex = (SqlAddIndex) alterItem;
                if (null != addIndex.getIndexName() && tableMeta.withGsi(addIndex.getIndexName().getLastName())) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "Duplicated index name "
                        + addIndex.getIndexName()
                        .getLastName());
                }
                // Fall over.
            case DROP_INDEX:
                clusteredExists = true;
                break;
            case DROP_PRIMARY_KEY:
                if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Does not support drop primary key from table with global secondary index");
                }
                break;
            case CONVERT_TO_CHARACTER_SET:
                gsiExists = true;
                if (!paramManager.getBoolean(ConnectionParams.ALLOW_ALTER_GSI_INDIRECTLY)) {
                    // Check correctness. Because this can not rollback.
                    final SqlConvertToCharacterSet convert = (SqlConvertToCharacterSet) alterItem;
                    final CharsetName charsetName = CharsetName.of(convert.getCharset());
                    if (null == charsetName || !charsetName.name().equalsIgnoreCase(convert.getCharset())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Unknown charset name '" + convert.getCharset() + "'");
                    }
                    if (convert.getCollate() != null) {
                        final CollationName collationName = CollationName.of(convert.getCollate());
                        if (null == collationName || !collationName.name().equalsIgnoreCase(convert.getCollate())) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Unknown collate name '" + convert.getCollate() + "'");
                        }
                        if (!charsetName.match(collationName)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                                "Collate name '" + convert.getCollate() + "' not support for '" + convert
                                    .getCharset() + "'");
                        }
                    }
                }
                break;
            default:
                break;
            }
        }

        columnNameOut.set(columnName);
        gsiExistsOut.set(gsiExists);
        clusterExistsOut.set(clusteredExists);
        alterTypeOut.set(alterType);
    }

    private void prepareAlterGsiData() {
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        if (!tableMeta.withGsi()) {
            return;
        }

        MetaUtils.TableColumns tableColumns = MetaUtils.TableColumns.build(tableMeta);

        // pass as output parameters
        AtomicReference<String> columnNameOut = new AtomicReference<>();
        AtomicReference<SqlKind> alterTypeOut = new AtomicReference<>();
        AtomicBoolean gsiExistsOut = new AtomicBoolean(false);
        AtomicBoolean clusteredExistsOut = new AtomicBoolean();

        // Clear column specifications.
        alterColumnSpecificationSets.clear();

        // Validate alter
        validateAlters(tableMeta, tableColumns, columnNameOut, alterTypeOut, gsiExistsOut, clusteredExistsOut);

        String columnName = columnNameOut.get();
        SqlKind alterType = alterTypeOut.get();
        boolean gsiExists = gsiExistsOut.get();
        boolean clusteredExists = clusteredExistsOut.get();

        alterTableWithGsiPreparedData = new AlterTableWithGsiPreparedData();

        if ((gsiExists || clusteredExists) && sqlAlterTable.getAlters().size() > 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "Do not support multi ALTER statements on table with global secondary index");
        }

        if (gsiExists) {
            if (alterType.belongsTo(SqlKind.ALTER_ALTER_COLUMN) && TStringUtil.isNotBlank(columnName)) {
                // Alter gsi table column when alter primary table columns
                final Set<String> gsiNameByColumn = tableColumns.getGsiNameByColumn(columnName);

                final GsiTableMetaBean gsiTableMetaBean = tableMeta.getGsiTableMetaBean();
                for (Map.Entry<String, GsiIndexMetaBean> indexEntry : gsiTableMetaBean.indexMap.entrySet()) {
                    final String indexTableName = indexEntry.getKey();

                    if (!gsiNameByColumn.contains(indexTableName)) {
                        continue;
                    }

                    if (null != sqlAlterTable.getTableOptions() &&
                        GeneralUtil.isNotEmpty(sqlAlterTable.getTableOptions().getUnion())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Do not support set table option UNION to table with global secondary index");
                    }

                    alterTableWithGsiPreparedData
                        .addAlterGlobalIndexPreparedData(prepareAlterTableData(indexTableName));
                }
            } else if (alterType == SqlKind.CONVERT_TO_CHARACTER_SET) {
                // Alter charset of gsi if primary table's charset is changed
                final GsiTableMetaBean gsiTableMetaBean = tableMeta.getGsiTableMetaBean();
                for (Map.Entry<String, GsiIndexMetaBean> indexEntry : gsiTableMetaBean.indexMap.entrySet()) {
                    final String indexTableName = indexEntry.getKey();

                    if (null != sqlAlterTable.getTableOptions()
                        && GeneralUtil.isNotEmpty(sqlAlterTable.getTableOptions().getUnion())) {
                        throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                            "Do not support set table option UNION to table with global secondary index");
                    }

                    alterTableWithGsiPreparedData
                        .addAlterGlobalIndexPreparedData(prepareAlterTableData(indexTableName));
                }
            }
        } else if (null != sqlAlterTable.getTableOptions() && GeneralUtil.isEmpty(sqlAlterTable.getAlters())) {
            // Alter table options
            if (GeneralUtil.isEmpty(sqlAlterTable.getTableOptions().getUnion())) {
                final GsiTableMetaBean gsiTableMetaBean = tableMeta.getGsiTableMetaBean();
                for (Map.Entry<String, GsiIndexMetaBean> indexEntry : gsiTableMetaBean.indexMap.entrySet()) {
                    final String indexTableName = indexEntry.getKey();
                    alterTableWithGsiPreparedData
                        .addAlterGlobalIndexPreparedData(prepareAlterTableData(indexTableName));
                }
            }
        } else if (sqlAlterTable.getAlters().size() == 2
            && sqlAlterTable.getAlters().get(0) instanceof SqlDropPrimaryKey
            && sqlAlterTable.getAlters().get(1) instanceof SqlAddPrimaryKey
            && DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            final GsiTableMetaBean gsiTableMetaBean = tableMeta.getGsiTableMetaBean();
            for (Map.Entry<String, GsiIndexMetaBean> indexEntry : gsiTableMetaBean.indexMap.entrySet()) {
                final String indexTableName = indexEntry.getKey();
                alterTableWithGsiPreparedData
                    .addAlterGlobalIndexPreparedData(prepareAlterTableData(indexTableName));
            }
        } else if (sqlAlterTable.getAlters().size() == 1
            && sqlAlterTable.getAlters().get(0) instanceof SqlAddPrimaryKey
            && DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            final GsiTableMetaBean gsiTableMetaBean = tableMeta.getGsiTableMetaBean();
            for (Map.Entry<String, GsiIndexMetaBean> indexEntry : gsiTableMetaBean.indexMap.entrySet()) {
                final String indexTableName = indexEntry.getKey();
                alterTableWithGsiPreparedData
                    .addAlterGlobalIndexPreparedData(prepareAlterTableData(indexTableName));
            }
        } else if (clusteredExists) {
            // NOTE: All modifications on clustered-index are processes at `alterClusterIndexData1
            if (null != sqlAlterTable.getTableOptions()
                && GeneralUtil.isNotEmpty(sqlAlterTable.getTableOptions().getUnion())) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Do not support set table option UNION to table with clustered index");
            }
        }
    }

    public void validateColumnar() {
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        MetaUtils.TableColumns tableColumns = MetaUtils.TableColumns.build(tableMeta);

        if (!tableMeta.withCci()) {
            return;
        }

        // forbid multiple statements for now
//        if (getSqlAlterTable().getAlters().size() > 1) {
//            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
//                "Do not support multiple ALTER TABLE statements on table with clustered columnar index");
//        }

        for (SqlAlterSpecification alterItem : getSqlAlterTable().getAlters()) {
            SqlKind alterType = alterItem.getKind();
            switch (alterType) {
            case DROP_COLUMN:
                SqlDropColumn dropColumn = (SqlDropColumn) alterItem;
                String columnName = dropColumn.getColName().getLastName();
                // columnar primary key column can never modified.
                if (tableColumns.isColumnarPrimaryKey(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support drop primary key of clustered columnar index");
                }

                // columnar sort key column can never modified.
                if (tableColumns.isColumnarIndexColumn(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support drop sort key of clustered columnar index");
                }

                // columnar partition key can never modified.
                if (tableColumns.isColumnarShardingKey(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support drop partition key of clustered columnar index");
                }
                break;
            case ALTER_COLUMN_DEFAULT_VAL:
                SqlAlterColumnDefaultVal alterDefaultVal = (SqlAlterColumnDefaultVal) alterItem;
                columnName = alterDefaultVal.getColumnName().getLastName();
                // columnar primary key column can never modified.
                if (tableColumns.isColumnarPrimaryKey(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support drop or set default value of primary key in clustered columnar index");
                }
                // columnar index column default value can never modified.
                if (tableColumns.isColumnarIndexColumn(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support drop or set default value of sort key in clustered columnar index");
                }
                // columnar sharding key default value can never modified.
                if (tableColumns.isColumnarShardingKey(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support drop or set default value of partition key in clustered columnar index");
                }
                break;
            case MODIFY_COLUMN:
                SqlModifyColumn modifyColumn = (SqlModifyColumn) alterItem;
                columnName = modifyColumn.getColName().getLastName();

                SqlValidatorImpl.validateUnsupportedTypeWithCciWhenModifyColumn(modifyColumn.getColDef());

                // columnar primary key column can never modified.
                if (tableColumns.isColumnarPrimaryKey(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support modify primary key of clustered columnar index");
                }

                // columnar sort key column can never modified.
                if (tableColumns.isColumnarIndexColumn(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support modify sort key of clustered columnar index");
                }
                // columnar sharding key can never modified.
                if (tableColumns.isColumnarShardingKey(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support modify partition key of clustered columnar index");
                }
                break;
            case CHANGE_COLUMN:
                SqlChangeColumn changeColumn = (SqlChangeColumn) alterItem;
                columnName = changeColumn.getOldName().getLastName();

                SqlValidatorImpl.validateUnsupportedTypeWithCciWhenModifyColumn(changeColumn.getColDef());

                // columnar primary key column can never modified.
                if (tableColumns.isColumnarPrimaryKey(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support change primary key of clustered columnar index");
                }
                // columnar sort key column can never modified.
                if (tableColumns.isColumnarIndexColumn(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support change sort key of clustered columnar index");
                }
                // columnar partition key can never modified.
                if (tableColumns.isColumnarShardingKey(columnName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support change partition key of clustered columnar index");
                }
                break;
            case DROP_PRIMARY_KEY:
                if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Does not support drop primary key from table with global secondary index");
                }
                // columnar primary key column can never modified.
                if (tableMeta.withCci()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                        "Do not support drop primary key of clustered columnar index");
                }
                break;
            }
        }
    }

    public void prepareOnlineModifyColumn() {
        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        final GsiTableMetaBean gsiTableMetaBean = tableMeta.getGsiTableMetaBean();

        MetaUtils.TableColumns tableColumns = MetaUtils.TableColumns.build(tableMeta);
        SqlKind alterType = null;
        String colName = null;
        Set<String> rebuildIndexes = new TreeSet<>(String::compareToIgnoreCase);

        for (SqlAlterSpecification alterItem : sqlAlterTable.getAlters()) {
            alterType = alterItem.getKind();
            switch (alterType) {
            case CHANGE_COLUMN:
                SqlChangeColumn changeColumn = (SqlChangeColumn) alterItem;
                colName = changeColumn.getOldName().getLastName();
                break;
            case MODIFY_COLUMN:
                SqlModifyColumn modifyColumn = (SqlModifyColumn) alterItem;
                colName = modifyColumn.getColName().getLastName();
                break;
            case DROP_COLUMN:
                SqlDropColumn sqlDropColumn = (SqlDropColumn) alterItem;
                colName = sqlDropColumn.getColName().getLastName();
            }

            if (TStringUtil.isNotBlank(colName)) {
                final Set<String> gsiNameByColumn = tableColumns.getGsiNameByColumn(colName);

                if (gsiNameByColumn == null || gsiNameByColumn.isEmpty()) {
                    continue;
                }

                for (Map.Entry<String, GsiIndexMetaBean> indexEntry : gsiTableMetaBean.indexMap.entrySet()) {
                    final String indexTableName = indexEntry.getKey();

                    if (!gsiNameByColumn.contains(indexTableName)) {
                        continue;
                    }

                    rebuildIndexes.add(indexTableName);
                }
            }
        }

        alterTableWithGsiPreparedData = new AlterTableWithGsiPreparedData();

        for (String indexName : rebuildIndexes) {
            alterTableWithGsiPreparedData
                .addAlterGlobalIndexPreparedData(prepareAlterTableDataWithRebuild(indexName));
        }

        alterTablePreparedData = prepareAlterTableDataWithRebuild(tableName);
    }

    private Set<AlterColumnSpecification> getAlterColumnSpecification(TableMeta tableMeta,
                                                                      SqlAlterSpecification specification) {
        final Set<AlterColumnSpecification> specificationSet = new HashSet<>();

        switch (specification.getKind()) {
        case CHANGE_COLUMN: {
            final SqlChangeColumn changeColumn = (SqlChangeColumn) specification;

            // Check name.
            final String oldName = changeColumn.getOldName().getLastName();
            if (!changeColumn.getNewName().getLastName().equalsIgnoreCase(oldName)) {
                specificationSet.add(AlterColumnSpecification.AlterColumnName);
            }

            // Check definition.
            final ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(oldName);
            if (null == columnMeta) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Modify unknown column '" + oldName + "'");
            }
            updateAlterColumnSpecification(columnMeta, changeColumn.getColDef(), specificationSet);

            // Check reorder.
            if (changeColumn.isFirst()) {
                // Check whether first column.
                if (!tableMeta.getPhysicalColumns().get(0).getName().equalsIgnoreCase(oldName)) {
                    specificationSet.add(AlterColumnSpecification.AlterColumnOrder);
                }
            } else if (changeColumn.getAfterColumn() != null) {
                final String afterColName = changeColumn.getAfterColumn().getLastName();
                for (int colIdx = 0; colIdx < tableMeta.getPhysicalColumns().size(); ++colIdx) {
                    final ColumnMeta probCol = tableMeta.getPhysicalColumns().get(colIdx);
                    if (probCol.getName().equalsIgnoreCase(afterColName)) {
                        // Find the before col.
                        if (colIdx >= tableMeta.getPhysicalColumns().size() - 1 || !tableMeta.getPhysicalColumns()
                            .get(colIdx + 1).getName().equalsIgnoreCase(oldName)) {
                            specificationSet.add(AlterColumnSpecification.AlterColumnOrder);
                        }
                        break;
                    }
                }
            } // Or change in place.
        }
        break;

        case MODIFY_COLUMN: {
            final SqlModifyColumn modifyColumn = (SqlModifyColumn) specification;

            // Modify doesn't change the name.
            // Now check definition.
            final String colName = modifyColumn.getColName().getLastName();
            final ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(colName);
            if (null == columnMeta) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                    "Modify unknown column '" + colName + "'");
            }
            updateAlterColumnSpecification(columnMeta, modifyColumn.getColDef(), specificationSet);

            // Check reorder.
            if (modifyColumn.isFirst()) {
                // Check whether first column.
                if (!tableMeta.getPhysicalColumns().get(0).getName().equalsIgnoreCase(colName)) {
                    specificationSet.add(AlterColumnSpecification.AlterColumnOrder);
                }
            } else if (modifyColumn.getAfterColumn() != null) {
                final String afterColName = modifyColumn.getAfterColumn().getLastName();
                for (int colIdx = 0; colIdx < tableMeta.getPhysicalColumns().size(); ++colIdx) {
                    final ColumnMeta probCol = tableMeta.getPhysicalColumns().get(colIdx);
                    if (probCol.getName().equalsIgnoreCase(afterColName)) {
                        // Find the before col.
                        if (colIdx >= tableMeta.getPhysicalColumns().size() - 1 || !tableMeta.getPhysicalColumns()
                            .get(colIdx + 1).getName().equalsIgnoreCase(colName)) {
                            specificationSet.add(AlterColumnSpecification.AlterColumnOrder);
                        }
                        break;
                    }
                }
            } // Or modify in place.
        }
        break;

        default:
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "Unknown alter specification");
        }
        return specificationSet;
    }

    private void updateAlterColumnSpecification(ColumnMeta columnMeta,
                                                SqlColumnDeclaration columnDeclaration,
                                                Set<AlterColumnSpecification> specificationSet) {
        // Check basic type.
        final RelDataType targetDataType = columnDeclaration.getDataType().deriveType(getCluster().getTypeFactory());
        if (!SqlTypeUtil.equalSansNullability(getCluster().getTypeFactory(), targetDataType,
            columnMeta.getField().getRelType())) {
            specificationSet.add(AlterColumnSpecification.AlterColumnType);
        }

        // Check nullable.
        boolean targetNullable = null == columnDeclaration.getNotNull() ||
            SqlColumnDeclaration.ColumnNull.NULL == columnDeclaration.getNotNull();
        if (null == columnDeclaration.getNotNull() && columnMeta.getField().isPrimary()) {
            targetNullable = false;
        }
        if (columnMeta.getField().getRelType().isNullable() != targetNullable) {
            specificationSet.add(AlterColumnSpecification.AlterColumnType);
        }

        // Check default value.
        final String originalDefault = null == columnMeta.getField().getDefault() ?
            (columnMeta.getField().getRelType().isNullable() ? "NULL" : null) : columnMeta.getField().getDefault();
        final String targetDefault;
        if (columnDeclaration.getDefaultExpr() != null) {
            targetDefault = columnDeclaration.getDefaultExpr().getOperator().getName();
        } else if (columnDeclaration.getDefaultVal() != null) {
            targetDefault = columnDeclaration.getDefaultVal().toValue();
        } else if (targetNullable) {
            targetDefault = "NULL"; // Default null.
        } else {
            targetDefault = null;
        }
        if ((null == originalDefault && targetDefault != null) || (originalDefault != null && null == targetDefault) ||
            (originalDefault != null && targetDefault != null && !originalDefault.equals(targetDefault))) {
            specificationSet.add(AlterColumnSpecification.AlterColumnDefault);
        }

        // Check comment.
        if (columnDeclaration.getComment() != null) {
            specificationSet.add(AlterColumnSpecification.AlterColumnComment);
        }
    }

    /**
     * 仅判断改变的类型是否影响路由，没有判断 default 值，以及 nullable
     */
    private boolean willModifyPartitionRoute(TableMeta tableMeta, MetaUtils.TableColumns tableColumns,
                                             String column, String newColName,
                                             SqlColumnDeclaration columnDeclaration) {
        final ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(column);
        if (null == columnMeta) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "Modify unknown column '" + column + "'");
        }

        if (!StringUtils.equalsIgnoreCase(column, newColName)) {
            return true;
        }

        if (!tableColumns.isActualShardingKey(column) && !tableColumns.isGsiActualShardingKey(column)) {
            return false;
        }

        final RelDataType targetDataType = columnDeclaration.getDataType().deriveType(getCluster().getTypeFactory());
        final RelDataType sourceDataType = columnMeta.getField().getRelType();

        return !SqlTypeUtil.canDirectModifyColumn(getCluster().getTypeFactory(), sourceDataType, targetDataType);
    }

    /**
     * Rewrite a local index to global index, if the table is auto-partitioned
     */
    public boolean needRewriteToGsi(boolean rewrite) {
        final String logicalTableName = sqlAlterTable.getOriginTableName().getLastName();
        final TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);

        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName) && tableMeta.isAutoPartition()) {
            // Legacy code. (auto partition on sharding table do rewrite here)
            for (int idx = 0; idx < sqlAlterTable.getAlters().size(); ++idx) {
                final SqlAlterSpecification specification = sqlAlterTable.getAlters().get(idx);
                if (specification instanceof SqlAddIndex) {
                    final SqlAddIndex addIndex = (SqlAddIndex) specification;
                    if (!addIndex.getIndexDef().isClustered() &&
                        !addIndex.getIndexDef().isGlobal() &&
                        !addIndex.getIndexDef().isLocal()) {
                        // Need rewrite.
                        if (rewrite) {
                            SqlAddIndex newIndex = new SqlAddIndex(
                                addIndex.getParserPosition(),
                                addIndex.getIndexName(),
                                addIndex.getIndexDef().rebuildToGsi(null, null)
                            );
                            sqlAlterTable.getAlters().set(idx, newIndex);
                        }
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public void setDdlVersionId(Long ddlVersionId) {
        if (null != getAlterTablePreparedData()) {
            getAlterTablePreparedData().setDdlVersionId(ddlVersionId);
        }
        if (null != getCreateGlobalIndexesPreparedData()) {
            getCreateGlobalIndexesPreparedData().forEach(p -> p.setDdlVersionId(ddlVersionId));
        }
        if (null != getAlterTableWithGsiPreparedData()) {
            getAlterTableWithGsiPreparedData().setDdlVersionId(ddlVersionId);
        }
        if (null != getRepartitionPrepareData()) {
            getRepartitionPrepareData().setDdlVersionId(ddlVersionId);
        }
        if (null != getAlterTableWithFileStorePreparedData()) {
            getAlterTableWithFileStorePreparedData().setDdlVersionId(ddlVersionId);
        }
    }

    private Boolean checkIfSupportMultipleStatementInAlterTableStmt(String schemaName, String tableName,
                                                                    String primaryName
    ) {
        // There are several case that we don't support
        // PRIMARY KEY
        // GENERATED KEY
        // FOREIGN KEY
        // PARTITION KEY
        // COLUMN EXPR
        // BINARY STRING
        // OMC
        SqlIdentifier algorithm =
            sqlAlterTable.getTableOptions() == null ? null : sqlAlterTable.getTableOptions().getAlgorithm();
        Boolean isOMC =
            (algorithm != null && algorithm.getSimple().equalsIgnoreCase(Attribute.ALTER_TABLE_ALGORITHM_OMC_INDEX));
        Boolean containsAlterPartitionKey = false;
        Boolean containsAlterForeignKey = false;
        Boolean containsAlterPrimaryKey = false;
        Boolean containsAlterGeneratedColumn = false;
        Boolean containsColumnExpr = false;
        Boolean containsBinaryStringLiteral = false;
        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        TableMeta tableMeta = sm.getTable(primaryName);
        Map<String, Set<String>> allReferencedColumns =
            GeneratedColumnUtil.getAllLogicalReferencedColumnsByGen(tableMeta);
        Set<String> referencedColumns =
            allReferencedColumns.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
        Set<String> generatedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        generatedColumns.addAll(tableMeta.getLogicalGeneratedColumnNames());
        generatedColumns.addAll(tableMeta.getGeneratedColumnNames());
        List<String> alterColumns = new ArrayList<>();
        List<SqlColumnDeclaration> alterColumnDefs = new ArrayList<>();
        MetaUtils.TableColumns tableColumns = MetaUtils.TableColumns.build(tableMeta);
        for (SqlAlterSpecification alterItem : GeneralUtil.emptyIfNull(sqlAlterTable.getAlters())) {
            if (alterItem instanceof SqlChangeColumn) {
                SqlChangeColumn changeColumn = (SqlChangeColumn) alterItem;
                String oldColumnName = changeColumn.getOldName().getLastName();
                alterColumns.add(oldColumnName);
                alterColumnDefs.add(changeColumn.getColDef());
            } else if (alterItem instanceof SqlModifyColumn) {
                SqlModifyColumn sqlModifyColumn = (SqlModifyColumn) alterItem;
                alterColumns.add(sqlModifyColumn.getColName().getLastName());
                alterColumnDefs.add(sqlModifyColumn.getColDef());
            } else if (alterItem instanceof SqlAddForeignKey || alterItem instanceof SqlDropForeignKey) {
                containsAlterForeignKey = true;
            } else if (alterItem instanceof SqlAddPrimaryKey || alterItem instanceof SqlDropPrimaryKey) {
                containsAlterPrimaryKey = true;
            } else if (alterItem instanceof SqlAddColumn) {
                SqlAddColumn sqlAddColumn = (SqlAddColumn) alterItem;
                alterColumnDefs.add(sqlAddColumn.getColDef());
            } else if (alterItem instanceof SqlDropColumn) {
                SqlDropColumn sqlDropColumn = (SqlDropColumn) alterItem;
                alterColumns.add(sqlDropColumn.getColName().getLastName());
            } else if (alterItem instanceof SqlConvertToCharacterSet) {
                if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                    if (CollectionUtils.isNotEmpty(tableColumns.shardingKeys)) {
                        containsAlterForeignKey =
                            containsAlterPartitionKey || tableColumns.shardingKeys.stream().anyMatch(
                                e -> tableMeta.getColumnIgnoreCase(e).getDataType().getSqlType() == Types.VARCHAR
                                    || tableMeta.getColumnIgnoreCase(e).getDataType().getSqlType() == Types.CHAR);
                    }
                    if (CollectionUtils.isNotEmpty(tableColumns.gsiShardingKeys.values())) {
                        for (Set<String> cset : tableColumns.gsiShardingKeys.values()) {
                            containsAlterPartitionKey = containsAlterPartitionKey || cset.stream().anyMatch(
                                e -> tableMeta.getColumnIgnoreCase(e).getDataType().getSqlType() == Types.VARCHAR
                                    || tableMeta.getColumnIgnoreCase(e).getDataType().getSqlType() == Types.CHAR);
                        }
                    }
                }
            }
        }
        for (String alterColumn : alterColumns) {
            if (referencedColumns.contains(alterColumn)) {
                containsAlterGeneratedColumn = true;
            }
            if (tableColumns.isShardingKey(alterColumn) || tableColumns.isGsiShardingKey(alterColumn)) {
                containsAlterPartitionKey = true;
            }
        }
        for (SqlColumnDeclaration columnDef : alterColumnDefs) {
            if (columnDef == null) {
                continue;
            }
            if (columnDef.getDefaultVal() instanceof SqlBinaryStringLiteral) {
                containsBinaryStringLiteral = true;
            }
            if (columnDef.getDefaultExpr() != null && InstanceVersion.isMYSQL80()) {
                containsColumnExpr = true;
            }
            if (columnDef.isGeneratedAlways() || columnDef.isGeneratedAlwaysLogical()) {
                containsAlterGeneratedColumn = true;
            }
            if (columnDef.getSpecialIndex() != null) {
                String specialKeyName = columnDef.getSpecialIndex().name();
                if (TStringUtil.equalsIgnoreCase("PRIMARY", specialKeyName)) {
                    containsAlterPrimaryKey = true;
                }
            }
        }
        return !(isOMC || containsAlterPartitionKey || containsAlterPrimaryKey || containsAlterForeignKey
            || containsAlterGeneratedColumn || containsBinaryStringLiteral || containsColumnExpr);
    }

    // For alter table column with GSI.
    public enum AlterColumnSpecification {
        AlterColumnName,
        AlterColumnType,
        AlterColumnDefault, // Should start auto fill.
        AlterColumnComment, // May set to null and this flag is not set. Just push down this alter.
        AlterColumnOrder // Should alter order in metaDB first.
    }

    public boolean needRefreshArcTblView(ExecutionContext ec) {
        boolean needFreshView = TtlUtil.checkIfNeedRefreshViewForArcTbl(this.alterTablePreparedData, ec);
        return needFreshView;
    }
}
