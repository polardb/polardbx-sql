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

import com.alibaba.polardbx.common.ColumnarTableOptions;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.ImplicitTableGroupUtil;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.gms.metadb.seq.SequencesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.view.InformationSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.MysqlSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.lang.Long.max;

/**
 * @author mengshi
 */
public class LogicalShowCreateTablesForPartitionDatabaseHandler extends HandlerCommon {
    private static final Logger logger =
        LoggerFactory.getLogger(LogicalShowCreateTablesForPartitionDatabaseHandler.class);

    public LogicalShowCreateTablesForPartitionDatabaseHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowCreateTable showCreateTable = (SqlShowCreateTable) show.getNativeSqlNode();
        final String tableName = RelUtils.lastStringValue(showCreateTable.getTableName());
        String schemaName = show.getSchemaName();
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }

        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager()
            .getTableWithNull(tableName);

        if (tableMeta == null) {
            // can not find tableMeta, so try view
            ArrayResultCursor showCreateView = showCreateView(schemaName, tableName, showCreateTable);

            if (showCreateView == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST, tableName, schemaName);
            }
            return showCreateView;
        }

        ArrayResultCursor result = new ArrayResultCursor("Create Table");
        result.addColumn("Table", DataTypes.StringType, false);
        result.addColumn("Create Table", DataTypes.StringType, false);
        result.initMeta();

        final boolean outputMySQLIndent =
            executionContext.getParamManager().getBoolean(ConnectionParams.OUTPUT_MYSQL_INDENT);

        String sql;
        if (executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_LOGICAL_TABLE_META) ||
            ConfigDataMode.isColumnarMode()) {
            MySqlCreateTableStatement tableStatement = LogicalShowCreateTableHandler.fetchShowCreateTableFromMetaDb(
                schemaName, tableName, executionContext);
            sql = tableStatement.toString();
        } else {
            sql = fetchShowCreateTableFromPhy(schemaName, tableName, showCreateTable, show, executionContext);
            sql = LogicalShowCreateTableHandler.reorgLogicalColumnOrder(schemaName, tableName, sql);
        }

        StringBuilder partitionStr = new StringBuilder();
        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        PartitionInfoManager partitionInfoManager = tddlRuleManager.getPartitionInfoManager();
        PartitionInfo partInfo = partitionInfoManager.getPartitionInfo(tableName);

        Boolean showHashPartitionByRange = Boolean.valueOf(ConnectionParams.SHOW_HASH_PARTITIONS_BY_RANGE.getDefault());
        if (executionContext != null) {
            showHashPartitionByRange =
                executionContext.getParamManager().getBoolean(ConnectionParams.SHOW_HASH_PARTITIONS_BY_RANGE);
        }
        boolean needShowHashByRange = showHashPartitionByRange || show.isShowForTruncateTable();
        String partitionByStr = partInfo.showCreateTablePartitionDefInfo(needShowHashByRange);
        partitionStr.append("\n").append(partitionByStr);

        final GsiMetaBean gsiMeta = ExecutorContext.getContext(schemaName)
            .getGsiManager()
            .getGsiTableAndIndexMeta(schemaName, tableName, IndexStatus.ALL);

        boolean containImplicitColumn = false;
        boolean containAutoIncrement = false;
        // handle implicit pk
        MySqlCreateTableStatement createTable =
            (MySqlCreateTableStatement) SQLUtils.parseStatementsWithDefaultFeatures(sql, JdbcConstants.MYSQL).get(0)
                .clone();

        createTable.setTableName(SqlIdentifier.surroundWithBacktick(tableName));

        List<SQLTableElement> toRemove = Lists.newArrayList();
        List<SQLTableElement> toAdd = Lists.newArrayList();
        for (SQLTableElement sqlTableElement : createTable.getTableElementList()) {
            if (sqlTableElement instanceof SQLColumnDefinition) {
                SQLColumnDefinition sqlColumnDefinition = (SQLColumnDefinition) sqlTableElement;
                String columnName = SQLUtils.normalizeNoTrim(sqlColumnDefinition.getColumnName());
                ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(columnName);
                if (sqlColumnDefinition.isAutoIncrement()) {
                    containAutoIncrement = true;
                }

                if (columnMeta != null && columnMeta.isBinaryDefault()) {
                    // handle binary default value
                    SQLHexExpr newDefaultVal = new SQLHexExpr(columnMeta.getField().getDefault());
                    sqlColumnDefinition.setDefaultExpr(newDefaultVal);
                } else if (columnMeta != null && columnMeta.isLogicalGeneratedColumn()) {
                    // handle generated column
                    sqlColumnDefinition.setGeneratedAlawsAs(
                        new MySqlExprParser(ByteString.from(columnMeta.getField().getDefault())).expr());
                    sqlColumnDefinition.setLogical(true);
                    sqlColumnDefinition.setDefaultExpr(null);
                }
            }

            if (sqlTableElement instanceof SQLColumnDefinition
                && SqlValidatorImpl.isImplicitKey(((SQLColumnDefinition) sqlTableElement).getNameAsString())) {
                containImplicitColumn = true;
            }

            if (sqlTableElement instanceof SQLColumnDefinition
                && SqlValidatorImpl.isImplicitKey(((SQLColumnDefinition) sqlTableElement).getNameAsString())
                && !needShowImplicitId(executionContext) && !showCreateTable.isFull()) {
                toRemove.add(sqlTableElement);
            }

            if (sqlTableElement instanceof SQLColumnDefinition
                && TableColumnUtils.isHiddenColumn(executionContext, schemaName, tableName,
                SQLUtils.normalizeNoTrim(((SQLColumnDefinition) sqlTableElement).getNameAsString()))
                && !needShowImplicitId(executionContext) && !showCreateTable.isFull()) {
                toRemove.add(sqlTableElement);
            }

            if (sqlTableElement instanceof MySqlPrimaryKey
                && SqlValidatorImpl.isImplicitKey(((MySqlPrimaryKey) sqlTableElement).getColumns().get(0).toString())
                && !needShowImplicitId(executionContext) && !showCreateTable.isFull()) {
                toRemove.add(sqlTableElement);
            }

            // Remove index key and add foreign key if it is logical FK.
            if (sqlTableElement instanceof MySqlKey && ((MySqlKey) sqlTableElement).getName() != null &&
                ((MySqlKey) sqlTableElement).getName().getSimpleName() != null) {
                final ForeignKeyData foreignKeyData =
                    tableMeta.getForeignKeys()
                        .get(SQLUtils.normalizeNoTrim(((MySqlKey) sqlTableElement).getName().getSimpleName()));
                if (foreignKeyData != null && !foreignKeyData.isPushDown()) {
                    toRemove.add(sqlTableElement);
                    final MysqlForeignKey mysqlForeignKey = new MysqlForeignKey();
                    mysqlForeignKey.setName(
                        new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(foreignKeyData.constraint)));
                    mysqlForeignKey.setHasConstraint(true);
                    // do not show fk index name
//                    mysqlForeignKey.setIndexName(
//                        new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(foreignKeyData.indexName)));
                    mysqlForeignKey.setReferencedTable(
                        new SQLExprTableSource(foreignKeyData.refSchema.equals(schemaName) ?
                            SqlIdentifier.surroundWithBacktick(foreignKeyData.refTableName) :
                            SqlIdentifier.surroundWithBacktick(foreignKeyData.refSchema) + "."
                                + SqlIdentifier.surroundWithBacktick(foreignKeyData.refTableName)));
                    mysqlForeignKey.getReferencingColumns().addAll(foreignKeyData.columns.stream()
                        .map(col -> new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(col)))
                        .collect(Collectors.toList()));
                    mysqlForeignKey.getReferencedColumns().addAll(foreignKeyData.refColumns.stream()
                        .map(col -> new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(col)))
                        .collect(Collectors.toList()));
                    if (foreignKeyData.onDelete != null && !foreignKeyData.onDelete.equals(
                        ForeignKeyData.ReferenceOptionType.NO_ACTION)) {
                        mysqlForeignKey.setOnDelete(
                            MysqlForeignKey.Option.fromString(foreignKeyData.onDelete.getText()));
                    }
                    if (foreignKeyData.onUpdate != null && !foreignKeyData.onUpdate.equals(
                        ForeignKeyData.ReferenceOptionType.NO_ACTION)) {
                        mysqlForeignKey.setOnUpdate(
                            MysqlForeignKey.Option.fromString(foreignKeyData.onUpdate.getText()));
                    }
                    if (showCreateTable.isFull()) {
                        mysqlForeignKey.setPushDown(MysqlForeignKey.PushDown.fromBoolean(foreignKeyData.isPushDown()));
                    }
                    toAdd.add(mysqlForeignKey);
                }
            }

            // Remove duplicate foreign key if it is identical with logical one.
            if (sqlTableElement instanceof MysqlForeignKey) {
                // Remove the physical table suffix anyway.
                final ForeignKeyData foreignKeyData =
                    tableMeta.getForeignKeys()
                        .get(SQLUtils.normalizeNoTrim(((MysqlForeignKey) sqlTableElement).getName().getSimpleName()));
                ((MysqlForeignKey) sqlTableElement).setReferencedTableName(new SQLIdentifierExpr(
                    SqlIdentifier.surroundWithBacktick(foreignKeyData.refTableName)));
                ((MysqlForeignKey) sqlTableElement).setName(new SQLIdentifierExpr(
                    SqlIdentifier.surroundWithBacktick(foreignKeyData.constraint)));
                // Enable constraint name by default.
                if (((MysqlForeignKey) sqlTableElement).getName() != null) {
                    ((MysqlForeignKey) sqlTableElement).setHasConstraint(true);
                }
                if (showCreateTable.isFull()) {
                    ((MysqlForeignKey) sqlTableElement).setPushDown(
                        MysqlForeignKey.PushDown.fromBoolean(foreignKeyData.isPushDown()));
                }
            }
        }
        createTable.getTableElementList().removeAll(toRemove);
        createTable.getTableElementList().addAll(toAdd);
        List<SQLTableElement> localIndexes = Lists.newArrayList();

        for (SQLTableElement element : createTable.getTableElementList()) {
            if ((element instanceof MySqlKey || element instanceof MySqlTableIndex)
                && !(element instanceof MySqlPrimaryKey)) {
                localIndexes.add(element);
            }
        }
        createTable.getTableElementList().removeAll(localIndexes);
        if (createTable.getOptionHints() != null) {
            createTable.getOptionHints().removeIf(
                e -> StringUtils.contains(e.getText(), "PARTITION BY")
            );
        }

        List<SQLTableElement> indexDefs =
            buildIndexDefs(schemaName, gsiMeta, tableName, tableMeta, localIndexes, showCreateTable.isFull(),
                needShowHashByRange);
        createTable.getTableElementList().addAll(indexDefs);

        if (tableMeta.isAutoPartition() && showCreateTable.isFull()) {
            createTable.setPrefixPartition(true);
        }

        // fix table options
        Engine engine = tableMeta.getEngine();
        SQLAssignItem autoIncrementOption = null;
        for (SQLAssignItem tableOption : createTable.getTableOptions()) {
            if (tableOption.getTarget().toString().equalsIgnoreCase("ENGINE")) {
                if (tableOption.getValue() == null || !tableOption.getValue().toString()
                    .equalsIgnoreCase(engine.name())) {
                    tableOption.setValue(new SQLCharExpr(engine.name()));
                }
            }

            if (tableOption.getTarget().toString().equalsIgnoreCase("AUTO_INCREMENT")) {
                autoIncrementOption = tableOption;
                continue;
            }
        }

        /**
         * 单独处理autoIncrement
         * 1. 值取自sequence manager, 而非物理表
         * 2. 如果存在`_drds_implicit_id_`列, 则不显示autoIncrement值
         * 3. 对于group sequence, simple sequence、new sequence: 如果该表的sequence还未使用过，则show create table时不显示autoIncrement值(与mysql保持一致)
         * */
        if (!containImplicitColumn && containAutoIncrement) {
            String sequenceName = SequenceAttribute.AUTO_SEQ_PREFIX + tableName;

            SequencesAccessor sequencesAccessor = new SequencesAccessor();
            boolean hide = false;
            List<SequencesRecord> sequence = null;
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                sequencesAccessor.setConnection(metaDbConn);
                String whereClause = String.format(" where name = \"%s\"", sequenceName);
                sequence = sequencesAccessor.show(schemaName, whereClause);
            } catch (SQLException e) {
                logger.error("failed to query sequence info", e);
                hide = true;
            }

            Long valueInMeta = null;
            if (sequence != null && sequence.size() == 1) {
                SequencesRecord sequencesRecord = sequence.get(0);
                SequenceAttribute.Type type = SequenceAttribute.Type.fromString(sequencesRecord.type);
                if (type == SequenceAttribute.Type.NEW || type == SequenceAttribute.Type.SIMPLE) {
                    Long startWith = null;
                    boolean parseSucceed = true;
                    try {
                        valueInMeta = Long.parseLong(sequencesRecord.value);
                        startWith = Long.parseLong(sequencesRecord.startWith);
                    } catch (Exception ignore) {
                        hide = true;
                        parseSucceed = false;
                    }

                    if (parseSucceed && valueInMeta.equals(startWith)) {
                        hide = true;
                    }
                } else if (type == SequenceAttribute.Type.GROUP) {
                    Long unitCount = null, unitIndex = null, innerStep = null;
                    boolean parseSucceed = true;
                    try {
                        valueInMeta = Long.parseLong(sequencesRecord.value);
                        unitCount = Long.parseLong(sequencesRecord.unitCount);
                        unitIndex = Long.parseLong(sequencesRecord.unitIndex);
                        innerStep = Long.parseLong(sequencesRecord.innerStep);
                    } catch (Exception ignore) {
                        hide = true;
                        parseSucceed = false;
                    }
                    //value为初始值，代表sequence尚未被用, 可以隐藏
                    if (parseSucceed) {
                        Long initBound = (unitIndex + unitCount) * innerStep;
                        if (valueInMeta < initBound) {
                            hide = true;
                        }
                    }
                }
            } else {
                hide = true;
            }

            if (!hide) {
                Long seqVal = SequenceManagerProxy.getInstance()
                    .currValue(schemaName, sequenceName) + 1L;
                if (valueInMeta != null) {
                    seqVal = max(seqVal, valueInMeta);
                }
                if (autoIncrementOption != null) {
                    autoIncrementOption.setValue(new SQLIntegerExpr(seqVal));
                } else {
                    autoIncrementOption = new SQLAssignItem();
                    autoIncrementOption.setTarget(new SQLIdentifierExpr("AUTO_INCREMENT"));
                    autoIncrementOption.setValue(new SQLIntegerExpr(seqVal));
                    createTable.getTableOptions().add(autoIncrementOption);
                }
            } else {
                Iterator<SQLAssignItem> iterator = createTable.getTableOptions().iterator();
                while (iterator.hasNext()) {
                    SQLAssignItem option = iterator.next();
                    if (option.getTarget().toString().equalsIgnoreCase("AUTO_INCREMENT")) {
                        iterator.remove();
                    }
                }
            }
        } else {
            Iterator<SQLAssignItem> iterator = createTable.getTableOptions().iterator();
            while (iterator.hasNext()) {
                SQLAssignItem option = iterator.next();
                if (option.getTarget().toString().equalsIgnoreCase("AUTO_INCREMENT")) {
                    iterator.remove();
                }
            }
        }

        //handle lbac attr
        if (showCreateTable.isFull()) {
            LogicalShowCreateTablesForShardingDatabaseHandler.buildLBACAttr(createTable, schemaName, tableName);
        }

        //sql = createTable.toString();
        sql = createTable.toSqlString(needShowHashByRange, outputMySQLIndent);

        String tableLocality = partitionInfoManager.getPartitionInfo(tableName).getLocality();
        LocalityDesc localityDesc = LocalityInfoUtils.parse(tableLocality);
        if (!localityDesc.isEmpty()) {
            sql += "\n" + localityDesc.showCreate();
        }

        if (!tableMeta.isAutoPartition() || showCreateTable.isFull()) {
            sql = sql + partitionStr;
        }
        if (tableMeta.getLocalPartitionDefinitionInfo() != null) {
            sql += "\n" + tableMeta.getLocalPartitionDefinitionInfo().toString();
        }

        sql = sql + buildTableGroupInfo(schemaName, showCreateTable, partInfo);
        sql = tryAttachImplicitTableGroupInfo(executionContext, schemaName, tableName, sql);

        result.addRow(new Object[] {tableName, sql});
        return result;
    }

    private String buildTableGroupInfo(String schemaName, SqlShowCreateTable showCreateTable,
                                       PartitionInfo partInfo) {
        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tgConfig = oc.getTableGroupInfoManager().getTableGroupConfigById(partInfo.getTableGroupId());

        if (tgConfig == null) {
            return "";
        }

        String tgName = tgConfig != null ? tgConfig.getTableGroupRecord().tg_name : "";

        if (tgConfig.getTableGroupRecord().getManual_create() == 1) {
            return String.format("\ntablegroup = %s ", SqlIdentifier.surroundWithBacktick(tgName));
        } else if (showCreateTable.isFull()) {
            return String.format("\n/* tablegroup = %s */", SqlIdentifier.surroundWithBacktick(tgName));
        } else {
            return "";
        }
    }

    private ArrayResultCursor showCreateView(String schemaName, String tableName, SqlShowCreateTable showCreateTable) {
        ViewManager viewManager;
        if (InformationSchema.NAME.equalsIgnoreCase(schemaName)) {
            viewManager = InformationSchemaViewManager.getInstance();
        } else if (RelUtils.informationSchema(showCreateTable.getTableName())) {
            viewManager = InformationSchemaViewManager.getInstance();
        } else if (RelUtils.mysqlSchema(showCreateTable.getTableName())) {
            viewManager = MysqlSchemaViewManager.getInstance();
        } else {
            viewManager = OptimizerContext.getContext(schemaName).getViewManager();
        }

        SystemTableView.Row row = viewManager.select(tableName);
        if (row != null) {
            ArrayResultCursor resultCursor = new ArrayResultCursor(tableName);
            // | View | Create View | character_set_client | collation_connection |
            resultCursor.addColumn("View", DataTypes.StringType, false);
            resultCursor.addColumn("Create View", DataTypes.StringType, false);
            resultCursor.addColumn("character_set_client", DataTypes.StringType, false);
            resultCursor.addColumn("collation_connection", DataTypes.StringType, false);
            resultCursor.initMeta();
            String createView = row.isVirtual() ? "[VIRTUAL_VIEW] " + row.getViewDefinition() :
                "CREATE VIEW `" + tableName + "` AS " + row.getViewDefinition();
            resultCursor.addRow(new Object[] {
                tableName,
                createView, "utf8",
                "utf8_general_ci"});
            return resultCursor;
        } else {
            return null;
        }
    }

    private String fetchShowCreateTableFromPhy(String schemaName, String tableName,
                                               SqlShowCreateTable showCreateTable, LogicalShow show,
                                               ExecutionContext executionContext) {
        Cursor cursor = null;
        try {

            SqlNode logTbSqlNode = showCreateTable.getTableName();
            SqlIdentifier phyTbSqlId = null;
            if (logTbSqlNode instanceof SqlIdentifier) {
                SqlIdentifier logTbSqlId = (SqlIdentifier) logTbSqlNode;
                phyTbSqlId = new SqlIdentifier(logTbSqlId.getLastName(), logTbSqlId.getCollation(),
                    logTbSqlId.getParserPosition());
            }

            cursor = repo.getCursorFactory()
                .repoCursor(executionContext,
                    new PhyShow(show.getCluster(),
                        show.getTraitSet(),
                        SqlShowCreateTable.create(SqlParserPos.ZERO,
                            TStringUtil.isEmpty(show.getPhyTable()) ? phyTbSqlId :
                                show.getPhyTableNode()),
                        show.getRowType(),
                        show.getDbIndex(),
                        show.getPhyTable(),
                        show.getSchemaName()));
            Row row;
            if ((row = cursor.next()) != null) {
                String sql = row.getString(1);
                return sql;
            }

            return null;
        } finally {
            // 关闭cursor
            if (cursor != null) {
                cursor.close(new ArrayList<Throwable>());
            }
        }
    }

    public List<SQLTableElement> buildIndexDefs(String schemaName,
                                                GsiMetaBean gsiMeta,
                                                String mainTableName,
                                                TableMeta meta,
                                                List<SQLTableElement> localIndexes,
                                                boolean full,
                                                boolean needShowHashByRange) {
        Set<String> ignoredLocalIndexNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        List<SQLTableElement> indexDefs = new ArrayList<>();
        if (meta.withGsi() && gsiMeta.getTableMeta() != null && !gsiMeta.getTableMeta().isEmpty()) {
            final GsiMetaManager.GsiTableMetaBean mainTableMeta = gsiMeta.getTableMeta().get(mainTableName);
            for (Map.Entry<String, GsiMetaManager.GsiIndexMetaBean> entry : mainTableMeta.indexMap.entrySet()) {
                final String indexName = entry.getKey();
                final GsiMetaManager.GsiIndexMetaBean indexMeta = entry.getValue();
                final GsiMetaManager.GsiTableMetaBean indexTableMeta = gsiMeta.getTableMeta().get(indexName);

                // Ignore GSI which is not public.
                if (indexMeta.indexStatus != IndexStatus.PUBLIC) {
                    continue;
                }

                List<SQLSelectOrderByItem> indexColumns = new ArrayList<>(indexMeta.indexColumns.size());
                SQLCharExpr comment =
                    TStringUtil.isEmpty(indexMeta.indexComment) ? null : new SQLCharExpr(indexMeta.indexComment);

                SQLPartitionBy sqlPartitionBy = buildSqlPartitionBy(indexTableMeta, needShowHashByRange);

                for (GsiMetaManager.GsiIndexColumnMetaBean indexColumn : indexMeta.indexColumns) {
                    SQLSelectOrderByItem orderByItem = new SQLSelectOrderByItem();
                    if (null != indexColumn.subPart && indexColumn.subPart > 0) {
                        final SQLMethodInvokeExpr methodInvoke =
                            new SQLMethodInvokeExpr(SqlIdentifier.surroundWithBacktick(indexColumn.columnName));
                        methodInvoke.addArgument(new SQLIntegerExpr(indexColumn.subPart));
                        orderByItem.setExpr(methodInvoke);
                    } else {
                        orderByItem
                            .setExpr(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(indexColumn.columnName)));
                    }
                    if (TStringUtil.equals("A", indexColumn.collation)) {
                        orderByItem.setType(SQLOrderingSpecification.ASC);
                    } else if (TStringUtil.equals("D", indexColumn.collation)) {
                        orderByItem.setType(SQLOrderingSpecification.DESC);
                    }
                    indexColumns.add(orderByItem);
                }
                List<SQLName> coveringColumns = new ArrayList<>(indexMeta.coveringColumns.size());
                for (GsiMetaManager.GsiIndexColumnMetaBean coveringColumn : indexMeta.coveringColumns) {
                    if (SqlValidatorImpl.isImplicitKey(coveringColumn.columnName)) {
                        continue;
                    }
                    // ignore primary columns
                    if (meta.getPrimaryIndex().getKeyColumn(coveringColumn.columnName) != null) {
                        continue;
                    }
                    coveringColumns
                        .add(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(coveringColumn.columnName)));
                }

                // Get show name.
                String showName = DbInfoManager.getInstance().isNewPartitionDb(schemaName) ?
                    TddlSqlToRelConverter.unwrapGsiName(indexMeta.indexName) : indexMeta.indexName;
                String prefix =
                    showName.equals(indexMeta.indexName) ? "" : "/* " + indexMeta.indexName + " */ ";
                ignoredLocalIndexNames.add(TddlConstants.AUTO_LOCAL_INDEX_PREFIX + showName);
                showName = SqlIdentifier.surroundWithBacktick(showName);

                if (full) {
                    showName = prefix + showName;
                }

                final MySqlUnique indeDef = new MySqlUnique();
                indeDef.getIndexDefinition().setIndex(true);
                if (!indexMeta.nonUnique) {
                    indeDef.getIndexDefinition().setType("UNIQUE");
                }
                indeDef.getIndexDefinition().getOptions().setIndexType(indexMeta.indexType);
                indeDef.setName(showName);
                indeDef.setComment(comment);
                if (indexMeta.visibility == IndexVisibility.VISIBLE) {
                    indeDef.getIndexDefinition().setVisible(true);
                } else {
                    indeDef.getIndexDefinition().setVisible(false);
                }
                if (full || !meta.isAutoPartition()) {
                    indeDef.setPartitioning(sqlPartitionBy);
                }
                indeDef.getColumns().addAll(indexColumns);
                if (indexMeta.clusteredIndex) {
                    indeDef.setClustered(true);
                } else {
                    indeDef.getCovering().addAll(coveringColumns);
                }
                indeDef.setColumnar(indexMeta.columnarIndex);
                if (full && indexMeta.columnarIndex) {
                    // set options
                    Map<String, String> options = getColumnarIndexOptions(indexMeta.tableSchema, indexMeta.indexName);
                    if (options != null) {
                        indeDef.setDictionaryColumns(options.get(ColumnarTableOptions.DICTIONARY_COLUMNS));
                    }
                }

                if (!coveringColumns.isEmpty() || full || !meta.isAutoPartition()) {
                    if (!indeDef.isClustered() && !indeDef.isColumnar()) {
                        indeDef.setGlobal(true); // Set one of global or clustered.
                    }
                }
                indexDefs.add(indeDef);
            } // end of for
        }
        for (SQLTableElement localIndexElement : localIndexes) {
            String indexName;
            if (localIndexElement instanceof MySqlKey) {
                MySqlKey key = (MySqlKey) localIndexElement;
                indexName = SQLUtils.normalizeNoTrim(key.getName().getSimpleName());
                if (meta.isAutoPartition() || full) {
                    key.getIndexDefinition().setLocal(true);
                }
            } else if (localIndexElement instanceof MySqlTableIndex) {
                MySqlTableIndex key = (MySqlTableIndex) localIndexElement;
                indexName = SQLUtils.normalizeNoTrim(key.getName().getSimpleName());
                if (meta.isAutoPartition() || full) {
                    key.setLocal(true);
                }
            } else {
                continue;
            }

            if (!full && meta.isAutoPartition()) {
                if (ignoredLocalIndexNames.contains(indexName)) {
                    continue;
                }
            }

            indexDefs.add(localIndexElement);
        }
        return indexDefs;
    }

    private Map<String, String> getColumnarIndexOptions(String schemaName, String indexName) {
        ColumnarTableEvolutionAccessor accessor = new ColumnarTableEvolutionAccessor();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            accessor.setConnection(metaDbConn);
            List<ColumnarTableEvolutionRecord> records =
                accessor.querySchemaIndexLatest(schemaName, indexName);
            if (CollectionUtils.isEmpty(records)) {
                logger.error("empty columnar_table_evolution record: " + indexName);
                return null;
            }
            return records.get(0).options;
        } catch (SQLException e) {
            logger.error("failed to query columnar option info", e);
        }
        return null;
    }

    public SQLPartitionBy buildSqlPartitionBy(GsiMetaManager.GsiTableMetaBean indexTableMeta,
                                              boolean needShowHashByRange) {
        String indexName = indexTableMeta.gsiMetaBean.indexName;
        String schemaName = indexTableMeta.gsiMetaBean.indexSchema;
        PartitionInfoManager partitionInfoManager =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(indexName);
        if (partitionInfo != null) {
            boolean usePartitionBy = partitionInfo.isGsi() || partitionInfo.isPartitionedTable() ||
                partitionInfo.isColumnar();
            ByteString byteString = ByteString.from(
                usePartitionBy ? partitionInfo.showCreateTablePartitionDefInfo(needShowHashByRange, "\t\t") : "");
            final MySqlCreateTableParser createParser = new MySqlCreateTableParser(byteString);
            final SQLPartitionBy partitionBy = createParser.parsePartitionBy();
            partitionBy.setSourceSql(byteString);
            return partitionBy;
        }
        return null;
    }

    private boolean needShowImplicitId(ExecutionContext executionContext) {
        Object value = executionContext.getExtraCmds().get(ConnectionProperties.SHOW_IMPLICIT_ID);
        return value != null && Boolean.parseBoolean(value.toString());
    }

    private String tryAttachImplicitTableGroupInfo(ExecutionContext executionContext, String schemaName,
                                                   String tableName, String sql) {
        Object value = executionContext.getExtraCmds().get(ConnectionProperties.SHOW_IMPLICIT_TABLE_GROUP);
        if (value != null && Boolean.parseBoolean(value.toString())) {
            return ImplicitTableGroupUtil.tryAttachImplicitTableGroup(schemaName, tableName, sql);
        }
        return sql;
    }
}
