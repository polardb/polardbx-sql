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

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderingSpecification;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.locality.LocalityInfo;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author mengshi
 */
public class LogicalShowCreateTablesForPartitionDatabaseHandler extends HandlerCommon {

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
        result.addColumn("Table", DataTypes.StringType);
        result.addColumn("Create Table", DataTypes.StringType);
        result.initMeta();

        String sql = fetchShowCreateTableFromPhy(schemaName, tableName, showCreateTable, show, executionContext);
        result.initMeta();

        StringBuilder partitionStr = new StringBuilder();
        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        PartitionInfoManager partitionInfoManager = tddlRuleManager.getPartitionInfoManager();
        PartitionInfo partInfo = partitionInfoManager.getPartitionInfo(tableName);

        final ParamManager pm = executionContext.getParamManager();
        boolean needShowHashByRange = pm.getBoolean(ConnectionParams.SHOW_HASH_PARTITIONS_BY_RANGE);
        String partitionByStr = partInfo.showCreateTablePartitionDefInfo(needShowHashByRange);
        partitionStr.append("\n").append(partitionByStr);

        final GsiMetaBean gsiMeta = ExecutorContext.getContext(schemaName)
            .getGsiManager()
            .getGsiTableAndIndexMeta(schemaName, tableName, IndexStatus.ALL);

        // handle implicit pk
        MySqlCreateTableStatement createTable =
            (MySqlCreateTableStatement) SQLUtils.parseStatements(sql,
                JdbcConstants.MYSQL)
                .get(0)
                .clone();

        createTable.setTableName(SqlIdentifier.surroundWithBacktick(tableName));

        List<SQLTableElement> toRemove = Lists.newArrayList();
        for (SQLTableElement sqlTableElement : createTable.getTableElementList()) {
            if (sqlTableElement instanceof SQLColumnDefinition
                && SqlValidatorImpl.isImplicitKey(((SQLColumnDefinition) sqlTableElement).getNameAsString())
                && !needShowImplicitId(executionContext) && !showCreateTable.isFull()) {
                toRemove.add(sqlTableElement);
            }
            if (sqlTableElement instanceof MySqlPrimaryKey
                && SqlValidatorImpl
                .isImplicitKey(((MySqlPrimaryKey) sqlTableElement).getColumns().get(0).toString())
                && !needShowImplicitId(executionContext) && !showCreateTable.isFull()) {
                toRemove.add(sqlTableElement);
            }
        }
        createTable.getTableElementList().removeAll(toRemove);
        List<SQLTableElement> localIndexes = Lists.newArrayList();

        for (SQLTableElement element : createTable.getTableElementList()) {
            if ((element instanceof MySqlKey || element instanceof MySqlTableIndex)
                && !(element instanceof MySqlPrimaryKey)) {
                localIndexes.add(element);
            }
        }
        createTable.getTableElementList().removeAll(localIndexes);

        List<SQLTableElement> indexDefs =
            buildIndexDefs(schemaName, gsiMeta, tableName, tableMeta, localIndexes, showCreateTable.isFull());
        createTable.getTableElementList().addAll(indexDefs);

        if (tableMeta.isAutoPartition() && showCreateTable.isFull()) {
            createTable.setPrefixPartition(true);
        }

        sql = createTable.toString();

        LocalityManager lm = LocalityManager.getInstance();
        LocalityInfo localityInfo = lm.getLocalityOfTable(tableMeta.getId());
        if (localityInfo != null) {
            LocalityDesc localityDesc = LocalityDesc.parse(localityInfo.getLocality());
            sql += "\n" + localityDesc.showCreate();
        }

        if (!tableMeta.isAutoPartition() || showCreateTable.isFull()) {
            sql = sql + partitionStr;
        }

        sql = sql + buildTableGroupInfo(schemaName, showCreateTable, partInfo);

        result.addRow(new Object[] {tableName, sql});
        return result;
    }

    private String buildTableGroupInfo(String schemaName, SqlShowCreateTable showCreateTable,
                                       PartitionInfo partInfo) {
        TableGroupConfig tgConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigById(partInfo.getTableGroupId());

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
            resultCursor.addColumn("View", DataTypes.StringType);
            resultCursor.addColumn("Create View", DataTypes.StringType);
            resultCursor.addColumn("character_set_client", DataTypes.StringType);
            resultCursor.addColumn("collation_connection", DataTypes.StringType);
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
                                                boolean full) {
        Set<String> ignoredLocalIndexNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        List<SQLTableElement> indexDefs = new ArrayList<>();
        if (meta.withGsi()) {
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

                SQLPartitionBy sqlPartitionBy = buildSqlPartitionBy(indexTableMeta);

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
                if (full || !meta.isAutoPartition()) {
                    indeDef.setPartitioning(sqlPartitionBy);
                }
                indeDef.getColumns().addAll(indexColumns);
                if (indexMeta.clusteredIndex) {
                    indeDef.setClustered(true);
                } else {
                    indeDef.getCovering().addAll(coveringColumns);
                }
                if (!coveringColumns.isEmpty() || full || !meta.isAutoPartition()) {
                    if (!indeDef.isClustered()) {
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

    public SQLPartitionBy buildSqlPartitionBy(GsiMetaManager.GsiTableMetaBean indexTableMeta) {
        String indexName = indexTableMeta.gsiMetaBean.indexName;
        String schemaName = indexTableMeta.gsiMetaBean.indexSchema;
        PartitionInfoManager partitionInfoManager =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(indexName);
        if (partitionInfo != null) {
            final MySqlCreateTableParser createParser =
                new MySqlCreateTableParser(new MySqlExprParser(partitionInfo.getPartitionBy().toString()));
            final SQLPartitionBy partitionBy = createParser.parsePartitionBy();
            return partitionBy;
        }
        return null;
    }

    private boolean needShowImplicitId(ExecutionContext executionContext) {
        Object value = executionContext.getExtraCmds().get(ConnectionProperties.SHOW_IMPLICIT_ID);
        return value != null && Boolean.parseBoolean(value.toString());
    }
}
