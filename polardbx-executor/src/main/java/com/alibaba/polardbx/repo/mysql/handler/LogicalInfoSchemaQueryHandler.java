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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.atom.config.TAtomDsConfDO;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.common.RecycleBin;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.seq.SequenceAccessor;
import com.alibaba.polardbx.gms.metadb.seq.SequenceOptAccessor;
import com.alibaba.polardbx.gms.metadb.seq.SequenceOptRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequenceRecord;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.TruncateUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.BooleanType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.StringType;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Like;
import com.alibaba.polardbx.optimizer.core.rel.PhyQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.metadata.InfoSchemaCommon;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyQueryCursor;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.dal.Show;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlShowTableStatus;
import org.apache.calcite.sql.SqlShowTables;
import org.apache.calcite.sql.SqlShowTables.SqlShowTablesOperator;
import org.apache.calcite.sql.TDDLSqlSelect;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public abstract class LogicalInfoSchemaQueryHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalInfoSchemaQueryHandler.class);

    public LogicalInfoSchemaQueryHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(final RelNode logicalPlan, ExecutionContext executionContext) {
        verifyLogicalPlan(logicalPlan, executionContext);

        LogicalInfoSchemaContext infoSchemaContext = new LogicalInfoSchemaContext(executionContext);

        ArrayResultCursor resultCursor = new ArrayResultCursor(InfoSchemaCommon.INFO_SCHEMA_QUERY_TITLE);

        PhyQueryOperation infoSchemaQuery = (PhyQueryOperation) logicalPlan;

        // Get target schema since we support cross schema query.
        extractTargetSchema(infoSchemaQuery, infoSchemaContext, executionContext);

        if (infoSchemaContext.isSystemSchema()) {
            return repo.getCursorFactory().repoCursor(executionContext, logicalPlan);
        }

        infoSchemaContext.prepareContextAndRepository(repo);

        // Get logical table names including tables from current rule plus
        // single tables not exist in current rule.
        // The mapping is logical table name -> sharding type (i.e. whether
        // table partitions exist or not)
        showTables(infoSchemaQuery, infoSchemaContext);

        // Generate the mapping of physical and logical table names.
        generateTableNameMapping(infoSchemaContext);

        // Rewrite original logical plan to meet our needs.
        rewriteLogicalPlan(infoSchemaQuery, infoSchemaContext, executionContext);

        // Execute the rewritten query against the default physical database.
        MyPhyQueryCursor cursor = null;
        try {
            cursor = (MyPhyQueryCursor) infoSchemaContext.getRealRepo()
                .getCursorFactory()
                .repoCursor(executionContext, infoSchemaQuery);
            List<ColumnMeta> columnMetas = cursor.getReturnColumns();

            int numOfTotalCols = columnMetas.size();
            int numOfResultCols = numOfTotalCols - infoSchemaContext.getNumOfAddedCols();

            buildResultColumns(resultCursor, columnMetas, numOfResultCols, infoSchemaContext.getCountAggFuncTitle());

            // Get necessary column index.
            int firstTableSchemaIndex = getFirstColumnIndex(columnMetas, InfoSchemaCommon.COLUMN_TABLE_SCHEMA);
            int firstTableNameIndex = getFirstColumnIndex(columnMetas, InfoSchemaCommon.COLUMN_TABLE_NAME);

            // Get alias indexes and replace original ones if exist.
            Map<String, String> origToAliasMapping = infoSchemaContext.getOrigToAliasMapping();
            if (origToAliasMapping.size() > 0) {
                int firstTableSchemaAliasIndex = getFirstColumnIndex(columnMetas,
                    origToAliasMapping.get(InfoSchemaCommon.COLUMN_TABLE_SCHEMA));
                if (firstTableSchemaAliasIndex >= 0) {
                    firstTableSchemaIndex = firstTableSchemaAliasIndex;
                }
                int firstTableNameAliasIndex = getFirstColumnIndex(columnMetas,
                    origToAliasMapping.get(InfoSchemaCommon.COLUMN_TABLE_NAME));
                if (firstTableNameAliasIndex >= 0) {
                    firstTableNameIndex = firstTableNameAliasIndex;
                }
            }

            // Build context for specific INFORMATION_SCHEMA query handling.
            infoSchemaContext.setColumnMetas(columnMetas);
            infoSchemaContext.setCursor(cursor);
            infoSchemaContext.setNumOfTotalCols(numOfTotalCols);
            infoSchemaContext.setNumOfResultCols(numOfResultCols);
            infoSchemaContext.setResultCursor(resultCursor);
            infoSchemaContext.setFirstTableSchemaIndex(firstTableSchemaIndex);
            infoSchemaContext.setFirstTableNameIndex(firstTableNameIndex);

            // Handle specific INFORMATION_SCHEMA query.
            doHandle(infoSchemaContext);

            // Handle LIMIT [OFFSET]
            handleLimit(infoSchemaContext);

        } finally {
            if (cursor != null) {
                cursor.close(new ArrayList<>());
            }
        }

        replaceResultForCountAggFunction(resultCursor, infoSchemaContext);

        return resultCursor;
    }

    protected void doHandle(LogicalInfoSchemaContext infoSchemaContext) {
        // Empty operations for being overridden by non-INFORMATION_SCHEMA
        // queries, such as SHOW TABLES.
    }

    protected void handleLimit(LogicalInfoSchemaContext infoSchemaContext) {
        int offset = 0, rowCount = 0;
        for (Object[] resultRow : infoSchemaContext.getResultSet()) {
            if (infoSchemaContext.getRowCount() >= 0) {
                if (++offset > infoSchemaContext.getOffset()) {
                    if (++rowCount <= infoSchemaContext.getRowCount()) {
                        infoSchemaContext.getResultCursor().addRow(resultRow);
                    } else {
                        // Already get the rows required.
                        break;
                    }
                } else {
                    // Continue until specified offset.
                    continue;
                }
            } else {
                // Fetch all the rows since no LIMIT clause is specified.
                infoSchemaContext.getResultCursor().addRow(resultRow);
            }
        }
    }

    protected void verifyLogicalPlan(final RelNode logicalPlan, ExecutionContext executionContext) {
        if (!(logicalPlan instanceof PhyQueryOperation)
            || !(((PhyQueryOperation) logicalPlan).getNativeSqlNode() instanceof TDDLSqlSelect)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Unexpected logical plan for the INFORMATION_SCHEMA query: " + logicalPlan.toString());
        }
    }

    // Build plan for SHOW TABLES to get logical table names.
    private void showTables(PhyQueryOperation infoSchemaQuery, LogicalInfoSchemaContext infoSchemaContext) {
        String schemaName = infoSchemaContext.getTargetSchema();

        final SqlShowTables showTables = SqlShowTables.create(SqlParserPos.ZERO,
            false,
            null,
            schemaName,
            null,
            null,
            null,
            null);

        final LogicalShow showTablesNode = LogicalShow.create(
            Show.create(
                showTables,
                SqlShowTablesOperator.getRelDataType(infoSchemaQuery.getCluster().getTypeFactory(), null, false),
                infoSchemaQuery.getCluster()),
            infoSchemaContext.getOptimizerContext().getRuleManager().getTddlRule().getDefaultDbIndex(),
            null,
            schemaName);

        showTables(showTablesNode, infoSchemaContext);
    }

    // Collect table names only
    protected void showTables(LogicalShow showNode, LogicalInfoSchemaContext infoSchemaContext) {
        List<String> tableNames = infoSchemaContext.getLogicalTableNames();

        List<Object[]> fullTables = showFullTables(showNode, infoSchemaContext);

        for (Object[] fullTable : fullTables) {
            tableNames.add((String) fullTable[0]);
        }
    }

    // Collect table name Object[0] along with table type Object[1]
    protected List<Object[]> showFullTables(LogicalShow showNode, LogicalInfoSchemaContext infoSchemaContext) {
        ExecutionContext executionContext = infoSchemaContext.getExecutionContext();

        SqlShowTables show = (SqlShowTables) showNode.getNativeSqlNode();
        boolean full = show.isFull();

        List<String> defaultDbTables = new ArrayList<>();
        Map<String, String> tableTypes = new HashMap<>();

        boolean showFromRuleOnly;
        if (infoSchemaContext.getExecutorContext().getTopologyHandler().getGroupNames().size() > 1) {
            showFromRuleOnly = executionContext.getParamManager().getBoolean(
                ConnectionParams.SHOW_TABLES_FROM_RULE_ONLY, true);
        } else {
            showFromRuleOnly = executionContext.getParamManager().getBoolean(
                ConnectionParams.SHOW_TABLES_FROM_RULE_ONLY, false);
        }

        boolean useDataBase = true;
        String schema = show.getSchema() == null ? "" : show.getSchema();
        String dbName = show.getDbName() == null ? "" : show.getDbName().toString();
        if (schema.length() == 0 || dbName.length() == 0 || schema.equalsIgnoreCase(dbName)) {
            useDataBase = false;
        }

        if (!showFromRuleOnly || useDataBase) {
            // Retrieve table names from the default database if needed.
            Cursor cursor = null;
            try {
                SqlShowTables showTables = SqlShowTables.create(SqlParserPos.ZERO,
                    show.isFull(),
                    useDataBase ? show.getDbName() : null,
                    show.getSchema(),
                    show.like,
                    null,
                    null,
                    null);

                PhyShow plan = new PhyShow(showNode.getCluster(),
                    showNode.getTraitSet(),
                    showTables,
                    showNode.getRowType(),
                    showNode.getDbIndex(),
                    showNode.getPhyTable(),
                    showNode.getSchemaName());
                cursor = ExecutorHelper.execute(plan, executionContext);

                Row row;
                while ((row = cursor.next()) != null) {
                    String table = row.getString(0);
                    defaultDbTables.add(table);
                    if (full) {
                        String type = row.getString(1);
                        tableTypes.put(table, type);
                    }
                }
            } finally {
                if (cursor != null) {
                    cursor.close(new ArrayList<>());
                }
            }
        }

        Set<String> tableNames = null;
        Map<String, Boolean> tablesAutoPartInfo = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        String targetDb = showNode.getSchemaName();
        if (!DbInfoManager.getInstance().isNewPartitionDb(targetDb)) {

            // Merge with logical tables from current rule.
            tableNames = infoSchemaContext.getOptimizerContext().getRuleManager().mergeTableRule(defaultDbTables);

            /**
             * This should be removed because in old part db , there are not any partition tables
             */
            Set<String> partitionTables =
                infoSchemaContext.getOptimizerContext().getPartitionInfoManager().getPartitionTables();
            tableNames.addAll(partitionTables);
        } else {
            tableNames = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            tableNames.addAll(infoSchemaContext.getOptimizerContext().getPartitionInfoManager().getPartitionTables());
        }

        if (infoSchemaContext.isWithView()) {
            tableNames.addAll(infoSchemaContext.getOptimizerContext().getViewManager().selectAllViewName());
        }

        SchemaManager schemaManager =
            OptimizerContext.getContext(targetDb).getLatestSchemaManager();

        Iterator<String> iter = tableNames.iterator();
        // Mock mode dont consider mem table/recycle bin table/hidden table/privilege of table
        while (!ConfigDataMode.isFastMock() && iter.hasNext()) {
            String tableName = iter.next();

            boolean isRecycleBinTable = RecycleBin.isRecyclebinTable(tableName);

            boolean isTruncateTmpTable = TruncateUtil.isTruncateTmpPrimaryTable(tableName);

            boolean isTableWithoutPrivileges = !CanAccessTable.verifyPrivileges(
                infoSchemaContext.getTargetSchema(),
                tableName,
                infoSchemaContext.getExecutionContext());

            boolean needRemoveGsi = false;
            boolean needRemoveNonPublic = false;

                try {
                    TableMeta tableMeta = schemaManager.getTable(tableName);
                    needRemoveGsi = tableMeta.isGsi();
                    needRemoveNonPublic = tableMeta.getStatus() != TableStatus.PUBLIC;
                    tablesAutoPartInfo.put(tableName, tableMeta.isAutoPartition());
                } catch (Throwable t) {
                    // ignore table not exists

            }

            if (isRecycleBinTable || isTableWithoutPrivileges || needRemoveGsi || needRemoveNonPublic
                || isTruncateTmpTable) {
                iter.remove();
            }
        }

        // Handle the LIKE operator.
        Like like = null;
        String likeExpr = null;
        if (null != show.like) {
            likeExpr = show.like.toString();
            like = new Like(Arrays.asList(new StringType(), new StringType()), null);
            if (likeExpr.charAt(0) == '\'' && likeExpr.charAt(likeExpr.length() - 1) == '\'') {
                likeExpr = likeExpr.substring(1, likeExpr.length() - 1);
            }
        }

        List<Object[]> result = new ArrayList<>();
        for (String table : tableNames) {
            if (like != null) {
                boolean bool = BooleanType.isTrue(DataTypes.BooleanType.convertFrom(like.compute(new Object[] {
                    table,
                    likeExpr}, executionContext)));
                if (!bool) {
                    continue;
                }
            }
            if (full) {
                String type = tableTypes.get(table);
                if (type == null) {
                    type = InfoSchemaCommon.DEFAULT_TABLE_TYPE;
                }
                String autoPart = "NO";
                if (tablesAutoPartInfo.get(table) != null) {
                    autoPart = tablesAutoPartInfo.get(table) ? "YES" : "NO";
                }
                result.add(new Object[] {table, type, autoPart});
            } else {
                result.add(new Object[] {table});
            }
        }

        // Sort by table name.
        result.sort((o1, o2) -> TStringUtil.compareTo((String) o1[0], (String) o2[0]));

        return result;
    }

    protected void extractTargetSchema(PhyQueryOperation infoSchemaQuery, LogicalInfoSchemaContext infoSchemaContext,
                                       ExecutionContext executionContext) {
        SqlNode whereNode = ((TDDLSqlSelect) infoSchemaQuery.getNativeSqlNode()).getWhere();

        if (whereNode == null && !infoSchemaContext.isCustomSchemaFilter()) {
            // Current query doesn't have any condition, so that we have to
            // build a schema filter with current schema to improve performance.
            buildCustomSchemaFilter(infoSchemaQuery, infoSchemaContext);
            // Get where node again.
            whereNode = ((TDDLSqlSelect) infoSchemaQuery.getNativeSqlNode()).getWhere();
        }

        if (whereNode instanceof SqlBasicCall) {
            // Parameter indexes for table schema and name
            Map<String, Set<Integer>> paramIndexes = new HashMap<>();
            paramIndexes.put(InfoSchemaCommon.COLUMN_TABLE_SCHEMA, new HashSet<>());
            paramIndexes.put(InfoSchemaCommon.COLUMN_TABLE_NAME, new HashSet<>());
            paramIndexes.put(InfoSchemaCommon.COLUMN_SCHEMA_NAME, new HashSet<>());
            infoSchemaContext.setParamIndexes(paramIndexes);

            findSchemaAndReplaceNameOperator((SqlBasicCall) whereNode, paramIndexes);

            // We only support one schema as target schema.
            int schemaParamIndex = 0;
            if (paramIndexes.get(InfoSchemaCommon.COLUMN_TABLE_SCHEMA).size() == 1) {
                schemaParamIndex = paramIndexes.get(InfoSchemaCommon.COLUMN_TABLE_SCHEMA).iterator().next();
            } else if (paramIndexes.get(InfoSchemaCommon.COLUMN_SCHEMA_NAME).size() == 1) {
                schemaParamIndex = paramIndexes.get(InfoSchemaCommon.COLUMN_SCHEMA_NAME).iterator().next();
            }
            if (schemaParamIndex > 0) {
                Map<Integer, ParameterContext> params =
                    infoSchemaQuery.getDbIndexAndParam(null, executionContext).getValue();
                if (params != null) {
                    for (Integer index : params.keySet()) {
                        if (index == schemaParamIndex) {
                            String schemaName = (String) params.get(index).getValue();
                            if (InfoSchemaCommon.MYSQL_SYS_SCHEMAS.contains(schemaName.toUpperCase())) {
                                infoSchemaContext.setSystemSchema(true);
                            } else {
                                infoSchemaContext.setTargetSchema(schemaName);
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    private void buildCustomSchemaFilter(PhyQueryOperation infoSchemaQuery,
                                         LogicalInfoSchemaContext infoSchemaContext) {
        // Check parameters first to know the index that we should build for
        // new where clause.
        Map<Integer, ParameterContext> params = infoSchemaQuery.getParam();
        if (params == null) {
            params = new HashMap<>(1);
        }

        int newDynamicParamIndex = params.size();

        int newParamIndex = newDynamicParamIndex + 1;
        ParameterContext parameterContext = new ParameterContext(ParameterMethod.setString, new Object[] {
            newParamIndex, infoSchemaContext.getTargetSchema()});
        params.put(newParamIndex, parameterContext);

        // Then build a where clause
        SqlNode whereClause = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
            new SqlIdentifier(InfoSchemaCommon.COLUMN_TABLE_SCHEMA, SqlParserPos.ZERO),
            new SqlDynamicParam(newDynamicParamIndex, SqlTypeName.CHAR, SqlParserPos.ZERO));
        ((TDDLSqlSelect) infoSchemaQuery.getNativeSqlNode()).setWhere(whereClause);

        // Indicate that there is a custom schema filter, so that we can adjust
        // parameter index accordingly later if necessary.
        infoSchemaContext.setCustomSchemaFilter(true);
    }

    // Physical and logical table name mappings
    private void generateTableNameMapping(LogicalInfoSchemaContext infoSchemaContext) {
        Map<String, String> tableNameMapping = infoSchemaContext.getTableNameMapping();
        Set<String> logicalTableNamesFromRule = new HashSet<>();

        OptimizerContext optimizerContext = infoSchemaContext.getOptimizerContext();

        String defaultDbIndex = optimizerContext.getRuleManager().getDefaultDbIndex(null);

        // Add table name mapping from current rule
        Collection<TableRule> tableRules = optimizerContext.getRuleManager().getTddlRule().getTables();
        for (TableRule tableRule : tableRules) {
            String logicalTableNameFromRule = tableRule.getVirtualTbName();

            boolean isLegacySecondaryIndex = TStringUtil.contains(
                logicalTableNameFromRule,
                InfoSchemaCommon.KEYWORD_SECONDARY_INDEX);

            boolean isSystemTable = SystemTables.contains(logicalTableNameFromRule);
            boolean isTableWithoutPrivileges = !CanAccessTable.verifyPrivileges(
                infoSchemaContext.getTargetSchema(),
                logicalTableNameFromRule,
                infoSchemaContext.getExecutionContext());

            if (isLegacySecondaryIndex || isSystemTable || isTableWithoutPrivileges) {
                continue;
            }

            for (Map.Entry entry : tableRule.getActualTopology().entrySet()) {
                // We only need table names from the default database
                if (TStringUtil.equalsIgnoreCase((String) entry.getKey(), defaultDbIndex)) {
                    for (String physicalTableName : (Set<String>) entry.getValue()) {
                        tableNameMapping.put(physicalTableName, logicalTableNameFromRule);
                        tableNameMapping.put(physicalTableName.toUpperCase(), logicalTableNameFromRule);
                        tableNameMapping.put(physicalTableName.toLowerCase(), logicalTableNameFromRule);
                    }
                    break;
                }
            }

            logicalTableNamesFromRule.add(logicalTableNameFromRule);
        }

        // Add table names that are not in current rule
        for (String logicalTableName : infoSchemaContext.getLogicalTableNames()) {
            boolean found = false;
            for (String logicTableNameFromRule : logicalTableNamesFromRule) {
                if (logicTableNameFromRule.equalsIgnoreCase(logicalTableName)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                tableNameMapping.put(logicalTableName, logicalTableName);
            }
        }
    }

    // Rewrite original logical plan.
    private void rewriteLogicalPlan(PhyQueryOperation infoSchemaQuery, LogicalInfoSchemaContext infoSchemaContext,
                                    ExecutionContext executionContext) {
        // Rewrite the plan to
        // - add extra columns for subsequent mapping and
        // - replace logical name with the default physical name since we don't
        // rely on shadow database anymore
        TDDLSqlSelect queryNode = (TDDLSqlSelect) infoSchemaQuery.getNativeSqlNode();

        // Collect the default physical table name for each logical table
        Map<String, String> defaultPhyTableNamesForLogical = collectDefaultPhyTableNames(infoSchemaContext);

        // Add extra columns in order to map logical table names.
        addExtraColumns(queryNode, infoSchemaContext);

        // Handle the case in which the aliases of some columns are different
        // from their original names.
        handleColumnAlias(queryNode, infoSchemaContext);

        // Rewrite logical filter
        SqlNode whereNode = queryNode.getWhere();
        if (whereNode instanceof SqlBasicCall) {
            // Parameter indexes for table schema and name
            Map<String, Set<Integer>> paramIndexes = infoSchemaContext.getParamIndexes();

            if (paramIndexes.get(InfoSchemaCommon.COLUMN_TABLE_SCHEMA).size() > 0
                || paramIndexes.get(InfoSchemaCommon.COLUMN_TABLE_NAME).size() > 0) {
                Map<Integer, ParameterContext> params =
                    infoSchemaQuery.getDbIndexAndParam(null, executionContext).getValue();
                if (params != null) {
                    for (Integer index : params.keySet()) {
                        ParameterContext param = params.get(index);
                        if (paramIndexes.get(InfoSchemaCommon.COLUMN_TABLE_SCHEMA).contains(index)) {
                            // Get the default physical database name and reset
                            // the parameter value.
                            String defaultDbName = getDefaultDbName((String) param.getValue(), infoSchemaContext);
                            if (TStringUtil.isNotEmpty(defaultDbName)) {
                                param.setValue(defaultDbName);
                            }
                        } else if (paramIndexes.get(InfoSchemaCommon.COLUMN_TABLE_NAME).contains(index)) {
                            // Get the default physical table name in default
                            // database and use it to reset the parameter value
                            // if the logical table has table partitions.
                            String logicalTableName = (String) param.getValue();
                            if (TStringUtil.isNotEmpty(logicalTableName)) {
                                String defaultPhysicalTableName =
                                    defaultPhyTableNamesForLogical.get(logicalTableName.toLowerCase());
                                if (TStringUtil.isNotEmpty(defaultPhysicalTableName)) {
                                    param.setValue(defaultPhysicalTableName);
                                }
                            }
                        }
                    }
                }
            }
        }

        String targetDbIndex = infoSchemaContext.getOptimizerContext().getRuleManager().getDefaultDbIndex(null);
        if (TStringUtil.isNotEmpty(targetDbIndex) && !targetDbIndex.equalsIgnoreCase(infoSchemaQuery.getDbIndex())) {
            infoSchemaQuery.setDbIndex(targetDbIndex);
        }

        // Reset LIMIT M,N or LIMIT N OFFSET M and postpone handling them.
        // NOTE that the order of parameters has been changed for offset and
        // fetch in PlannerUtils.getDynamicParamIndex(), so we first try to get
        // fetch from offset and then get offset from fetch.
        int rowCount = getAndResetDynamicParamValue(infoSchemaQuery, queryNode.getOffset());
        if (rowCount >= 0) {
            // Both offset and fetch are specified, so rowCount is actually from
            // offset and offset is from fetch.
            infoSchemaContext.setRowCount(rowCount);
            queryNode.setOffset(null);
            // Then get offset from fetch.
            int offset = getAndResetDynamicParamValue(infoSchemaQuery, queryNode.getFetch());
            infoSchemaContext.setOffset(offset >= 0 ? offset : InfoSchemaCommon.LIMIT_DEFAULT_OFFSET);
            queryNode.setFetch(null);
            // Adjust customer schema filter if have.
            adjustIndexForCustomSchemaFilter(offset >= 0 ? 2 : 1, infoSchemaQuery, infoSchemaContext);
        } else {
            // No offset is specified, so get from fetch as is.
            rowCount = getAndResetDynamicParamValue(infoSchemaQuery, queryNode.getFetch());
            infoSchemaContext.setRowCount(rowCount >= 0 ? rowCount : InfoSchemaCommon.LIMIT_NO_ROW_COUNT_SPECIFIED);
            queryNode.setFetch(null);
            // Offset is 0 by default.
            infoSchemaContext.setOffset(InfoSchemaCommon.LIMIT_NO_OFFSET_SPECIFIED);
            queryNode.setOffset(null);
            // Adjust customer schema filter if have.
            adjustIndexForCustomSchemaFilter(rowCount >= 0 ? 1 : 0, infoSchemaQuery, infoSchemaContext);
        }

        // Re-generate native sql
        infoSchemaQuery.setBytesSql(RelUtils.toNativeBytesSql(queryNode));
    }

    private void adjustIndexForCustomSchemaFilter(int count, PhyQueryOperation infoSchemaQuery,
                                                  LogicalInfoSchemaContext infoSchemaContext) {
        if (!infoSchemaContext.isCustomSchemaFilter() || count <= 0) {
            return;
        }

        // Adjust dynamic parameter index in the where clause
        SqlBasicCall whereNode = (SqlBasicCall) ((TDDLSqlSelect) infoSchemaQuery.getNativeSqlNode()).getWhere();
        SqlDynamicParam oldOperand = (SqlDynamicParam) whereNode.getOperands()[1];
        int oldDynamicParamIndex = oldOperand.getIndex();
        SqlDynamicParam newOperand = new SqlDynamicParam(oldDynamicParamIndex - count,
            oldOperand.getTypeName(),
            oldOperand.getParserPosition(),
            oldOperand.getValue());
        whereNode.setOperand(1, newOperand);

        // Adjust the parameter index
        int oldParamIndex = oldDynamicParamIndex + 1;
        Map<Integer, ParameterContext> params = infoSchemaQuery.getParam();
        ParameterContext parameterContext = params.get(oldParamIndex);
        int newParamIndex = oldParamIndex - count;
        parameterContext.setArgs(new Object[] {newParamIndex, parameterContext.getValue()});
        params.remove(oldParamIndex);
        params.put(newParamIndex, parameterContext);
    }

    protected void buildResultColumns(ArrayResultCursor resultCursor, List<ColumnMeta> columnMetas,
                                      int numOfResultCols, String countAggFuncTitle) {
        if (resultCursor.getReturnColumns().size() == 0) {
            if (numOfResultCols == 0 && TStringUtil.isNotEmpty(countAggFuncTitle)) {
                resultCursor.addColumn(countAggFuncTitle, countAggFuncTitle, DataTypes.StringType);
            } else {
                for (int i = 0; i < numOfResultCols; i++) {
                    ColumnMeta cm = columnMetas.get(i);
                    resultCursor
                        .addColumn(new ColumnMeta(cm.getTableName(), cm.getName(), cm.getAlias(), cm.getField()));
                }
            }
            resultCursor.initMeta();
        }
    }

    private int getAndResetDynamicParamValue(PhyQueryOperation infoSchemaQuery, SqlNode node) {
        int paramValue = -1;
        if (node != null && node instanceof SqlDynamicParam) {
            int offsetParamIndex = ((SqlDynamicParam) node).getIndex() + 1;
            paramValue = (int) infoSchemaQuery.getParam().get(offsetParamIndex).getValue();
            // Remove the dynamic parameter and postpone handling it.
            infoSchemaQuery.getParam().remove(offsetParamIndex);
        }
        return paramValue;
    }

    // Collect the default physical table name for each logical table
    private Map<String, String> collectDefaultPhyTableNames(LogicalInfoSchemaContext infoSchemaContext) {
        Map<String, String> defaultPhyTableNameForLogical = new HashMap<>();

        String defaultDbIndex = infoSchemaContext.getOptimizerContext().getRuleManager().getDefaultDbIndex(null);

        for (String logicalTableName : infoSchemaContext.getLogicalTableNames()) {
            TableRule tableRule =
                infoSchemaContext.getOptimizerContext().getRuleManager().getTableRule(logicalTableName);

            // Exclude table name of secondary index and system table name
            if (TStringUtil.contains(logicalTableName, InfoSchemaCommon.KEYWORD_SECONDARY_INDEX)
                || SystemTables.contains(logicalTableName)) {
                continue;
            }

            if (tableRule != null && tableRule.getActualTopology() != null
                && tableRule.getActualTopology().values().size() > 0) {
                for (Map.Entry entry : tableRule.getActualTopology().entrySet()) {
                    // We only need table names from the default database
                    if (TStringUtil.equalsIgnoreCase((String) entry.getKey(), defaultDbIndex)) {
                        Set<String> physicalTablesInDefaultDB = (Set<String>) entry.getValue();
                        if (physicalTablesInDefaultDB != null && physicalTablesInDefaultDB.size() > 0) {
                            String physicalTableName = physicalTablesInDefaultDB.iterator().next();
                            defaultPhyTableNameForLogical.put(logicalTableName.toLowerCase(), physicalTableName);
                        }
                        break;
                    }
                }
            }
        }

        return defaultPhyTableNameForLogical;
    }

    // Add extra columns in case user doesn't specify them that we rely on.
    private void addExtraColumns(TDDLSqlSelect infoSchemaQuery, LogicalInfoSchemaContext infoSchemaContext) {
        int numOfColsAdded = 0;

        List<SqlNode> selectList = infoSchemaQuery.getSelectList().getList();

        checkAndRemoveCountAggFunction(selectList, infoSchemaQuery, infoSchemaContext);

        selectList.add(new SqlBasicCall(new SqlAsOperator(), new SqlNode[] {
            new SqlIdentifier(InfoSchemaCommon.COLUMN_TABLE_SCHEMA, SqlParserPos.ZERO),
            new SqlIdentifier(InfoSchemaCommon.COLUMN_TABLE_SCHEMA, SqlParserPos.ZERO)}, SqlParserPos.ZERO));
        selectList.add(new SqlBasicCall(new SqlAsOperator(), new SqlNode[] {
            new SqlIdentifier(InfoSchemaCommon.COLUMN_TABLE_NAME, SqlParserPos.ZERO),
            new SqlIdentifier(InfoSchemaCommon.COLUMN_TABLE_NAME, SqlParserPos.ZERO)}, SqlParserPos.ZERO));
        numOfColsAdded += 2;

        String tableName = "";
        SqlNode fromNode = infoSchemaQuery.getFrom();
        if (fromNode instanceof SqlIdentifier) {
            tableName = ((SqlIdentifier) fromNode).getLastName();
        } else if (fromNode instanceof SqlBasicCall) {
            SqlOperator operator = ((SqlBasicCall) fromNode).getOperator();
            if (operator instanceof SqlAsOperator) {
                SqlNode[] operands = ((SqlBasicCall) fromNode).getOperands();
                if (operands[0] instanceof SqlIdentifier) {
                    tableName = ((SqlIdentifier) operands[0]).getLastName();
                }
            }
        }

        List<String> specificColumnNames = new ArrayList<>();
        switch (tableName.toUpperCase()) {
        case InfoSchemaCommon.INFO_SCHEMA_TABLES:
            specificColumnNames.add(InfoSchemaCommon.COLUMN_TABLE_TYPE);
            break;
        case InfoSchemaCommon.INFO_SCHEMA_COLUMNS:
            specificColumnNames.add(InfoSchemaCommon.COLUMN_COLUMN_NAME);
            break;
        case InfoSchemaCommon.INFO_SCHEMA_STATISTICS:
            specificColumnNames.add(InfoSchemaCommon.COLUMN_INDEX_NAME);
            specificColumnNames.add(InfoSchemaCommon.COLUMN_COLUMN_NAME);
            break;
        default:
            break;
        }

        if (!specificColumnNames.isEmpty()) {
            for (String specificColumnName : specificColumnNames) {
                selectList.add(new SqlBasicCall(new SqlAsOperator(), new SqlNode[] {
                    new SqlIdentifier(specificColumnName, SqlParserPos.ZERO),
                    new SqlIdentifier(specificColumnName, SqlParserPos.ZERO)}, SqlParserPos.ZERO));
                numOfColsAdded++;
            }
        }

        infoSchemaContext.setNumOfAddedCols(numOfColsAdded);
    }

    protected void checkAndRemoveCountAggFunction(List<SqlNode> selectList, TDDLSqlSelect infoSchemaQuery,
                                                  LogicalInfoSchemaContext infoSchemaContext) {
        // If the aggregate function COUNT() exists, we have to use normal
        // columns instead as select list to push down, and then compute it
        // according to returned rows.
        // Note that we only support single COUNT() function in the select list
        // without any GROUP BY and HAVING clauses.
        if (selectList.size() == 1 && selectList.get(0) instanceof SqlBasicCall && infoSchemaQuery.getGroup() == null
            && infoSchemaQuery.getHaving() == null) {
            SqlBasicCall sqlNode = (SqlBasicCall) selectList.get(0);
            SqlNode[] operands = sqlNode.getOperands();
            if (operands[0] instanceof SqlBasicCall
                && ((SqlBasicCall) operands[0]).getOperator() instanceof SqlCountAggFunction
                && operands[1] instanceof SqlIdentifier) {
                // Remove the COUNT() aggregate function and use the following
                // customized columns instead.
                infoSchemaContext.setCountAggFuncTitle(((SqlIdentifier) operands[1]).getLastName());
                selectList.remove(0);
            }
        }

    }

    protected void replaceResultForCountAggFunction(ArrayResultCursor resultCursor,
                                                    LogicalInfoSchemaContext infoSchemaContext) {
        if (infoSchemaContext != null && TStringUtil.isNotEmpty(infoSchemaContext.getCountAggFuncTitle())) {
            // We have to compute the count of returned rows as result.
            int rowCount = resultCursor.getRows().size();
            resultCursor.getRows().clear();
            resultCursor.addRow(new Object[] {rowCount});
        }
    }

    // Rewrite operator and record the param index if needed.
    protected void findSchemaAndReplaceNameOperator(SqlBasicCall filterNode, Map<String, Set<Integer>> paramIndexes) {
        SqlOperator operator = filterNode.getOperator();
        SqlNode[] operands = filterNode.getOperands();
        for (int i = 0; i < operands.length; i++) {
            if (operator instanceof SqlBinaryOperator && operands[i] instanceof SqlIdentifier) {
                String paramName = ((SqlIdentifier) operands[i]).getLastName();
                if (TStringUtil.equalsIgnoreCase(paramName, InfoSchemaCommon.COLUMN_TABLE_SCHEMA)
                    || TStringUtil.equalsIgnoreCase(paramName, InfoSchemaCommon.COLUMN_TABLE_NAME)
                    || TStringUtil.equalsIgnoreCase(paramName, InfoSchemaCommon.COLUMN_SCHEMA_NAME)) {
                    switch (operator.getKind()) {
                    case EQUALS:
                        if (operands[i + 1] instanceof SqlDynamicParam) {
                            int paramIndex = ((SqlDynamicParam) operands[++i]).getIndex() + 1;
                            paramIndexes.get(paramName.toUpperCase()).add(paramIndex);
                        }
                        break;
                    case IN:
                    case NOT_IN:
                        if (operands[i + 1] instanceof SqlNodeList) {
                            List<SqlNode> sqlNodes = ((SqlNodeList) operands[i + 1]).getList();
                            for (SqlNode sqlNode : sqlNodes) {
                                if (sqlNode instanceof SqlDynamicParam) {
                                    int paramIndex = ((SqlDynamicParam) sqlNode).getIndex() + 1;
                                    paramIndexes.get(paramName.toUpperCase()).add(paramIndex);
                                }
                            }
                        }
                        break;
                    default:
                        // Do nothing
                        break;
                    }
                }
            } else if (operands[i] instanceof SqlBasicCall) {
                findSchemaAndReplaceNameOperator((SqlBasicCall) operands[i], paramIndexes);
            }
        }
    }

    // Get the default physical database name.
    protected String getDefaultDbName(String schemaName, LogicalInfoSchemaContext infoSchemaContext) {
        LogicalInfoSchemaContext tmpInfoSchemaContext = infoSchemaContext;

        if (TStringUtil.isNotEmpty(schemaName)) {
            // Build a temporary context for specified schema
            ExecutionContext tmpEc = infoSchemaContext.getExecutionContext().copy();
            tmpEc.setSchemaName(schemaName);

            tmpInfoSchemaContext = new LogicalInfoSchemaContext(tmpEc);
            tmpInfoSchemaContext.prepareContextAndRepository(infoSchemaContext.getRealRepo());
        }

        // Get the physical database information
        return getDefaultDbName(tmpInfoSchemaContext);
    }

    // Get the default physical database name.
    protected String getDefaultDbName(LogicalInfoSchemaContext infoSchemaContext) {
        TGroupDataSource defaultGroupDataSource = getGroupDataSource(null, infoSchemaContext);
        TAtomDsConfDO runTimeConf = getAtomRuntimeConfig(defaultGroupDataSource);
        return runTimeConf != null ? runTimeConf.getDbName() : null;
    }

    protected int getFirstColumnIndex(List<ColumnMeta> columnMetas, String columnName) {
        if (columnMetas == null || columnMetas.isEmpty() || TStringUtil.isEmpty(columnName)) {
            return -1;
        }
        for (int i = 0; i < columnMetas.size(); i++) {
            if (columnName.equalsIgnoreCase(columnMetas.get(i).getName())) {
                return i;
            }
        }
        return -1;
    }

    protected void handleColumnAlias(TDDLSqlSelect queryNode, LogicalInfoSchemaContext infoSchemaContext) {
        // Handle the case in which the aliases of some columns are different
        // from their original names.
        SqlNodeList selectList = queryNode.getSelectList();
        if (selectList != null && selectList.getList() != null) {
            for (SqlNode sqlNode : selectList.getList()) {
                if (sqlNode instanceof SqlBasicCall && ((SqlBasicCall) sqlNode)
                    .getOperator() instanceof SqlAsOperator) {
                    SqlNode[] operands = ((SqlBasicCall) sqlNode).getOperands();
                    if (operands.length == 2 && operands[0] instanceof SqlIdentifier
                        && operands[1] instanceof SqlIdentifier) {
                        String origName = ((SqlIdentifier) operands[0]).getLastName();
                        String alias = ((SqlIdentifier) operands[1]).getLastName();
                        if (TStringUtil.isNotEmpty(origName) && TStringUtil.isNotEmpty(alias)
                            && !origName.equalsIgnoreCase(alias)
                            && InfoSchemaCommon.COLUMN_ALIAS_MAPPING.contains(origName.toUpperCase())) {
                            infoSchemaContext.getOrigToAliasMapping().put(origName.toUpperCase(), alias.toUpperCase());
                            infoSchemaContext.getAliasToOrigMapping().put(alias.toUpperCase(), origName.toUpperCase());
                        }
                    }
                }
            }
        }
    }

    // Collect statistics for single logical table.
    protected Object[] showSingleTableStatus(String logicalTableName, LogicalInfoSchemaContext infoSchemaContext) {
        String engine = null, rowFormat = null;
        long version = 0L, autoIncrement;
        BigDecimal rows = BigDecimal.ZERO, avgRowLength = BigDecimal.ZERO;
        BigDecimal dataLength = BigDecimal.ZERO, maxDataLength = BigDecimal.ZERO;
        BigDecimal indexLength = BigDecimal.ZERO, dataFree = BigDecimal.ZERO;
        BigDecimal totalLength = BigDecimal.ZERO;
        String createTime = null, updateTime = null, checkTime = null, collation = null;
        String checksum = null, createOptions = null, comment = null;

        // Get next sequence value if the corresponding sequence exists.
        if (ConfigDataMode.isFastMock()) {
            autoIncrement = 100;
        } else {
            autoIncrement = checkNextSeqValue(logicalTableName, infoSchemaContext);
        }

        // Get the logical table topology:
        // { group name, [ physical table name ] }
        Map<String, Set<String>> tableTopology = getLogicalTableTopology(logicalTableName, infoSchemaContext);

        // Organize groups by instance:
        // { ip:port, [ < group name, database name > ] }
        Map<String, Set<Pair<String, String>>> groupsByInstance = organizeGroupsByInstance(tableTopology,
            infoSchemaContext);

        // Generate executable groups and filters:
        // { group name, < ip:port, physical table filter > }
        Map<String, Pair<String, String>> groupsAndFilters = generateExecutableGroups(tableTopology,
            groupsByInstance,
            infoSchemaContext.getExecutionContext());

        boolean shouldCheckView = true;

        for (Entry<String, Pair<String, String>> groupAndFilter : groupsAndFilters.entrySet()) {
            TGroupDataSource groupDataSource = (TGroupDataSource) infoSchemaContext.getRealRepo()
                .getDataSource(groupAndFilter.getKey());

            try (Connection conn = groupDataSource.getConnection()) {
                String collectSql = getCollectSql(groupAndFilter.getValue(),
                    groupsByInstance,
                    infoSchemaContext.getExecutionContext());

                try (PreparedStatement ps = conn.prepareStatement(collectSql); ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        shouldCheckView = false;
                        engine = rs.getString("Engine");
                        version = rs.getLong("Version");
                        rowFormat = rs.getString("Row_format");

                        BigDecimal newRows = getDecimalValue(rs, "Rows");
                        rows = rows.add(newRows);
                        totalLength = totalLength.add(newRows.multiply(getDecimalValue(rs, "Avg_row_length")));

                        dataLength = dataLength.add(getDecimalValue(rs, "Data_length"));

                        BigDecimal newMaxDataLength = getDecimalValue(rs, "Max_data_length");
                        maxDataLength =
                            maxDataLength.compareTo(newMaxDataLength) < 0 ? newMaxDataLength : maxDataLength;

                        indexLength = indexLength.add(getDecimalValue(rs, "Index_length"));
                        dataFree = getDecimalValue(rs, "Data_free");

                        if (autoIncrement < 0) {
                            autoIncrement = rs.getLong("Auto_increment");
                        }

                        createTime = rs.getString("Create_time");
                        updateTime = rs.getString("Update_time");
                        checkTime = rs.getString("Check_time");
                        collation = rs.getString("Collation");
                        checksum = rs.getString("Checksum");
                        createOptions = rs.getString("Create_options");
                        comment = rs.getString("Comment");
                    }
                }
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e, e.getMessage());
            }
        }

        if (rows.compareTo(BigDecimal.ZERO) > 0) {
            avgRowLength = totalLength.divide(rows, 0, RoundingMode.HALF_UP);
        }

        if (shouldCheckView) {
            if (infoSchemaContext.getOptimizerContext().getViewManager().select(logicalTableName) != null) {
                comment = "VIEW";
            }
        }

        return new Object[] {
            logicalTableName, engine, version, rowFormat, rows, avgRowLength, dataLength,
            maxDataLength, indexLength, dataFree, autoIncrement, createTime, updateTime, checkTime, collation,
            checksum, createOptions, comment};
    }

    private BigDecimal getDecimalValue(ResultSet rs, String columnName) throws SQLException {
        BigDecimal value = rs.getBigDecimal(columnName);
        return value != null ? value : BigDecimal.ZERO;
    }

    private TGroupDataSource getGroupDataSource(String groupName, LogicalInfoSchemaContext infoSchemaContext) {
        TGroupDataSource defaultGroupDataSource = null;

        OptimizerContext optimizerContext = infoSchemaContext.getOptimizerContext();

        if (TStringUtil.isEmpty(groupName)) {
            groupName = optimizerContext.getRuleManager().getDefaultDbIndex(null);
        }

        DataSource dataSource = infoSchemaContext.getRealRepo().getDataSource(groupName);

        defaultGroupDataSource = (TGroupDataSource) dataSource;
        return defaultGroupDataSource;
    }

    private TAtomDsConfDO getAtomRuntimeConfig(TGroupDataSource groupDataSource) {
        if (groupDataSource != null && groupDataSource.getConfigManager() != null) {
            TAtomDataSource atomDataSource = groupDataSource.getConfigManager().getDataSource(MasterSlave.MASTER_ONLY);
            if (atomDataSource != null && atomDataSource.getDsConfHandle() != null) {
                return atomDataSource.getDsConfHandle().getRunTimeConf();
            }
        }
        return null;
    }

    // { group name, [ physical table name ] }
    private Map<String, Set<String>> getLogicalTableTopology(String logicalTableName,
                                                             LogicalInfoSchemaContext infoSchemaContext) {
        Map<String, Set<String>> topology = null;

        TddlRuleManager tddlRuleManager = infoSchemaContext.getOptimizerContext().getRuleManager();

        if (tddlRuleManager.getPartitionInfoManager().isNewPartDbTable(logicalTableName)) {
            topology = tddlRuleManager.getPartitionInfoManager().getPartitionInfo(logicalTableName).getTopology();
        }

        if (tddlRuleManager.getTableRule(logicalTableName) != null) {
            topology = tddlRuleManager.getTableRule(logicalTableName).getActualTopology();
        }

        if (topology == null) {
            topology = new HashMap(1);
            Set<String> groupTopology = new HashSet(1);
            groupTopology.add(logicalTableName);
            topology.put(tddlRuleManager.getDefaultDbIndex(logicalTableName), groupTopology);
        }

        return topology;
    }

    // { ip:port, [ < group name, database name > ] }
    private Map<String, Set<Pair<String, String>>> organizeGroupsByInstance(Map<String, Set<String>> topology,
                                                                            LogicalInfoSchemaContext infoSchemaContext) {
        // Put groups together by instance.
        Map<String, Set<Pair<String, String>>> groupsByInstance = new HashMap<>();

        for (String groupName : topology.keySet()) {
            TGroupDataSource groupDataSource = getGroupDataSource(groupName, infoSchemaContext);
            TAtomDsConfDO runTimeConf = getAtomRuntimeConfig(groupDataSource);
            if (runTimeConf != null) {
                String instance = runTimeConf.getIp() + ":" + runTimeConf.getPort();
                String dbName = runTimeConf.getDbName();
                if (!groupsByInstance.containsKey(instance)) {
                    groupsByInstance.put(instance,
                        new TreeSet<>((p1, p2) -> TStringUtil.compareTo(p1.getKey(), p2.getKey())));
                }
                groupsByInstance.get(instance).add(new Pair<>(groupName, dbName));
            }
        }

        return groupsByInstance;
    }

    // { group name, < ip:port, physical table filter > }
    private Map<String, Pair<String, String>> generateExecutableGroups(Map<String, Set<String>> topology,
                                                                       Map<String, Set<Pair<String, String>>> groupsByInstance,
                                                                       ExecutionContext executionContext) {
        Map<String, Pair<String, String>> executableGroups = new HashMap<>();

        // Generate executable groups and their filters.
        boolean collectByGroup = executionContext.getParamManager()
            .getBoolean(ConnectionParams.INFO_SCHEMA_QUERY_STAT_BY_GROUP);

        for (String instance : groupsByInstance.keySet()) {
            StringBuffer physicalTableListByInstance = new StringBuffer();

            Set<Pair<String, String>> groupsAndDatabases = groupsByInstance.get(instance);

            for (Pair<String, String> groupAndDatabase : groupsAndDatabases) {
                StringBuffer physicalTableListByGroup = new StringBuffer();

                for (String physicalTable : topology.get(groupAndDatabase.getKey())) {
                    physicalTableListByGroup.append("'").append(physicalTable).append("',");
                }

                if (collectByGroup) {
                    physicalTableListByGroup.deleteCharAt(physicalTableListByGroup.length() - 1);
                    executableGroups.put(groupAndDatabase.getKey(),
                        new Pair<>(instance, physicalTableListByGroup.toString()));
                } else {
                    physicalTableListByInstance.append(physicalTableListByGroup);
                }
            }

            if (!collectByGroup) {
                physicalTableListByInstance.deleteCharAt(physicalTableListByInstance.length() - 1);
                executableGroups.put(groupsAndDatabases.iterator().next().getKey(), new Pair<>(instance,
                    physicalTableListByInstance.toString()));
            }
        }

        return executableGroups;
    }

    private String getCollectSql(Pair<String, String> instanceAndFilter,
                                 Map<String, Set<Pair<String, String>>> groupsByInstance,
                                 ExecutionContext executionContext) {
        if (executionContext.getParamManager().getBoolean(ConnectionParams.INFO_SCHEMA_QUERY_STAT_BY_GROUP)) {
            return "SHOW TABLE STATUS WHERE NAME IN (" + instanceAndFilter.getValue() + ")";
        } else {
            StringBuilder sql = new StringBuilder();

            sql.append("SELECT ");
            for (int i = 0; i < SqlShowTableStatus.NUM_OF_COLUMNS; i++) {
                String showColumnName = SqlShowTableStatus.COLUMN_NAMES.get(i);
                String infoColumnName = InfoSchemaCommon.TABLES_COLUMN_MAPPING_REVERSED.get(showColumnName);
                if (InfoSchemaCommon.TABLES_SPECIAL_COLUMNS.keySet().contains(showColumnName)) {
                    sql.append(infoColumnName).append(" AS '").append(showColumnName).append("'");
                } else {
                    sql.append(showColumnName);
                }
                sql.append(",");
            }
            sql.deleteCharAt(sql.length() - 1);

            sql.append(" FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN (");

            StringBuilder filter = new StringBuilder();
            Set<Pair<String, String>> groupsAndDatabases = groupsByInstance.get(instanceAndFilter.getKey());
            for (Pair<String, String> groupAndDatabase : groupsAndDatabases) {
                filter.append("'").append(groupAndDatabase.getValue()).append("',");
            }
            filter.deleteCharAt(filter.length() - 1);
            sql.append(filter);

            sql.append(") AND TABLE_NAME IN (").append(instanceAndFilter.getValue()).append(")");

            return sql.toString();
        }
    }

    private long checkNextSeqValue(String logicalTableName, LogicalInfoSchemaContext infoSchemaContext) {
        String seqName = SequenceAttribute.AUTO_SEQ_PREFIX + logicalTableName;
        Type seqType = infoSchemaContext.getExecutorContext().getSequenceManager()
            .checkIfExists(infoSchemaContext.getTargetSchema(), seqName);
        return checkNextSeqValueForGMS(seqName, seqType, infoSchemaContext);
    }

    private long checkNextSeqValueForGMS(String seqName, Type seqType, LogicalInfoSchemaContext infoSchemaContext) {
        long autoIncrement = 0;
        String schemaName = infoSchemaContext.getTargetSchema();
        if (seqType != Type.NA) {
            switch (seqType) {
            case GROUP:
                try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                    SequenceAccessor seqAccessor = new SequenceAccessor();
                    seqAccessor.setConnection(metaDbConn);
                    SequenceRecord record = seqAccessor.query(schemaName, seqName);
                    if (record != null) {
                        autoIncrement = record.value + (record.unitCount + record.unitIndex) * record.innerStep;
                    }
                } catch (Exception e) {
                    logger.error("Failed to get next value for GROUP sequence '" + seqName, e);
                }
                break;
            case SIMPLE:
                try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                    SequenceOptAccessor seqOptAccessor = new SequenceOptAccessor();
                    seqOptAccessor.setConnection(metaDbConn);
                    SequenceOptRecord record = seqOptAccessor.query(schemaName, seqName);
                    if (record != null) {
                        autoIncrement = record.value;
                    }
                } catch (Exception e) {
                    logger.error("Failed to get next value for SIMPLE sequence '" + seqName, e);
                }
                break;
            default:
                break;
            }
        }
        return autoIncrement;
    }
}
