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

package com.alibaba.polardbx.executor.ddl.util;

import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.ZeroDate;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.changeset.ChangeSetManager;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyFinishTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetCatchUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetStartTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterComplexTaskUpdateJobStatusTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TablesMetaChangePreemptiveSyncAction;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.BooleanType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import com.alibaba.polardbx.optimizer.core.datatype.FloatType;
import com.alibaba.polardbx.optimizer.core.datatype.TinyIntType;
import com.alibaba.polardbx.optimizer.core.datatype.UTinyIntType;
import com.alibaba.polardbx.optimizer.core.datatype.YearType;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.rel.BuildInsertValuesVisitor;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.common.properties.ConnectionParams.CHANGE_SET_APPLY_OPTIMIZATION;
import static com.alibaba.polardbx.common.properties.ConnectionParams.CHANGE_SET_CHECK_TWICE;
import static com.alibaba.polardbx.executor.columns.ColumnBackfillExecutor.isAllDnUseXDataSource;

public class ChangeSetUtils {
    public final static String SQL_START_CHANGESET = "call polarx.changeset_start(%s, %s);";
    public final static String SQL_FETCH_CHANGESET_TIMES = "call polarx.changeset_times(%s);";
    public final static String SQL_FETCH_CAHNGESET = "call polarx.changeset_fetch(%s, %d);";
    public final static String SQL_FINISH_CHANGESET = "call polarx.changeset_finish(%s);";
    public final static String SQL_CALL_CHANGESET_STATS = "call polarx.changeset_stats('');";

    public static final int RETRY_COUNT = 10;
    public static final long[] RETRY_WAIT = new long[RETRY_COUNT];

    static {
        IntStream.range(0, RETRY_COUNT).forEach(i -> RETRY_WAIT[i] = 500L * i);
    }

    public static boolean isChangeSetProcedure(ExecutionContext ec) {
        return ec.getParamManager().getBoolean(ConnectionParams.CN_ENABLE_CHANGESET)
            && ExecutorContext.getContext(ec.getSchemaName()).getStorageInfoManager().supportChangeSet();
    }

    public static void execGroup(ExecutionContext ec, String schema, String groupName, String sql) {
        queryGroup(ec, schema, groupName, sql);
    }

    /**
     * Execute SQL on a physical group, used in change-set management
     */
    public static List<List<Object>> queryGroup(ExecutionContext ec, String schema, String groupName, String sql) {
        List<List<Object>> result = new ArrayList<>();
        if (ec.getParamManager().getBoolean(ConnectionParams.SKIP_CHANGE_SET)) {
            return result;
        }

        ExecutorContext executorContext = ExecutorContext.getContext(schema);
        IGroupExecutor ge = executorContext.getTopologyExecutor().getGroupExecutor(groupName);

        try (Connection conn = ge.getDataSource().getConnection()) {
            Statement stmt = conn.createStatement();
            try (ResultSet rs = stmt.executeQuery(sql)) {
                int columns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= columns; i++) {
                        row.add(rs.getObject(i));
                    }
                    result.add(row);
                }
            }
            return result;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(
                String.format("failed to execute on group(%s): %s , Caused by: %s", groupName, sql, e.getMessage()), e);
        }
    }

    public static CursorMeta buildCursorMeta(String schemaName, String tableName) {
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta tableMeta = sm.getTable(tableName);
        List<String> primaryKeys = GlobalIndexMeta.getPrimaryKeysNotOrdered(tableMeta).getKey();
        List<ColumnMeta> columnMetas = primaryKeys.stream().map(tableMeta::getColumn).collect(Collectors.toList());
        return CursorMeta.build(columnMetas);
    }

    public static PhyTableOperation buildSelectWithInPk(String schemaName, String tableName,
                                                        String grpKey, String phyTbName,
                                                        List<String> tableColumns,
                                                        List<String> notUsingBinaryStringColumns,
                                                        List<Row> data, ExecutionContext ec, boolean lock) {
        if (data.isEmpty()) {
            return null;
        }
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta baseTableMeta = sm.getTable(tableName);

        Pair<List<String>, List<Integer>> primaryKeys = GlobalIndexMeta.getPrimaryKeysNotOrdered(baseTableMeta);

        final boolean useBinary = ec.getParamManager().getBoolean(ConnectionParams.BACKFILL_USING_BINARY);
        final PhysicalPlanBuilder builder =
            new PhysicalPlanBuilder(schemaName, useBinary, notUsingBinaryStringColumns, ec);
        final Pair<SqlSelect, PhyTableOperation> selectWithInPk = builder
            .buildSelectWithInForChecker(baseTableMeta, tableColumns, primaryKeys.getKey(), "PRIMARY");

        final Map<Integer, ParameterContext> planParams = new HashMap<>(1);
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTbName, 1));

        SqlNodeList inValues = buildInValues(primaryKeys, data);

        BytesSql sql;
        SqlSelect planSelectWithInTemplate = selectWithInPk.getKey();
        PhyTableOperation targetPhyOp = selectWithInPk.getValue();
        // Generate template.
        SqlSelect.LockMode lockMode = lock ? SqlSelect.LockMode.SHARED_LOCK : SqlSelect.LockMode.UNDEF;
        planSelectWithInTemplate.setLockMode(lockMode);
        ((SqlBasicCall) planSelectWithInTemplate.getWhere()).getOperands()[1] = inValues;
        sql = RelUtils.toNativeBytesSql(planSelectWithInTemplate);

        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(grpKey);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTbName)));
        buildParams.setDynamicParams(planParams);
        buildParams.setBytesSql(sql);
        buildParams.setLockMode(lockMode);
        buildParams.setSchemaName(schemaName);

        return PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);
    }

    public static PhyTableOperation buildDeleteWithInPk(String schemaName, String tableName,
                                                        String grpKey, String phyTbName,
                                                        List<Row> data,
                                                        ExecutionContext ec) {
        if (data.isEmpty()) {
            return null;
        }
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta baseTableMeta = sm.getTable(tableName);

        Pair<List<String>, List<Integer>> primaryKeys = GlobalIndexMeta.getPrimaryKeysNotOrdered(baseTableMeta);

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);
        final Pair<SqlDelete, PhyTableOperation> deleteWithInPk =
            builder.buildDeleteForChangeSet(baseTableMeta, primaryKeys.getKey());

        final Map<Integer, ParameterContext> planParams = new HashMap<>(1);
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTbName, 1));

        SqlNodeList inValues = buildInValues(primaryKeys, data);

        BytesSql sql;
        SqlDelete planDeleteWithInTemplate = deleteWithInPk.getKey();
        PhyTableOperation targetPhyOp = deleteWithInPk.getValue();
        // Generate template.
        ((SqlBasicCall) planDeleteWithInTemplate.getCondition()).getOperands()[1] = inValues;
        sql = RelUtils.toNativeBytesSql(planDeleteWithInTemplate);

        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(grpKey);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTbName)));
        buildParams.setDynamicParams(planParams);
        buildParams.setBytesSql(sql);
        buildParams.setSchemaName(schemaName);

        return PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);
    }

    public static SqlNodeList buildInValues(Pair<List<String>, List<Integer>> primaryKeys, List<Row> RowList) {
        final SqlNodeList inValues = new SqlNodeList(SqlParserPos.ZERO);
        for (Row row : RowList) {
            List<Pair<ParameterContext, byte[]>> rows = row2objects(row);
            // Generate in XXX.
            List<Integer> appearedKeysId = primaryKeys.getValue();
            if (1 == appearedKeysId.size()) {
                final int pkIndex = appearedKeysId.get(0);
                inValues.add(ChangeSetUtils.value2node(rows.get(pkIndex).getKey().getValue()));
            } else {
                final SqlNode[] rowSet = appearedKeysId.stream().mapToInt(idx -> idx)
                    .mapToObj(i -> rows.get(i).getKey().getValue())
                    .map(ChangeSetUtils::value2node)
                    .toArray(SqlNode[]::new);
                inValues.add(new SqlBasicCall(SqlStdOperatorTable.ROW, rowSet, SqlParserPos.ZERO));
            }
        }
        return inValues;
    }

    static public PhyTableOperation buildReplace(String schemaName,
                                                 String tableName, String indexName,
                                                 String grpKey, String phyTbName,
                                                 List<String> tableColumns,
                                                 Parameters parameters,
                                                 ExecutionContext ec) {
        int dataSize = parameters.getBatchParameters().size();
        if (dataSize <= 0) {
            return null;
        }
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta baseTableMeta = indexName == null ? sm.getTable(tableName) : sm.getTable(indexName);

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);
        final Pair<SqlInsert, PhyTableOperation> replace =
            builder.buildReplaceForChangeSet(baseTableMeta, tableColumns);

        final Map<Integer, ParameterContext> planParams = new HashMap<>(1);
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTbName, 1));

        BytesSql sql;
        PhyTableOperation targetPhyOp = replace.getValue();

        List<Integer> valueIndices = new ArrayList<>();
        for (int i = 0; i < dataSize; ++i) {
            valueIndices.add(i);
        }
        BuildInsertValuesVisitor visitor = new BuildInsertValuesVisitor(
            valueIndices,
            parameters,
            planParams);
        SqlInsert planReplace = visitor.visit(replace.getKey());

        // Generate template.
        sql = RelUtils.toNativeBytesSql(planReplace);

        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(grpKey);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTbName)));
        buildParams.setDynamicParams(planParams);
        buildParams.setBytesSql(sql);
        buildParams.setSchemaName(schemaName);

        return PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);
    }

    public static Parameters executePhySelectPlan(PhyTableOperation selectPlan,
                                                  List<String> notConvertColumns,
                                                  ExecutionContext ec) {
        Cursor cursor = null;
        boolean useBinary = ec.getParamManager().getBoolean(ConnectionParams.BACKFILL_USING_BINARY);
        Parameters parameters = new Parameters();
        final List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();
        try {
            cursor = ExecutorHelper.executeByCursor(selectPlan, ec, false);
            Row row;
            while (cursor != null && (row = cursor.next()) != null) {
                final List<ColumnMeta> columns = row.getParentCursorMeta().getColumns();
                final Map<Integer, ParameterContext> item = new HashMap<>(columns.size());

                for (int i = 0; i < columns.size(); i++) {
                    ColumnMeta columnMeta = columns.get(i);
                    String colName = columnMeta.getName();
                    boolean canConvert =
                        useBinary && (notConvertColumns == null || !notConvertColumns.contains(colName));

                    item.put(i + 1, Transformer.buildColumnParam(row, i, canConvert));
                }
                batchParams.add(item);
            }
        } finally {
            if (cursor != null) {
                cursor.close(new ArrayList<>());
            }
        }
        parameters.setBatchParams(batchParams);
        return parameters;
    }

    public static int executePhyWritePlan(PhyTableOperation writePlan, ExecutionContext ec) {
        int affectRows = 0;
        Cursor cursor = null;
        try {
            cursor = ExecutorHelper.executeByCursor(writePlan, ec, false);
            affectRows = ExecUtils.getAffectRowsByCursor(cursor);
        } finally {
            if (cursor != null) {
                cursor.close(new ArrayList<>());
            }
        }
        return affectRows;
    }

    private static SqlNode value2node(Object value) {
        if (value instanceof Boolean) {
            return SqlLiteral.createBoolean(DataTypes.BooleanType.convertFrom(value) == 1, SqlParserPos.ZERO);
        } else {
            final String strValue = DataTypes.StringType.convertFrom(value);
            if (value instanceof Number) {
                return SqlLiteral.createExactNumeric(strValue, SqlParserPos.ZERO);
            } else if (value instanceof byte[]) {
                return SqlLiteral.createBinaryString((byte[]) value, SqlParserPos.ZERO);
            } else {
                return SqlLiteral.createCharString(strValue, SqlParserPos.ZERO);
            }
        }
    }

    public static List<Pair<ParameterContext, byte[]>> row2objects(Row rowSet) {
        final List<ColumnMeta> columns = rowSet.getParentCursorMeta().getColumns();
        final List<Pair<ParameterContext, byte[]>> result = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            result.add(new Pair<>(Transformer.buildColumnParam(rowSet, i), rowSet.getBytes(i)));
        }
        return result;
    }

    public static String buildNextDigestByTableMeta(TableMeta tableMeta) {
        String tableName = tableMeta.getTableName();
        String schemaName = tableMeta.getSchemaName();
        long version = tableMeta.getVersion() + 1;
        return schemaName + "." + tableName + "#version:" + version;
    }

    public static Map<String, ChangeSetCatchUpTask> genChangeSetCatchUpTasks(String schemaName,
                                                                             String tableName, String indexName,
                                                                             Map<String, Set<String>> sourcePhyTableNames,
                                                                             Map<String, String> targetTableLocations,
                                                                             ComplexTaskMetaManager.ComplexTaskType taskType,
                                                                             Long changeSetId) {
        Map<String, ChangeSetCatchUpTask> catchUpTasks = new HashMap<>(6);
        catchUpTasks.put(
            ChangeSetManager.ChangeSetCatchUpStatus.ABSENT.toString(),
            new ChangeSetCatchUpTask(
                schemaName,
                tableName,
                indexName,
                sourcePhyTableNames,
                targetTableLocations,
                ChangeSetManager.ChangeSetCatchUpStatus.ABSENT,
                taskType,
                changeSetId,
                false
            ));

        catchUpTasks.put(
            ChangeSetManager.ChangeSetCatchUpStatus.DELETE_ONLY.toString(),
            new ChangeSetCatchUpTask(
                schemaName,
                tableName,
                indexName,
                sourcePhyTableNames,
                targetTableLocations,
                ChangeSetManager.ChangeSetCatchUpStatus.DELETE_ONLY,
                taskType,
                changeSetId,
                false
            ));

        catchUpTasks.put(
            ChangeSetManager.ChangeSetCatchUpStatus.WRITE_ONLY.toString(),
            new ChangeSetCatchUpTask(
                schemaName,
                tableName,
                indexName,
                sourcePhyTableNames,
                targetTableLocations,
                ChangeSetManager.ChangeSetCatchUpStatus.WRITE_ONLY,
                taskType,
                changeSetId,
                false
            ));

        catchUpTasks.put(
            ChangeSetManager.ChangeSetCatchUpStatus.ABSENT_FINAL.toString(),
            new ChangeSetCatchUpTask(
                schemaName,
                tableName,
                indexName,
                sourcePhyTableNames,
                targetTableLocations,
                ChangeSetManager.ChangeSetCatchUpStatus.ABSENT_FINAL,
                taskType,
                changeSetId,
                true
            ));

        catchUpTasks.put(
            ChangeSetManager.ChangeSetCatchUpStatus.DELETE_ONLY_FINAL.toString(),
            new ChangeSetCatchUpTask(
                schemaName,
                tableName,
                indexName,
                sourcePhyTableNames,
                targetTableLocations,
                ChangeSetManager.ChangeSetCatchUpStatus.DELETE_ONLY_FINAL,
                taskType,
                changeSetId,
                false
            ));

        catchUpTasks.put(
            ChangeSetManager.ChangeSetCatchUpStatus.WRITE_ONLY_FINAL.toString(),
            new ChangeSetCatchUpTask(
                schemaName,
                tableName,
                indexName,
                sourcePhyTableNames,
                targetTableLocations,
                ChangeSetManager.ChangeSetCatchUpStatus.WRITE_ONLY_FINAL,
                taskType,
                changeSetId,
                false
            ));
        return catchUpTasks;
    }

    public static List<DdlTask> genChangeSetOnlineSchemaChangeTasks(String schemaName, String tableName,
                                                                    List<String> relatedTables,
                                                                    String finalStatus,
                                                                    ChangeSetStartTask changeSetStartTask,
                                                                    Map<String, ChangeSetCatchUpTask> catchUpTasks,
                                                                    DdlTask backFillTask,
                                                                    DdlTask changeSetCheckTask,
                                                                    DdlTask changeSetCheckTwiceTask,
                                                                    ChangeSetApplyFinishTask changeSetApplyFinishTask,
                                                                    List<DdlTask> outDdlTasks,
                                                                    ExecutionContext executionContext) {
        List<DdlTask> ddlTasks = new ArrayList<>();
        final boolean stayAtCreating =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.CREATING.name(), finalStatus);
        final boolean stayAtDeleteOnly =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY.name(), finalStatus);
        final boolean stayAtWriteOnly =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY.name(), finalStatus);
        final boolean stayAtWriteReOrg =
            StringUtils.equalsIgnoreCase(ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG.name(), finalStatus);
        final boolean useApplyOpt = changeSetApplyFinishTask != null
            && executionContext.getParamManager().getBoolean(CHANGE_SET_APPLY_OPTIMIZATION);
        final boolean checkTwice = executionContext.getParamManager().getBoolean(CHANGE_SET_CHECK_TWICE);

        AlterComplexTaskUpdateJobStatusTask absentTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                tableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.CREATING,
                ComplexTaskMetaManager.ComplexTaskStatus.CHANGE_SET_START,
                null,
                null);
        AlterComplexTaskUpdateJobStatusTask deleteOnlyTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                tableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.CHANGE_SET_START,
                ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
                null,
                null);
        AlterComplexTaskUpdateJobStatusTask writeOnlyTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                tableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY,
                null,
                null);
        AlterComplexTaskUpdateJobStatusTask writeReOrgTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                tableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG,
                null,
                null);
        AlterComplexTaskUpdateJobStatusTask readyToPublicTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                tableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG,
                ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC,
                null,
                null);

        AlterComplexTaskUpdateJobStatusTask deleteOnlyFinalTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                tableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.DOING_CHECKER,
                ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
                null,
                null);
        AlterComplexTaskUpdateJobStatusTask writeOnlyFinalTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                tableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY,
                null,
                null);
        AlterComplexTaskUpdateJobStatusTask readyToPublicFinalTask =
            new AlterComplexTaskUpdateJobStatusTask(
                schemaName,
                tableName,
                relatedTables,
                true,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC,
                null,
                null);

        Long initWait = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INITWAIT);
        Long interval = executionContext.getParamManager().getLong(ConnectionParams.PREEMPTIVE_MDL_INTERVAL);

        List<TableSyncTask> tableSyncTasks = new ArrayList<>();
        for (int i = 0; i < 7; ++i) {
            tableSyncTasks.add(new TableSyncTask(schemaName, relatedTables.get(0), true, initWait, interval,
                TimeUnit.MILLISECONDS));
        }

        if (stayAtCreating) {
            return ddlTasks;
        }
        ddlTasks.add(changeSetStartTask);
        ddlTasks.add(absentTask);
        ddlTasks.add(tableSyncTasks.get(0));
        if (backFillTask != null) {
            ddlTasks.add(backFillTask);
        } else {
            outDdlTasks.add(tableSyncTasks.get(0));
            outDdlTasks.add(catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.ABSENT.toString()));
        }

        ddlTasks.add(catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.ABSENT.toString()));

        ddlTasks.add(deleteOnlyTask);
        ddlTasks.add(tableSyncTasks.get(1));
        if (stayAtDeleteOnly) {
            ddlTasks.add(catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.WRITE_ONLY_FINAL.toString()));
            return ddlTasks;
        }
        ddlTasks.add(catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.DELETE_ONLY.toString()));

        ddlTasks.add(writeOnlyTask);
        ddlTasks.add(tableSyncTasks.get(2));
        if (stayAtWriteOnly) {
            ddlTasks.add(catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.WRITE_ONLY_FINAL.toString()));
            return ddlTasks;
        }
        ddlTasks.add(useApplyOpt ? catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.WRITE_ONLY.toString())
            : catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.WRITE_ONLY_FINAL.toString()));

        ddlTasks.add(writeReOrgTask);
        ddlTasks.add(tableSyncTasks.get(3));
        if (stayAtWriteReOrg) {
            if (useApplyOpt) {
                ddlTasks.add(catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.WRITE_ONLY_FINAL.toString()));
            }
            return ddlTasks;
        }

        if (useApplyOpt) {
            ddlTasks.add(changeSetCheckTask);
            ddlTasks.add(catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.ABSENT_FINAL.toString()));
            ddlTasks.add(changeSetApplyFinishTask);
            ddlTasks.add(deleteOnlyFinalTask);
            ddlTasks.add(tableSyncTasks.get(4));
            ddlTasks.add(catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.DELETE_ONLY_FINAL.toString()));
            ddlTasks.add(writeOnlyFinalTask);
            ddlTasks.add(tableSyncTasks.get(5));
            ddlTasks.add(catchUpTasks.get(ChangeSetManager.ChangeSetCatchUpStatus.WRITE_ONLY_FINAL.toString()));
            if (checkTwice) {
                ddlTasks.add(changeSetCheckTwiceTask);
            }
            ddlTasks.add(readyToPublicFinalTask);
        } else {
            ddlTasks.add(changeSetCheckTask);
            ddlTasks.add(readyToPublicTask);
        }

        ddlTasks.add(tableSyncTasks.get(6));

        return ddlTasks;
    }

    public static void doChangeSetSchemaChange(String schemaName, String logicalTableName, List<String> relatedTables,
                                               DdlTask currentTask, ComplexTaskMetaManager.ComplexTaskStatus oldStatus,
                                               ComplexTaskMetaManager.ComplexTaskStatus newStatus) {
        final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

        new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                ComplexTaskMetaManager
                    .updateSubTasksStatusByJobIdAndObjName(currentTask.getJobId(),
                        schemaName,
                        logicalTableName,
                        oldStatus,
                        newStatus,
                        getConnection());
                try {
                    for (String tbName : relatedTables) {
                        TableInfoManager.updateTableVersionWithoutDataId(schemaName, tbName, getConnection());
                    }
                } catch (Exception e) {
                    throw GeneralUtil.nestedException(e);
                }
                currentTask.setState(DdlTaskState.DIRTY);
                DdlEngineTaskRecord taskRecord = TaskHelper.toDdlEngineTaskRecord(currentTask);
                return engineTaskAccessor.updateTask(taskRecord);
            }
        }.execute();

        LOGGER.info(
            String.format(
                "Update table status[ schema:%s, table:%s, before state:%s, after state:%s]",
                schemaName,
                logicalTableName,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG.name(),
                ComplexTaskMetaManager.ComplexTaskStatus.DOING_CHECKER.name()));

        try {
            SyncManagerHelper.sync(
                new TablesMetaChangePreemptiveSyncAction(schemaName, relatedTables, 1500L, 1500L,
                    TimeUnit.SECONDS),
                SyncScope.ALL);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while sync table meta, schemaName:%s, tableName:%s", schemaName,
                logicalTableName));
            throw GeneralUtil.nestedException(t);
        }
    }

    public static List<List<Row>> genBatchRowList(List<Row> rows, int batchSize) {
        List<List<Row>> RowBatchList = new ArrayList<>();
        IntStream.range(0, (rows.size() + batchSize - 1) / batchSize)
            .mapToObj(i -> rows.subList(i * batchSize, Math.min(rows.size(), (i + 1) * batchSize)))
            .forEach(RowBatchList::add);

        Collections.shuffle(RowBatchList);
        return RowBatchList;
    }

    public static void deadlockErrConsumer(PhyTableOperation plan, ExecutionContext ec,
                                           TddlNestableRuntimeException e, int retryCount) {
        if (plan == null) {
            return;
        }
        final String dbIndex = plan.getDbIndex();
        final String phyTable = plan.getTableNames().get(0).get(0);
        final String row = GsiUtils.rowToString(plan.getParam());

        if (retryCount < RETRY_COUNT) {

            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] Deadlock found while apply changeset data from {1}[{2}][{3}] retry count[{4}]: {5}",
                ec.getTraceId(),
                dbIndex,
                phyTable,
                row,
                retryCount,
                e.getMessage()));

            try {
                TimeUnit.MILLISECONDS.sleep(RETRY_WAIT[retryCount]);
            } catch (InterruptedException ex) {
                // ignore
            }
        } else {
            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] Deadlock found while apply changeset data from {1}[{2}][{3}] throw: {4}",
                ec.getTraceId(),
                dbIndex,
                phyTable,
                row,
                e.getMessage()));

            throw GeneralUtil.nestedException(e);
        }
    }

    public static Object convertObject(ColumnMeta cm, Object obValue) {
        if (obValue == null) {
            return null;
        }
        Object res = obValue;

        DataType dataType = cm.getDataType();

        if (dataType instanceof EnumType) {
            res = new EnumValue((EnumType) dataType, DataTypes.StringType.convertFrom(obValue));
        }

        // boolean类型可以表示tinyint1范围的数字，直接getobject返回的是true/false，丢精度
        if (dataType instanceof BooleanType

            || (dataType instanceof TinyIntType && obValue instanceof Boolean)
            || (dataType instanceof UTinyIntType && obValue instanceof Boolean)) {
            res = ((Boolean) obValue) ? Integer.valueOf(1) : Integer.valueOf(0);
        }

        // For now, we only support YEAR(4).
        if (dataType instanceof YearType && obValue instanceof Date) {
            if (obValue instanceof ZeroDate) {
                res = 0;
            } else {
                res = ((Date) obValue).getYear() + 1900;
            }
        }
        return res;
    }

    // for move partitions / move database
    public static Map<String, String> genTargetTableLocations(
        Map<String, com.alibaba.polardbx.common.utils.Pair<String, String>> orderedTargetTableLocations) {
        Map<String, String> targetTableLocations = new HashMap<>(orderedTargetTableLocations.entrySet().size());
        orderedTargetTableLocations.values().stream().forEach(e -> {
            targetTableLocations.put(e.getKey(), e.getValue());
        });
        return targetTableLocations;
    }

    /**
     * 判断是否支持 changeset
     */
    public static boolean supportUseChangeSet(ComplexTaskMetaManager.ComplexTaskType taskType, TableMeta tableMeta) {
        if (taskType == ComplexTaskMetaManager.ComplexTaskType.SPLIT_HOT_VALUE) {
            return false;
        }
        if (taskType == ComplexTaskMetaManager.ComplexTaskType.MERGE_PARTITION) {
            return false;
        }

        // Changeset does not support table with generated column for now
        if (tableMeta.hasGeneratedColumn()) {
            return false;
        }

        if (!tableMeta.isHasPrimaryKey()) {
            return false;
        }

        final ExecutorContext executorContext = ExecutorContext.getContext(tableMeta.getSchemaName());
        final TopologyHandler topologyHandler = executorContext.getTopologyHandler();
        final boolean allDnUseXDataSource = isAllDnUseXDataSource(topologyHandler);
        if (!allDnUseXDataSource && tableMeta.getPrimaryKey().stream()
            .anyMatch(columnMeta -> columnMeta.getDataType() instanceof FloatType)) {
            return false;
        }

        return true;
    }

    public static Pair<String, String> getTargetGroupNameAndPhyTableName(
        String sourceTable,
        String sourceGroup,
        ComplexTaskMetaManager.ComplexTaskType taskType,
        Map<String, String> targetTableLocations) {
        String targetGroup = null;
        String targetTable = null;
        List<String> targetTableList = new ArrayList<>(targetTableLocations.keySet());
        if (taskType == ComplexTaskMetaManager.ComplexTaskType.MERGE_PARTITION) {
            targetTable = targetTableList.get(0);
            targetGroup = targetTableLocations.get(targetTable);
        } else if (taskType == ComplexTaskMetaManager.ComplexTaskType.SPLIT_PARTITION
            || taskType == ComplexTaskMetaManager.ComplexTaskType.SPLIT_HOT_VALUE) {
            // need route
        } else if (taskType == ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION) {
            // move partitions or move database
            targetTable = sourceTable;
            targetGroup = targetTableLocations.get(targetTable);
        } else if (taskType == ComplexTaskMetaManager.ComplexTaskType.MOVE_DATABASE) {
            targetTable = sourceTable;
            targetGroup = targetTableLocations.get(sourceGroup);
        } else if (taskType == ComplexTaskMetaManager.ComplexTaskType.ONLINE_MODIFY_COLUMN) {
            targetGroup = sourceGroup;
            targetTable = targetTableLocations.get(sourceTable);
        }

        return new Pair<>(targetGroup, targetTable);
    }
}
