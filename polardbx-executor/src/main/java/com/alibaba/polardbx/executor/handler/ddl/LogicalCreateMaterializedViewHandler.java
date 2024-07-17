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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateViewStatement;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.CreateViewSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.Blob;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateMaterializedView;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.memory.MemoryEstimator;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.IDistributedTransaction;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import com.alibaba.polardbx.rule.model.TargetDB;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.TDDLSqlSelect;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;
import static com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx.BLOCK_SIZE;

public class LogicalCreateMaterializedViewHandler extends LogicalCreateTableHandler {

    public LogicalCreateMaterializedViewHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        LogicalCreateMaterializedView logicalCreateTable = (LogicalCreateMaterializedView) logicalPlan;

        List<String> columnNameList = new ArrayList<>();
        String schemaName = logicalCreateTable.getSchemaName();
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }

        String tableName = logicalCreateTable.getViewName();

        if (!logicalCreateTable.bRefresh) {
            DdlContext ddlContext =
                DdlContext.create(schemaName, tableName, DdlType.CREATE_TABLE, executionContext);

            executionContext.setDdlContext(ddlContext);

            RelDataType relDataType = logicalCreateTable.getInput().getRowType();

            boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(logicalCreateTable.getSchemaName());

            //generate create table
            StringBuilder builder = new StringBuilder();
            builder.append("CREATE TABLE if not exists ").append(" ")
                .append(logicalCreateTable.getViewName()).append(" ")
                .append("(");

            int columnSize = relDataType.getFieldList().size();
            for (int i = 0; i < columnSize; i++) {
                RelDataTypeField field = relDataType.getFieldList().get(i);
                String typeName = field.getType().getFullTypeString();
                if (typeName.contains("VARCHAR(65536)")) {
                    typeName = typeName.replace("VARCHAR(65536)", "VARCHAR(16000)");
                }
                String columnName = field.getName();
                builder.append(columnName).append(" ").append(typeName);
                if (i != columnSize - 1) {
                    builder.append(",");
                }
                columnNameList.add(columnName);
            }

            builder.append(")")
                .append(" ").append("ENGINE = InnoDB");
            if (isNewPartDb) {
                builder.append(" SINGLE;");
            } else {
                builder.append(";");
            }
            String targetCreateTableSql = builder.toString();

            //ddl
            final SqlCreateTable targetTableAst = (SqlCreateTable)
                new FastsqlParser().parse(targetCreateTableSql, executionContext).get(0);

            PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);

            ExecutionPlan createTableSqlPlan = Planner.getInstance().getPlan(targetTableAst, plannerContext);
            LogicalCreateTable logicalCreateeRelNode = (LogicalCreateTable) createTableSqlPlan.getPlan();

            // Validate the plan on file storage first
            TableValidator.validateTableEngine(logicalCreateeRelNode, executionContext);
            boolean returnImmediately = validatePlan(logicalCreateeRelNode, executionContext);
            DdlJob ddlJob = null;
            if (returnImmediately) {
                ddlJob = new TransientDdlJob();
            } else {
                logicalCreateeRelNode.prepareData(executionContext);
                if (!isNewPartDb) {
                    ddlJob = buildCreateTableJob(logicalCreateeRelNode, executionContext, null);
                } else {
                    ddlJob = buildCreatePartitionTableJob(logicalCreateeRelNode, executionContext, null);
                }
            }

            // Handle the client DDL request on the worker side.
            handleDdlRequest(ddlJob, executionContext);

        }
        try {
            refreshMaterializedView(
                executionContext, schemaName, tableName,
                logicalCreateTable.getInput());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        markDdlForCdc(executionContext, schemaName, tableName, executionContext.getOriginSql());

        if (!logicalCreateTable.bRefresh) {
            syncView(logicalCreateTable, executionContext, columnNameList);
        }

        return new AffectRowCursor(new int[] {0});
    }

    public static void refreshMaterializedView(
        ExecutionContext context, String schema, String tableName, RelNode input) throws Exception {

        long id = 1;
        String dbIndex = OptimizerContext.getContext(
            schema).getRuleManager().getDefaultDbIndex(null);
        String phyTable = null;
        final OptimizerContext optimizerContext = OptimizerContext.getContext(schema);
        PartitionInfoManager partInfoMgr = optimizerContext.getPartitionInfoManager();
        PartitionInfo partInfo =
            partInfoMgr.getPartitionInfo(tableName);
        final TddlRuleManager rule = optimizerContext.getRuleManager();
        final boolean singleDbIndex = rule.isSingleDbIndex();
        if (!singleDbIndex && partInfo == null) {
            TargetDB target = rule.shardAny(tableName);
            phyTable = target.getTableNames().iterator().next();
            dbIndex = target.getDbIndex();
        } else if (partInfo != null) {
            PhysicalPartitionInfo prunedPartitionInfo = partInfoMgr.getFirstPhysicalPartition(tableName);
            dbIndex = prunedPartitionInfo.getGroupKey();
            phyTable = prunedPartitionInfo.getPhyTable();
        }

        //generate insert sql
        TableMeta tableMeta = optimizerContext.getLatestSchemaManager().getTable(tableName);
        List<ColumnMeta> columnMetas = tableMeta.getAllColumns();
        int rowSize = input.getRowType().getFieldCount();
        StringBuilder builder = new StringBuilder();
        builder.append("insert into ").append(phyTable);

        builder.append(" ( ");
        for (int i = 0; i < columnMetas.size(); i++) {
            builder.append(columnMetas.get(i).getName());
            if (i != columnMetas.size() - 1) {
                builder.append(",");
            }
        }
        builder.append(" ) ");

        builder.append(" values ");

        builder.append("(");
        for (int i = 0; i < rowSize; i++) {
            builder.append("?");
            builder.append(",");
        }
        builder.append("?");
        builder.append(")");

        String insertSql = builder.toString();

        TGroupDataSource groupDataSource =
            (TGroupDataSource) ExecutorContext.getContext(schema).getTopologyExecutor()
                .getGroupExecutor(dbIndex).getDataSource();
        TGroupDirectConnection directConnection = null;

        // Select all data at once or streaming select for multiple times.
        boolean cacheAllOutput = context.getTransaction() instanceof IDistributedTransaction;
        // How many records to insert each time in "insert ... select"
        long batchSize = context.getParamManager().getLong(ConnectionParams.INSERT_SELECT_BATCH_SIZE);

        final long maxMemoryLimit =
            MemoryEstimator.calcSelectValuesMemCost(batchSize, input.getRowType());
        final long memoryOfOneRow = maxMemoryLimit / batchSize;

        final String poolName = "MaterializedView@" + System.identityHashCode(input);
        final MemoryPool selectValuesPool = context.getMemoryPool().getOrCreatePool(
            poolName, Math.max(BLOCK_SIZE, maxMemoryLimit), MemoryType.OPERATOR);

        final MemoryAllocatorCtx memoryAllocator = selectValuesPool.getMemoryAllocatorCtx();

        // To make it concurrently execute to avoid inserting before some
        // selecting, which could make data duplicate.
        context.setModifySelect(true);

        Cursor selectCursor = null;
        final ExecutionContext selectEc = context.copy();
        try {
            selectCursor = ExecutorHelper.execute(input, selectEc, cacheAllOutput);

            List<List<Object>> values = null;
            // Select and insert loop
            do {
                values =
                    selectForModify(selectCursor, batchSize, memoryAllocator::allocateReservedMemory, memoryOfOneRow);
                if (values.isEmpty()) {
                    break;
                }

                if (directConnection == null) {
                    directConnection = groupDataSource.getConnection();

                    directConnection.setAutoCommit(false);
                    PreparedStatement
                        deleteStatement = directConnection.prepareStatement(String.format("delete from %s", phyTable));
                    deleteStatement.executeUpdate();
                    deleteStatement.close();
                }

                PreparedStatement
                    prepateStatement = directConnection.prepareStatement(insertSql);

                for (List<Object> rets : values) {
                    for (int i = 0; i < rets.size(); i++) {
                        Object value = rets.get(i);
                        if (value instanceof Blob) {
                            long length = ((java.sql.Blob) value).length();
                            value = ((java.sql.Blob) value).getBytes(1, (int) length);
                        }
                        prepateStatement.setObject(i + 1, value);
                    }
                    prepateStatement.setLong(rets.size() + 1, id++);
                    prepateStatement.addBatch();
                }
                prepateStatement.executeBatch();
                prepateStatement.close();
                memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), false);
            } while (true);
        } catch (Throwable t) {
            if (directConnection != null) {
                directConnection.rollback();
            }
            throw new RuntimeException(t);
        } finally {
            if (selectCursor != null) {
                selectCursor.close(new ArrayList<>());
            }
            selectValuesPool.destroy();

            if (directConnection != null) {
                directConnection.commit();
                directConnection.close();
            }
        }
    }

    public void syncView(final LogicalCreateMaterializedView materializedView, ExecutionContext executionContext,
                         List<String> columnList) {

        String schemaName = materializedView.getSchemaName();
        String viewName = materializedView.getViewName() + "_Materialized";
        String viewDefinition = RelUtils.toNativeSql(materializedView.getDefinition(), DbType.MYSQL);
        String planString = null;
        String planType = null;

        ViewManager viewManager = OptimizerContext.getContext(schemaName).getViewManager();

        TDDLSqlSelect tddlSqlSelect = (TDDLSqlSelect) materializedView.getDefinition();
        if (tddlSqlSelect.getHints() != null && tddlSqlSelect.getHints().size() != 0) {
            String withHintSql =
                ((SQLCreateViewStatement) FastsqlUtils.parseSql(executionContext.getSql()).get(0)).getSubQuery()
                    .toString();
            // FIXME: by now only support SMP plan.
            executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_MPP, false);
            executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_PARAMETER_PLAN, false);
            ExecutionPlan executionPlan =
                Planner.getInstance().plan(withHintSql, executionContext.copy());
            if (PlanManagerUtil.canConvertToJson(executionPlan, executionContext.getParamManager())) {
                planString = PlanManagerUtil.relNodeToJson(executionPlan.getPlan());
                planType = "SMP";
            }
        }

        // check view name
        TableMeta tableMeta;
        try {
            tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(viewName);
        } catch (Throwable throwable) {
            // pass
            tableMeta = null;
        }

        if (tableMeta != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "Materialized View '" + viewName + "' already exists ");
        }

        boolean success = false;

        if (viewManager.select(viewName) != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW, "table '" + viewName + "' already exists ");
        }
        success = viewManager
            .insert(viewName, columnList, viewDefinition, executionContext.getConnection().getUser(), planString,
                planType);

        if (!success) {
            throw new TddlRuntimeException(ErrorCode.ERR_VIEW,
                "create view fail for " + viewManager.getSystemTableView().getTableName() + " can not "
                    + "write");

        }
        SyncManagerHelper.sync(new CreateViewSyncAction(schemaName, viewName), schemaName, SyncScope.CURRENT_ONLY);
    }

    //TODO cdc@shengyu
    private void markDdlForCdc(ExecutionContext executionContext, String schemaName, String viewName, String ddlSql) {
        CdcManagerHelper.getInstance().notifyDdlNew(
            schemaName,
            viewName,
            SqlKind.CREATE_MATERIALIZED_VIEW.name(),
            ddlSql,
            DdlType.UNSUPPORTED,
            null,
            null,
            CdcDdlMarkVisibility.Protected,
            buildExtendParameter(executionContext));
    }
}
