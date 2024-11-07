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

package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateMaterializedViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropMaterializedViewStatement;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseCdcTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.CciSchemaEvolutionTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.cdc.ICdcManager.REFRESH_CREATE_SQL_4_PHY_TABLE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.SIM_CDC_FAILED;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDropTableIfExistsMarkTask.checkTableName;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.isUseFkOriginalDDL;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcSqlUtils.SQL_PARSE_FEATURES;

/**
 * Created by ziyang.lb
 **/
@TaskName(name = "CdcDdlMarkTask")
@Getter
@Setter
public class CdcDdlMarkTask extends BaseCdcTask {

    private final PhysicalPlanData physicalPlanData;
    private final Long versionId;

    private boolean useOriginalDDl;
    private boolean foreignKeys;
    private boolean isCci = false;

    /**
     * For statement like CREATE TABLE with CCI,
     * the original ddl statement will be normalized
     * (reassigned a unique index name, assigned a partitioning part if not already specified)
     * in {@link org.apache.calcite.sql.validate.SqlValidatorImpl#gsiNormalizeNewPartition}
     * and {@link com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter#checkAndRewriteGsiName}.
     * <br/>
     * We have to save the normalized ddl statement in ext part of cdc mark,
     * so that the down stream can replay the statement as the upper does.
     * <br/>
     * PS: PARTITIONS is assigned in {@link com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder#buildCompletePartByDefByAstParams}
     */
    private String normalizedOriginalDdl;
    private final List<CciSchemaEvolutionTask> schemaEvolutionRecordInitializer = new ArrayList<>();

    @JSONCreator
    public CdcDdlMarkTask(String schemaName, PhysicalPlanData physicalPlanData, Boolean useOriginalDdl,
                          Boolean foreignKeys, Long versionId) {
        super(schemaName);
        this.physicalPlanData = physicalPlanData;
        this.useOriginalDDl = useOriginalDdl != null && useOriginalDdl;
        this.foreignKeys = foreignKeys != null && foreignKeys;
        this.versionId = versionId;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        if (executionContext.getParamManager().getBoolean(SIM_CDC_FAILED)) {
            throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC, "Simulation cdc mark task failed");
        }

        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        if (CBOUtil.isGsi(schemaName, physicalPlanData.getLogicalTableName())) {
            // gsi task should use CdcGsiDdlMarkTask
            return;
        }

        if (executionContext.getDdlContext().isSkipSubJobCdcMark()) {
            // 跳过子任务 cdc 打标
            return;
        }

        prepareExtraCmdsKey(executionContext);
        if (physicalPlanData.getKind() == SqlKind.CREATE_TABLE) {
            if (executionContext.getDdlContext() != null &&
                executionContext.getDdlContext().getDdlType() == DdlType.CREATE_TABLE) {
                useOriginalDDl = true;
                prepareExtraCmdsKey(executionContext);
            }
            mark4CreateTable(metaDbConnection, executionContext);
        } else if (physicalPlanData.getKind() == SqlKind.DROP_TABLE) {
            mark4DropTable(executionContext);
        } else if (physicalPlanData.getKind() == SqlKind.RENAME_TABLE) {
            if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                mark4RenamePartitionModeTable(executionContext, physicalPlanData.isRenamePhyTable());
            } else {
                mark4RenameTable(executionContext, physicalPlanData.isRenamePhyTable());
            }
        } else if (physicalPlanData.getKind() == SqlKind.ALTER_TABLE) {
            mark4AlterTable(executionContext);
        } else if (physicalPlanData.getKind() == SqlKind.CREATE_INDEX) {
            mark4CreateIndex(executionContext);
        } else if (physicalPlanData.getKind() == SqlKind.DROP_INDEX) {
            mark4DropIndex(executionContext);
        } else if (physicalPlanData.getKind() == SqlKind.TRUNCATE_TABLE) {
            if (physicalPlanData.isTruncatePartition()) {
                mark4TruncatePartition(executionContext);
            } else {
                mark4TruncateTable(executionContext);
            }
        } else {
            throw new RuntimeException("not supported sql kind : " + physicalPlanData.getKind());
        }
    }

    private void prepareExtraCmdsKey(ExecutionContext executionContext) {
        if (useOriginalDDl) {
            executionContext.getExtraCmds().put(ICdcManager.USE_ORIGINAL_DDL, "true");
        }
        if (foreignKeys) {
            executionContext.getExtraCmds().put(ICdcManager.FOREIGN_KEYS_DDL, "true");
        }
        if (isCci) {
            executionContext.getExtraCmds().put(ICdcManager.CDC_IS_CCI, true);
        }
        if (CdcMarkUtil.isVersionIdInitialized(versionId)) {
            CdcMarkUtil.useDdlVersionId(executionContext, versionId);
        }
    }

    private void mark4CreateTable(Connection metaDbConnection, ExecutionContext executionContext) {
        //物化视图不在此处打标，在LogicalDropViewHandler中进行打标
        DdlContext ddlContext = executionContext.getDdlContext();
        if (isCreateMaterializedView(ddlContext.getDdlStmt())) {
            return;
        }

        for (CciSchemaEvolutionTask initializer : schemaEvolutionRecordInitializer) {
            initializer.duringTransaction(jobId, metaDbConnection, executionContext);
        }

        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                physicalPlanData.getCreateTablePhysicalSql(), ddlContext.getDdlType(),
                ddlContext.getJobId(), getTaskId(), CdcDdlMarkVisibility.Public,
                buildExtendParameterWithNormalizedDdl(executionContext));
    }

    private void mark4DropTable(ExecutionContext executionContext) {
        // CdcDdlMarkTask执行前，表已经对外不可见，进入此方法时所有物理表也已经删除成功
        DdlContext ddlContext = executionContext.getDdlContext();
        if (isDropMaterializedView(ddlContext.getDdlStmt())) {
            //物化视图不打标
            return;
        }
        checkTableName(getDdlStmt(executionContext));
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                getDdlStmt(executionContext), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                CdcDdlMarkVisibility.Public,
                buildExtendParameter(executionContext));
    }

    private void mark4RenameTable(ExecutionContext executionContext, boolean renamePhyTable) {
        // 如果物理表名也发生了变化，需要将新的tablePattern作为附加参数传给cdcManager
        // 如果物理表名也发生了变更，此处所有物理表已经都完成了rename(此时用户针对该逻辑表提交的任何dml操作都会报错)，cdc打标必须先于元数据变更
        // 如果物理表名未进行变更，那么tablePattern不会发生改变，Rename是一个轻量级的操作，打标的位置放到元数据变更之前或之后，都可以
        String newTbNamePattern = TableMetaChanger.buildNewTbNamePattern(executionContext, schemaName,
            physicalPlanData.getLogicalTableName(), physicalPlanData.getNewLogicalTableName(), renamePhyTable);
        Map<String, Object> params = buildExtendParameter(executionContext);
        params.put(ICdcManager.TABLE_NEW_NAME, physicalPlanData.getNewLogicalTableName());
        params.put(ICdcManager.TABLE_NEW_PATTERN, newTbNamePattern);

        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                getDdlStmt(executionContext), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                CdcDdlMarkVisibility.Public, params);
    }

    private void mark4RenamePartitionModeTable(ExecutionContext executionContext, boolean renamePhyTable) {
        //分区表没有tablePattern，也不会改物理表的名字，所以和非分区表区分开，单独打标
        Map<String, Object> params = buildExtendParameter(executionContext);
        params.put(ICdcManager.TABLE_NEW_NAME, physicalPlanData.getNewLogicalTableName());

        if (renamePhyTable) {
            Map<String, Set<String>> newTopology = new HashMap<>();
            Map<String, List<List<String>>> topology = physicalPlanData.getTableTopology();
            topology.forEach((k, v) ->
                newTopology.computeIfAbsent(k,
                    i -> v.stream().map(l -> l.get(1)).collect(Collectors.toSet()))
            );

            DdlContext ddlContext = executionContext.getDdlContext();
            CdcManagerHelper.getInstance()
                .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                    getDdlStmt(executionContext), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                    CdcDdlMarkVisibility.Public, params, true, newTopology);
        } else {
            DdlContext ddlContext = executionContext.getDdlContext();
            CdcManagerHelper.getInstance()
                .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                    getDdlStmt(executionContext), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                    CdcDdlMarkVisibility.Public, params);
        }
    }

    private void mark4AlterTable(ExecutionContext executionContext) {
        DdlContext ddlContext = executionContext.getDdlContext();
        AlterTablePreparedData alterTablePreparedData = physicalPlanData.getAlterTablePreparedData();

        // 加减列操作，可能会导致逻辑表结构和物理表结构不一致，重新对
        if (alterTablePreparedData != null) {
            boolean isAddColumns = alterTablePreparedData.getAddedColumns() != null && !alterTablePreparedData
                .getAddedColumns().isEmpty();
            boolean isDropColumns = alterTablePreparedData.getDroppedColumns() != null && !alterTablePreparedData
                .getDroppedColumns().isEmpty();
            if (isAddColumns || isDropColumns) {
                executionContext.getExtraCmds().put(REFRESH_CREATE_SQL_4_PHY_TABLE, "true");
            }
        }

        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                getDdlStmt(executionContext), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                CdcDdlMarkVisibility.Public,
                buildExtendParameter(executionContext));
    }

    private void mark4CreateIndex(ExecutionContext executionContext) {
        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                getDdlStmt(executionContext), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                CdcDdlMarkVisibility.Public,
                buildExtendParameter(executionContext));
    }

    private void mark4DropIndex(ExecutionContext executionContext) {
        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                getDdlStmt(executionContext), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                CdcDdlMarkVisibility.Public,
                buildExtendParameter(executionContext));
    }

    private void mark4TruncateTable(ExecutionContext executionContext) {
        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                getDdlStmt(executionContext), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                CdcDdlMarkVisibility.Public,
                buildExtendParameter(executionContext));
    }

    private void mark4TruncatePartition(ExecutionContext executionContext) {
        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                getDdlStmt(executionContext), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                CdcDdlMarkVisibility.Protected,
                buildExtendParameter(executionContext));
    }

    private boolean isCreateMaterializedView(String sql) {
        List<SQLStatement> list = SQLUtils.parseStatements(sql, DbType.mysql, SQL_PARSE_FEATURES);
        return !list.isEmpty() && list.get(0) instanceof SQLCreateMaterializedViewStatement;
    }

    private boolean isDropMaterializedView(String sql) {
        List<SQLStatement> list = SQLUtils.parseStatements(sql, DbType.mysql, SQL_PARSE_FEATURES);
        return !list.isEmpty() && list.get(0) instanceof SQLDropMaterializedViewStatement;
    }

    private String getDdlStmt(ExecutionContext executionContext) {
        // 对于create table 和 alter table，在LogicalCommonDdlHandler里可能进行了重写
        String cdcRewriteDdlStmt = executionContext.getDdlContext().getCdcRewriteDdlStmt();
        String ddl = StringUtils.isNotBlank(cdcRewriteDdlStmt) ? cdcRewriteDdlStmt :
            executionContext.getDdlContext().getDdlStmt();
        return getDdlStmt(ddl);
    }

    private String getDdlStmt(String ddl) {
        if (CdcMarkUtil.isVersionIdInitialized(versionId)) {
            return CdcMarkUtil.buildVersionIdHint(versionId) + ddl;
        }
        return ddl;
    }

    public void setUseOriginalDDl(boolean useOriginalDDl) {
        this.useOriginalDDl = useOriginalDDl;
    }

    public void setNormalizedOriginalDdl(String normalizedOriginalDdl) {
        this.normalizedOriginalDdl = normalizedOriginalDdl;
    }

    private Map<String, Object> buildExtendParameterWithNormalizedDdl(ExecutionContext executionContext) {
        if (null != normalizedOriginalDdl || isUseFkOriginalDDL(executionContext)) {
            return buildExtendParameter(executionContext, normalizedOriginalDdl);
        }
        return buildExtendParameter(executionContext);
    }

    public void addSchemaEvolutionInitializers(List<CciSchemaEvolutionTask> initializers) {
        schemaEvolutionRecordInitializer.addAll(initializers);
    }

    @Override
    protected String remark() {
        return String.format("|ddlVersionId: %s", versionId);
    }
}
