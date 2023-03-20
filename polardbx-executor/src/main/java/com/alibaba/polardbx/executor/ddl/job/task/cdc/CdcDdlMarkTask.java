package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.DdlVisibility;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateMaterializedViewStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropMaterializedViewStatement;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.cdc.ICdcManager.REFRESH_CREATE_SQL_4_PHY_TABLE;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcSqlUtils.SQL_PARSE_FEATURES;

/**
 * Created by ziyang.lb
 **/
@TaskName(name = "CdcDdlMarkTask")
@Getter
@Setter
public class CdcDdlMarkTask extends BaseDdlTask {
    private final PhysicalPlanData physicalPlanData;

    @JSONCreator
    public CdcDdlMarkTask(String schemaName, PhysicalPlanData physicalPlanData) {
        super(schemaName);
        this.physicalPlanData = physicalPlanData;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        if (physicalPlanData.getKind() == SqlKind.CREATE_TABLE) {
            mark4CreateTable(executionContext);
        } else if (physicalPlanData.getKind() == SqlKind.DROP_TABLE) {
            mark4DropTable(executionContext);
        } else if (physicalPlanData.getKind() == SqlKind.RENAME_TABLE) {
            if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                mark4RenamePartitionModeTable(executionContext);
            } else {
                mark4RenameTable(executionContext);
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

    private void mark4CreateTable(ExecutionContext executionContext) {
        DdlContext ddlContext = executionContext.getDdlContext();
        if (isCreateMaterializedView(ddlContext.getDdlStmt())) {
            //物化视图不打标
            return;
        }
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                physicalPlanData.getCreateTablePhysicalSql(), ddlContext.getDdlType(), ddlContext.getJobId(),
                getTaskId(),
                DdlVisibility.Public, buildExtendParameter(executionContext));
    }

    private void mark4DropTable(ExecutionContext executionContext) {
        // CdcDdlMarkTask执行前，表已经对外不可见，进入此方法时所有物理表也已经删除成功
        DdlContext ddlContext = executionContext.getDdlContext();
        if (isDropMaterializedView(ddlContext.getDdlStmt())) {
            //物化视图不打标
            return;
        }
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                ddlContext.getDdlStmt(), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                DdlVisibility.Public,
                buildExtendParameter(executionContext));
    }

    private void mark4RenameTable(ExecutionContext executionContext) {
        // 如果物理表名也发生了变化，需要将新的tablePattern作为附加参数传给cdcManager
        // 如果物理表名也发生了变更，此处所有物理表已经都完成了rename(此时用户针对该逻辑表提交的任何dml操作都会报错)，cdc打标必须先于元数据变更
        // 如果物理表名未进行变更，那么tablePattern不会发生改变，Rename是一个轻量级的操作，打标的位置放到元数据变更之前或之后，都可以
        String newTbNamePattern = TableMetaChanger.buildNewTbNamePattern(executionContext, schemaName,
            physicalPlanData.getLogicalTableName(), physicalPlanData.getNewLogicalTableName());
        Map<String, Object> params = buildExtendParameter(executionContext);
        params.put(ICdcManager.TABLE_NEW_NAME, physicalPlanData.getNewLogicalTableName());
        params.put(ICdcManager.TABLE_NEW_PATTERN, newTbNamePattern);

        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                ddlContext.getDdlStmt(), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                DdlVisibility.Public, params);
    }

    private void mark4RenamePartitionModeTable(ExecutionContext executionContext) {
        //分区表没有tablePattern，也不会改物理表的名字，所以和非分区表区分开，单独打标
        Map<String, Object> params = buildExtendParameter(executionContext);
        params.put(ICdcManager.TABLE_NEW_NAME, physicalPlanData.getNewLogicalTableName());

        if (executionContext.isPhyTableRenamed()) {
            Map<String, Set<String>> newTopology = new HashMap<>();
            Map<String, List<List<String>>> topology = physicalPlanData.getTableTopology();
            topology.forEach((k, v) ->
                newTopology.computeIfAbsent(k,
                    i -> v.stream().map(l -> l.get(1)).collect(Collectors.toSet()))
            );

            DdlContext ddlContext = executionContext.getDdlContext();
            CdcManagerHelper.getInstance()
                .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                    ddlContext.getDdlStmt(), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                    DdlVisibility.Public, params, true, newTopology);
        } else {
            DdlContext ddlContext = executionContext.getDdlContext();
            CdcManagerHelper.getInstance()
                .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                    ddlContext.getDdlStmt(), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                    DdlVisibility.Public, params);
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
                ddlContext.getDdlStmt(), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                DdlVisibility.Public,
                buildExtendParameter(executionContext));
    }

    private void mark4CreateIndex(ExecutionContext executionContext) {
        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                ddlContext.getDdlStmt(), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                DdlVisibility.Public,
                buildExtendParameter(executionContext));
    }

    private void mark4DropIndex(ExecutionContext executionContext) {
        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                ddlContext.getDdlStmt(), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                DdlVisibility.Public,
                buildExtendParameter(executionContext));
    }

    private void mark4TruncateTable(ExecutionContext executionContext) {
        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                ddlContext.getDdlStmt(), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                DdlVisibility.Public,
                buildExtendParameter(executionContext));
    }

    private void mark4TruncatePartition(ExecutionContext executionContext) {
        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, physicalPlanData.getLogicalTableName(), physicalPlanData.getKind().name(),
                ddlContext.getDdlStmt(), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                DdlVisibility.Private,
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
}
