package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.Map;

import static com.alibaba.polardbx.common.cdc.ICdcManager.CDC_TABLE_GROUP_MANUAL_CREATE_FLAG;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-08-28 18:52
 **/
@TaskName(name = "CdcAlterTableGroupRenamePartitionMarkTask")
@Getter
@Setter
public class CdcAlterTableGroupRenamePartitionMarkTask extends BaseDdlTask {

    private final String tableGroupName;

    @JSONCreator
    public CdcAlterTableGroupRenamePartitionMarkTask(String schemaName, String tableGroupName) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        Map<String, Object> parameter = buildExtendParameter(executionContext);

        boolean isManuallyCreatedTg = TableGroupUtils
            .getTableGroupInfoByGroupName(schemaName, tableGroupName).isManuallyCreated();
        parameter.put(CDC_TABLE_GROUP_MANUAL_CREATE_FLAG, isManuallyCreatedTg);

        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(
                schemaName,
                tableGroupName,
                SqlKind.ALTER_TABLEGROUP.name(),
                ddlContext.getDdlStmt(),
                ddlContext.getDdlType(),
                ddlContext.getJobId(),
                getTaskId(),
                CdcDdlMarkVisibility.Protected,
                parameter);
    }
}
