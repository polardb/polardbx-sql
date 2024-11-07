package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.DdlScope;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

/**
 * description:
 * author: chengjin.lyf
 * create: 2024-08-19 18:52
 **/
@TaskName(name = "CdcExpireLocalPartitionMarkTask")
@Getter
@Setter
public class CdcExpireLocalPartitionMarkTask extends BaseDdlTask {

    private final String tableName;

    @JSONCreator
    public CdcExpireLocalPartitionMarkTask(String schemaName, String tableName) {
        super(schemaName);
        this.tableName = tableName;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        DdlContext ddlContext = executionContext.getDdlContext();
        Map<String, Object> param = buildExtendParameter(executionContext);
        param.put(ICdcManager.CDC_DDL_SCOPE, DdlScope.Schema);

        Map<String, Object> userDefVariables = executionContext.getUserDefVariables();

        if (userDefVariables != null) {
            String overRideNow = (String) userDefVariables.get(StringUtils.lowerCase(FailPointKey.FP_OVERRIDE_NOW));
            if (StringUtils.isNotBlank(overRideNow)) {
                //used by local partition test case
                param.put(FailPointKey.FP_OVERRIDE_NOW, overRideNow);
            }
        }

        CdcManagerHelper.getInstance()
            .notifyDdlNew(
                schemaName,
                tableName,
                SqlKind.EXPIRE_LOCAL_PARTITION.name(),
                ddlContext.getDdlStmt(),
                ddlContext.getDdlType(),
                ddlContext.getJobId(),
                getTaskId(),
                CdcDdlMarkVisibility.Protected,
                param);
    }
}
