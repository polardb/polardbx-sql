package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

@TaskName(name = "CdcCreateTableIfNotExistsMarkTask")
@Getter
@Setter
@Slf4j
public class CdcCreateTableIfNotExistsMarkTask extends BaseDdlTask {

    private String tableName;

    @JSONCreator
    public CdcCreateTableIfNotExistsMarkTask(String schemaName, String tableName) {
        super(schemaName);
        this.tableName = tableName;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        DdlContext ddlContext = executionContext.getDdlContext();
        boolean tableExists = TableValidator.checkIfTableExists(schemaName, tableName);

        //如果表还存在，说明和prepare阶段的判断结果是一致的，正常打标即可
        //如表表已经不存在了，说明和prepare阶段的判断结果是不一致的，则不能打标，否则会引发一致性问题，如：
        //存在一张表t1，线程1执行drop，线程2执行create t1 if not exist，线程2看到表存在直接打标，但在线程2执行打标前，线程1先执行成功了，
        //同步到下游的顺序则为 drop -> create，最终的结果是上游不存在t1，但下游存在t1
        if (tableExists) {
            // 需要考虑历史兼容问题
            // 历史上cdc下游依据job_id是否为空来判断是否需要对打标sql进行apply，如果不为空则进行apply，如果为空则不进行apply
            // 所以此处需要继续保持job_id为空，来解决兼容性问题。否则，当只升级CN、没有升级CDC时，老版本的CDC无法识别是真实建表，还是单纯打标，会触发问题
            CdcManagerHelper.getInstance().notifyDdlNew(schemaName, tableName, SqlKind.CREATE_TABLE.name(),
                ddlContext.getDdlStmt(), ddlContext.getDdlType(), null, getTaskId(),
                CdcDdlMarkVisibility.Public, buildExtendParameter(executionContext));
        } else {
            log.warn("table {} has been dropped, cdc ddl mark for creating table with if not exits is ignored, sql {}",
                tableName, ddlContext.getDdlStmt());
        }
    }
}
