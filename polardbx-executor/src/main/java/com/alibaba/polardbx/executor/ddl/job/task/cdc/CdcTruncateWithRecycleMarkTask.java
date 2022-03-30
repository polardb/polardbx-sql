package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.DdlVisibility;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.Map;

/**
 * Created by ziyang.lb
 **/
@TaskName(name = "CdcTruncateWithRecycleMarkTask")
@Getter
@Setter
public class CdcTruncateWithRecycleMarkTask extends BaseDdlTask {
    public static final String CDC_RECYCLE_HINTS = "/* RECYCLEBIN_INTERMEDIATE_DDL=true */";

    private final String sourceTableName;
    private final String targetTableName;

    @JSONCreator
    public CdcTruncateWithRecycleMarkTask(String schemaName, String sourceTableName, String targetTableName) {
        super(schemaName);
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        mark4RenameTable(executionContext);
    }

    private void mark4RenameTable(ExecutionContext executionContext) {
        // 如果物理表名也发生了变化，需要将新的tablePattern作为附加参数传给cdcManager
        // 如果物理表名也发生了变更，此处所有物理表已经都完成了rename(此时用户针对该逻辑表提交的任何dml操作都会报错)，cdc打标必须先于元数据变更
        // 如果物理表名未进行变更，那么tablePattern不会发生改变，Rename是一个轻量级的操作，打标的位置放到元数据变更之前或之后，都可以
        String newTbNamePattern = TableMetaChanger.buildNewTbNamePattern(executionContext, schemaName,
            sourceTableName, targetTableName);
        Map<String, Object> params = Maps.newHashMap();
        params.put(ICdcManager.TABLE_NEW_NAME, targetTableName);
        params.put(ICdcManager.TABLE_NEW_PATTERN, newTbNamePattern);
        params.putAll(executionContext.getExtraCmds());

        String renameSql =
            String.format(CDC_RECYCLE_HINTS + "RENAME TABLE `%s` TO `%s`", sourceTableName, targetTableName);
        DdlContext ddlContext = executionContext.getDdlContext();
        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, sourceTableName, SqlKind.RENAME_TABLE.name(), renameSql,
                DdlType.RENAME_TABLE, ddlContext.getJobId(), getTaskId(), DdlVisibility.Public, params);
    }
}
