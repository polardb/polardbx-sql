package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.meta.misc.RepartitionMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.Map;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "RebuildTableCutOverTask")
public class RebuildTableCutOverTask extends BaseGmsTask {
    private final Map<String, String> tableNameMap;
    private final boolean autoPartition;
    private final boolean single;
    private final boolean broadcast;

    public RebuildTableCutOverTask(final String schemaName,
                                   final String logicalTableName,
                                   Map<String, String> tableNameMap,
                                   boolean autoPartition,
                                   boolean single,
                                   boolean broadcast) {
        super(schemaName, logicalTableName);
        this.tableNameMap = tableNameMap;
        this.autoPartition = autoPartition;
        this.single = single;
        this.broadcast = broadcast;
        onExceptionTryRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        LOGGER.info(
            String.format(
                "[rebuild table] start change meta during cutOver for primary table: %s.%s",
                schemaName, logicalTableName)
        );
        updateSupportedCommands(true, false, metaDbConnection);
        //allowing use hint to skip clean up stage
        final String skipCutover =
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_SKIP_CUTOVER);
        if (StringUtils.equalsIgnoreCase(skipCutover, Boolean.TRUE.toString())) {
            return;
        }

        RepartitionMetaChanger.alterTaleModifyColumnCutOver(
            metaDbConnection,
            schemaName,
            logicalTableName,
            tableNameMap,
            autoPartition,
            single,
            broadcast
        );
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        LOGGER.info(
            String.format(
                "[rebuild table] finish change meta during cutOver for primary table: %s.%s",
                schemaName, logicalTableName)
        );

        EventLogger.log(EventType.DDL_INFO, "Online modify column success");
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        //sync for CutOver should keep atomic, so we won't do notify here
        //see RepartitionSyncAction.java
    }

    @Override
    protected void updateTableVersion(Connection metaDbConnection) {
        //sync for CutOver should keep atomic, so we won't do notify here
        //see RepartitionSyncAction.java
        try {
            TableInfoManager.updateTableVersion4Repartition(schemaName, logicalTableName, metaDbConnection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
