package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gms.util.SequenceUtil;
import com.alibaba.polardbx.executor.sync.SequenceSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@Getter
@TaskName(name = "ResetSequence4TruncateTableTask")
public class ResetSequence4TruncateTableTask extends BaseDdlTask {
    final String logicalTableName;
    final String schemaName;

    @JSONCreator
    public ResetSequence4TruncateTableTask(String schemaName, String logicalTableName) {
        super(schemaName);
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void duringTransaction(Connection metaDbConn, ExecutionContext executionContext) {
        final String sequenceName = AUTO_SEQ_PREFIX + logicalTableName;
        try {
            SequenceUtil.resetSequence4TruncateTable(schemaName, logicalTableName, metaDbConn, executionContext);
            SyncManagerHelper.sync(new SequenceSyncAction(schemaName, sequenceName), schemaName,
                SyncScope.CURRENT_ONLY);
        } catch (Exception e) {
            LOGGER.error(String.format(
                "error occurs while reset sequence, schemaName:%s, tableNames:%s, sequenceName:%s", schemaName,
                logicalTableName, sequenceName), e);
        }
        LOGGER.info(String.format(
            "reset sequence successful, schemaName:%s, tableNames:%s, sequenceName:%s", schemaName, logicalTableName,
            sequenceName));
    }
}
