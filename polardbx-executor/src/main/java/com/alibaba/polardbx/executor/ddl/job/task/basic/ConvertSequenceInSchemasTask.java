package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.ClearSeqCacheSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.util.SeqTypeUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.Type;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */

@Getter
@TaskName(name = "ConvertSequenceInSchemasTask")
public class ConvertSequenceInSchemasTask extends BaseGmsTask {
    final List<String> schemaNames;
    Type fromType;
    Type toType;

    @JSONCreator
    public ConvertSequenceInSchemasTask(List<String> schemaNames, Type fromType, Type toType) {
        //just fill a causal db name
        super("polardbx", "none");
        this.schemaNames = schemaNames;
        this.fromType = fromType;
        this.toType = toType;
    }

    @Override
    public void executeImpl(Connection metaDbConn, ExecutionContext executionContext) {
        for (String schema : schemaNames) {
            convert(schema, fromType, toType, metaDbConn);
        }
    }

    @Override
    public void onExecutionSuccess(ExecutionContext executionContext) {
        boolean newSeqNotInvolved = fromType != Type.NEW && toType != Type.NEW;
        for (String schema : schemaNames) {
            if (SeqTypeUtil.isNewSeqSupported(schema) || newSeqNotInvolved) {
                try {
                    SyncManagerHelper.sync(new ClearSeqCacheSyncAction(schema, null, true, false), SyncScope.ALL);
                } catch (Exception e) {
                    throw new TddlNestableRuntimeException(e);
                }
            }

        }
    }

    private void convert(String schemaName, Type fromType, Type toType, Connection metaDbConn) {
        boolean newSeqNotInvolved = fromType != Type.NEW && toType != Type.NEW;
        if (SeqTypeUtil.isNewSeqSupported(schemaName) || newSeqNotInvolved) {
            try {
                SequencesAccessor.change(schemaName, fromType, toType, metaDbConn);
            } catch (Exception e) {
                throw new TddlNestableRuntimeException(e);
            }
        }
    }
}
