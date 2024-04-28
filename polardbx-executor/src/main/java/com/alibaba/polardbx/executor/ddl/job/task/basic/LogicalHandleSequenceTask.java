package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gms.util.SequenceUtil;
import com.alibaba.polardbx.gms.metadb.seq.SequenceBaseRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequencesAccessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import lombok.Getter;
import org.apache.calcite.sql.SequenceBean;

import java.sql.Connection;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@Getter
@TaskName(name = "LogicalHandleSequenceTask")
public class LogicalHandleSequenceTask extends BaseGmsTask {
    private SequenceBean sequenceBean;

    @JSONCreator
    public LogicalHandleSequenceTask(String schemaName, String logicalTableName, SequenceBean sequenceBean) {
        super(schemaName, logicalTableName);
        this.sequenceBean = sequenceBean;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(Connection metaDbConn, ExecutionContext executionContext) {
        SequencesAccessor sequencesAccessor = new SequencesAccessor();
        sequencesAccessor.setConnection(metaDbConn);

        final String seqSchema = sequenceBean.getSchemaName();
        final String seqName = sequenceBean.getName();

        SequenceBaseRecord record = SequenceUtil.convert(sequenceBean, null, executionContext);

        long newSeqCacheSize = executionContext.getParamManager().getLong(ConnectionParams.NEW_SEQ_CACHE_SIZE);
        newSeqCacheSize = newSeqCacheSize < 1 ? 0 : newSeqCacheSize;

        switch (sequenceBean.getKind()) {
        case CREATE_SEQUENCE:
            sequencesAccessor.insert(record, newSeqCacheSize,
                SequenceUtil.buildFailPointInjector(executionContext));
            break;
        case ALTER_SEQUENCE:
            boolean alterWithoutTypeChange = true;

            if (sequenceBean.getToType() != null && sequenceBean.getToType() != SequenceAttribute.Type.NA) {
                Pair<SequenceBaseRecord, SequenceBaseRecord> recordPair =
                    SequenceUtil.change(sequenceBean, null, executionContext);
                if (recordPair != null) {
                    sequencesAccessor.change(recordPair, newSeqCacheSize,
                        SequenceUtil.buildFailPointInjector(executionContext));
                    alterWithoutTypeChange = false;
                }
            }

            if (alterWithoutTypeChange) {
                SequenceAttribute.Type
                    existingType = SequenceManagerProxy.getInstance().checkIfExists(seqSchema, seqName);
                /**
                 * alter语句是不改变sequence类型的时候, group sequence 只允许修改start with, time based sequence 啥都不允许修改
                 * */
                if (existingType == SequenceAttribute.Type.GROUP && sequenceBean.getStart() != null
                    || existingType != SequenceAttribute.Type.GROUP && existingType != SequenceAttribute.Type.TIME) {
                    sequencesAccessor.update(record, newSeqCacheSize);
                }
            }

            break;
        case DROP_SEQUENCE:
            if (!DynamicConfig.getInstance().isSupportDropAutoSeq()
                && TStringUtil.startsWithIgnoreCase(seqName, AUTO_SEQ_PREFIX)) {
                throw new SequenceException(
                    "A sequence associated with a table is not allowed to be dropped separately");
            }

            sequencesAccessor.delete(record);

            break;
        case RENAME_SEQUENCE:
            sequencesAccessor.rename(record);
            break;
        default:
            throw new SequenceException("Unexpected operation: " + sequenceBean.getKind());
        }
    }
}
