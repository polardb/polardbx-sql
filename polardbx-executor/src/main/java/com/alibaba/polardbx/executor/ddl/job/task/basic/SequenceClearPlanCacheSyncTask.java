package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.sync.ClearPlanCacheSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.calcite.sql.SqlKind;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@TaskName(name = "SequenceClearPlanCacheSyncTask")
@Getter
public class SequenceClearPlanCacheSyncTask extends BaseSyncTask {
    final protected String seqName;
    final protected SqlKind sqlKind;

    @JSONCreator
    public SequenceClearPlanCacheSyncTask(String schemaName,
                                          String seqName, SqlKind sqlKind) {
        super(schemaName);
        this.seqName = seqName;
        this.sqlKind = sqlKind;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        if (TStringUtil.startsWithIgnoreCase(seqName, AUTO_SEQ_PREFIX) &&
            sqlKind == SqlKind.CREATE_SEQUENCE) {
            SyncManagerHelper.sync(new ClearPlanCacheSyncAction(schemaName), schemaName, SyncScope.ALL);
        }
    }
}
