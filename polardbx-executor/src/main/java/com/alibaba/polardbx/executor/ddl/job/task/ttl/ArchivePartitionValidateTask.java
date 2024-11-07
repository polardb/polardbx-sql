package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.List;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "ArchivePartitionValidateTask")
public class ArchivePartitionValidateTask extends BaseValidateTask {
    protected final String ossTableName;
    protected final String tmpTableSchema;
    protected final String tmpTableName;
    protected final List<String> partNames;

    public ArchivePartitionValidateTask(String schemaName, String ossTableName,
                                        String tmpTableSchema, String tmpTableName,
                                        List<String> partNames) {
        super(schemaName);
        this.ossTableName = ossTableName;
        this.tmpTableSchema = tmpTableSchema;
        this.tmpTableName = tmpTableName;
        this.partNames = partNames;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        CheckOSSArchiveUtil.checkColumnConsistency(schemaName, ossTableName, tmpTableSchema, tmpTableName);

        CheckOSSArchiveUtil.validateArchivePartitions(tmpTableSchema, tmpTableName, partNames, executionContext);
    }
}
