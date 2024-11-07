package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.serializable.SerializableClassMapper;

import java.util.List;

/**
 * @author chenghui.lch
 */
public abstract class AbstractTtlJobTask extends BaseDdlTask implements TtlJobTask {

    /**
     * The ttl table name
     */
    protected String logicalTableName;

    /**
     * The whole ttl context of a ttl job
     */
    protected TtlJobContext jobContext;

    public AbstractTtlJobTask(String schemaName, String logicalTableName) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
    }

    @Override
    public TtlJobContext getJobContext() {
        return jobContext;
    }

    @Override
    public void setJobContext(TtlJobContext jobContext) {
        this.jobContext = jobContext;
    }

    public String getLogicalTableName() {
        return logicalTableName;
    }

    public void setLogicalTableName(String logicalTableName) {
        this.logicalTableName = logicalTableName;
    }
}
