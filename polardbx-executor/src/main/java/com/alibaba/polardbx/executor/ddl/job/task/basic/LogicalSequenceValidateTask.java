package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.SequenceValidator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.calcite.sql.SequenceBean;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */

@Getter
@TaskName(name = "LogicalSequenceValidateTask")
public class LogicalSequenceValidateTask extends BaseValidateTask {
    private SequenceBean sequenceBean;

    @JSONCreator
    public LogicalSequenceValidateTask(String schemaName, SequenceBean sequenceBean) {
        super(schemaName);
        this.sequenceBean = sequenceBean;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        SequenceValidator.validate(sequenceBean, executionContext, true);
    }
}
