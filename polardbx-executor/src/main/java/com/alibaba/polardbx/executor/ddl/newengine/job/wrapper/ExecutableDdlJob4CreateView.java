package com.alibaba.polardbx.executor.ddl.newengine.job.wrapper;

import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateViewAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateViewSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ValidateCreateViewTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcCreateViewMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import lombok.Data;

/**
 * @author chenghui.lch
 */
@Data
public class ExecutableDdlJob4CreateView extends ExecutableDdlJob {
    private ValidateCreateViewTask validateCreateViewTask;
    private CreateViewAddMetaTask createViewAddMetaTask;
    private CdcCreateViewMarkTask cdcCreateViewMarkTask;
    private CreateViewSyncTask createViewSyncTask;
}