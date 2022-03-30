package com.alibaba.polardbx.executor.ddl.newengine.job.wrapper;

import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import lombok.Data;

@Data
public class ExecutableDdlJob4AlterTable extends ExecutableDdlJob {
    /**
     * first task in Alter Table
     */
    private BaseValidateTask tableValidateTask;

    /**
     * last task in Alter Table
     */
    private TableSyncTask tableSyncTask;
}
