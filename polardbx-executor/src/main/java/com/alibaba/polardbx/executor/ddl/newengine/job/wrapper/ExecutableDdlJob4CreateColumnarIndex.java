package com.alibaba.polardbx.executor.ddl.newengine.job.wrapper;

import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableShowTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcCreateColumnarIndexTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.*;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CciUpdateIndexStatusTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import lombok.Data;

@Data
public class ExecutableDdlJob4CreateColumnarIndex extends ExecutableDdlJob {

    private CreateColumnarIndexValidateTask createColumnarIndexValidateTask;
    private AddColumnarTablesPartitionInfoMetaTask addColumnarTablesPartitionInfoMetaTask;
    private CdcCreateColumnarIndexTask cdcCreateColumnarIndexTask;
    private CreateMockColumnarIndexTask createMockColumnarIndexTask;
    private CreateTableShowTableMetaTask createTableShowTableMetaTask;
    private InsertColumnarIndexMetaTask insertColumnarIndexMetaTask;
    private WaitColumnarTableCreationTask waitColumnarTableCreationTask;
    private CciUpdateIndexStatusTask changeCreatingToChecking;
    private CreateCheckCciTask createCheckCciTask;
    private CciUpdateIndexStatusTask cciUpdateIndexStatusTask;

    /**
     * last task
     */
    private DdlTask lastTask;

}