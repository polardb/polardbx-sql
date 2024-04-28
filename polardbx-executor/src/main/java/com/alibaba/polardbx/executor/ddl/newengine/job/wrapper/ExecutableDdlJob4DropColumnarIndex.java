package com.alibaba.polardbx.executor.ddl.newengine.job.wrapper;

import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDropColumnarIndexTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.CciSchemaEvolutionTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.DropColumnarTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.DropMockColumnarIndexTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.DropColumnarTableHideTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiDropCleanUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateGsiExistenceTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import lombok.Data;

/**
 * plan for dropping columnar index
 */
@Data
public class ExecutableDdlJob4DropColumnarIndex extends ExecutableDdlJob {

    private ValidateGsiExistenceTask validateTask;

    private DropColumnarTableHideTableMetaTask dropColumnarTableHideTableMetaTask;

    private GsiDropCleanUpTask gsiDropCleanUpTask;

    private TableSyncTask tableSyncTaskAfterCleanUpGsiIndexesMeta;

    private CdcDropColumnarIndexTask cdcDropColumnarIndexTask;

    private DropMockColumnarIndexTask dropMockColumnarIndexTask;

    private DropColumnarTableRemoveMetaTask dropColumnarTableRemoveMetaTask;

    private CciSchemaEvolutionTask cciSchemaEvolutionTask;

    private TableSyncTask finalSyncTask;
}