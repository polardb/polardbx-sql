/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.newengine.job.wrapper;

import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.DropGsiPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.DropGsiTableHideTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.DropGsiTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.DropPartitionGsiPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiDropCleanUpTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateGsiExistenceTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import lombok.Data;

/**
 * plain drop GSI
 */
@Data
public class ExecutableDdlJob4DropPartitionGsi extends ExecutableDdlJob {

    /**
     * first task in Drop GSI
     */
    private ValidateGsiExistenceTask validateTask;

    private DropGsiTableHideTableMetaTask dropGsiTableHideTableMetaTask;

    private DropPartitionGsiPhyDdlTask dropGsiPhyDdlTask;

    private GsiDropCleanUpTask gsiDropCleanUpTask;

    private TableSyncTask tableSyncTaskAfterCleanUpGsiIndexesMeta;

    private DropTableRemoveMetaTask dropGsiTableRemoveMetaTask;

    /**
     * last task in Drop GSI
     */
    private TableSyncTask finalSyncTask;

}
