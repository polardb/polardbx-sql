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

import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesExtMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableShowTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CreateGsiPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CreateGsiValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiInsertIndexMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiUpdateIndexStatusTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import lombok.Data;

@Data
public class ExecutableDdlJob4CreateGsi extends ExecutableDdlJob {

    private CreateGsiValidateTask createGsiValidateTask;
    private CreateTableAddTablesExtMetaTask createTableAddTablesExtMetaTask;
    private CreateTableAddTablesMetaTask createTableAddTablesMetaTask;
    private CreateTableShowTableMetaTask createTableShowTableMetaTask;
    private GsiInsertIndexMetaTask gsiInsertIndexMetaTask;
    private CreateGsiPhyDdlTask createGsiPhyDdlTask;
    private DdlTask lastUpdateGsiStatusTask;

    /**
     * last task
     */
    private DdlTask lastTask;

}