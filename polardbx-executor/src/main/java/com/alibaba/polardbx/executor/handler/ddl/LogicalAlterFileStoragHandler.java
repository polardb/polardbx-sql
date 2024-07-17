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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.ddl.job.factory.AlterFileStorageAsOfTimestampJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterFileStorageBackupJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.AlterFileStoragePurgeBeforeTimestampJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterFileStorage;
import org.apache.calcite.rel.ddl.AlterFileStorageAsOfTimestamp;
import org.apache.calcite.rel.ddl.AlterFileStorageBackup;
import org.apache.calcite.rel.ddl.AlterFileStoragePurgeBeforeTimestamp;

/**
 * @author chenzilin
 */
public class LogicalAlterFileStoragHandler extends LogicalCommonDdlHandler {

    public LogicalAlterFileStoragHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterFileStorage logicalAlterFileStorage =
            (LogicalAlterFileStorage) logicalDdlPlan;
        logicalAlterFileStorage.preparedData();
        // god privilege check.
        TableValidator.checkGodPrivilege(executionContext);
        if (logicalAlterFileStorage.relDdl instanceof AlterFileStorageAsOfTimestamp) {
            return new AlterFileStorageAsOfTimestampJobFactory(logicalAlterFileStorage.getPreparedData(),
                executionContext).create();
        } else if (logicalAlterFileStorage.relDdl instanceof AlterFileStoragePurgeBeforeTimestamp) {
            return new AlterFileStoragePurgeBeforeTimestampJobFactory(logicalAlterFileStorage.getPreparedData(),
                executionContext).create();
        } else if (logicalAlterFileStorage.relDdl instanceof AlterFileStorageBackup) {
            return new AlterFileStorageBackupJobFactory(logicalAlterFileStorage.getPreparedData(),
                executionContext).create();
        } else {
            throw new AssertionError("unknown alter file storage sql");
        }
    }
}
