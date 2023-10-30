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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterFileStoragePreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterFileStorageAsOfTimestamp;
import org.apache.calcite.rel.ddl.AlterFileStorageBackup;
import org.apache.calcite.rel.ddl.AlterFileStoragePurgeBeforeTimestamp;

/**
 * @author chenzilin
 */
public class LogicalAlterFileStorage extends BaseDdlOperation {

    private AlterFileStoragePreparedData preparedData;

    public LogicalAlterFileStorage(DDL ddl) {
        super(ddl);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    public void preparedData() {
        if (relDdl instanceof AlterFileStorageAsOfTimestamp) {
            AlterFileStorageAsOfTimestamp alterFileStorageAsOfTimestamp = (AlterFileStorageAsOfTimestamp) relDdl;
            preparedData = new AlterFileStoragePreparedData(alterFileStorageAsOfTimestamp.getFileStorageName(),
                alterFileStorageAsOfTimestamp.getTimestamp());
        } else if (relDdl instanceof AlterFileStoragePurgeBeforeTimestamp) {
            AlterFileStoragePurgeBeforeTimestamp alterFileStoragePurgeBeforeTimestamp =
                (AlterFileStoragePurgeBeforeTimestamp) relDdl;
            preparedData = new AlterFileStoragePreparedData(alterFileStoragePurgeBeforeTimestamp.getFileStorageName(),
                alterFileStoragePurgeBeforeTimestamp.getTimestamp());
        } else if (relDdl instanceof AlterFileStorageBackup) {
            AlterFileStorageBackup alterFileStorageBackup =
                (AlterFileStorageBackup) relDdl;
            preparedData = new AlterFileStoragePreparedData(alterFileStorageBackup.getFileStorageName());
        }
    }

    public AlterFileStoragePreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterFileStorage create(DDL ddl) {
        return new LogicalAlterFileStorage(ddl);
    }

}
