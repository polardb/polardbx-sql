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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropFileStoragePreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterFileStorageAsOfTimestamp;
import org.apache.calcite.rel.ddl.AlterFileStoragePurgeBeforeTimestamp;
import org.apache.calcite.rel.ddl.DropFileStorage;

public class LogicalDropFileStorage extends BaseDdlOperation {
    private DropFileStoragePreparedData preparedData;

    public LogicalDropFileStorage(DDL ddl) {
        super(ddl);
    }

    public void preparedData() {
        DropFileStorage dropFileStorage = (DropFileStorage) relDdl;
        preparedData = new DropFileStoragePreparedData(dropFileStorage.getFileStorageName());
    }

    public DropFileStoragePreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalDropFileStorage create(DDL ddl) {
        return new LogicalDropFileStorage(ddl);
    }
}

