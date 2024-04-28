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

import com.alibaba.polardbx.optimizer.core.rel.ddl.data.ClearFileStoragePreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.ClearFileStorage;

public class LogicalClearFileStorage extends BaseDdlOperation {
    private ClearFileStoragePreparedData preparedData;

    public LogicalClearFileStorage(DDL ddl) {
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
        ClearFileStorage clearFileStorage = (ClearFileStorage) relDdl;
        preparedData = new ClearFileStoragePreparedData(clearFileStorage.getFileStorageName());
    }

    public ClearFileStoragePreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalClearFileStorage create(DDL ddl) {
        return new LogicalClearFileStorage(ddl);
    }
}
