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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi;

import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DdlPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameTablePreparedData;
import org.jetbrains.annotations.NotNull;

public class RenameGlobalIndexPreparedData extends DdlPreparedData {

    private RenameLocalIndexPreparedData renameLocalIndexPreparedData;
    private RenameTablePreparedData indexTablePreparedData;
    private String primaryTableName;

    public RenameLocalIndexPreparedData getRenameLocalIndexPreparedData() {
        return renameLocalIndexPreparedData;
    }

    public void setRenameLocalIndexPreparedData(RenameLocalIndexPreparedData renameLocalIndexPreparedData) {
        this.renameLocalIndexPreparedData = renameLocalIndexPreparedData;
    }

    public RenameTablePreparedData getIndexTablePreparedData() {
        return indexTablePreparedData;
    }

    public void setIndexTablePreparedData(RenameTablePreparedData indexTablePreparedData) {
        this.indexTablePreparedData = indexTablePreparedData;
    }

    public String getPrimaryTableName() {
        return primaryTableName;
    }

    public void setPrimaryTableName(String primaryTableName) {
        this.primaryTableName = primaryTableName;
    }

    public String getIndexTableName() {
        return getTableName();
    }

    public void setDdlVersionId(@NotNull Long versionId) {
        super.setDdlVersionId(versionId);
        if (null != renameLocalIndexPreparedData) {
            renameLocalIndexPreparedData.setDdlVersionId(versionId);
        }
        if (null != indexTablePreparedData) {
            indexTablePreparedData.setDdlVersionId(versionId);
        }
    }
}
