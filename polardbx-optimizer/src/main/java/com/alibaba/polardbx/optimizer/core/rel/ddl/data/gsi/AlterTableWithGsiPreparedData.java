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

import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DdlPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameLocalIndexPreparedData;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class AlterTableWithGsiPreparedData extends DdlPreparedData {

    // TODO(moyi) merge duplication on alter-table and create/drop index
    private List<AlterTablePreparedData> clusteredIndexPrepareData = new ArrayList<>();
    private List<AlterTablePreparedData> globalIndexPreparedData = new ArrayList<>();

    private CreateIndexWithGsiPreparedData createIndexWithGsiPreparedData;
    private DropIndexWithGsiPreparedData dropIndexWithGsiPreparedData;
    private RenameGlobalIndexPreparedData renameGlobalIndexPreparedData;
    private AlterGlobalIndexVisibilityPreparedData globalIndexVisibilityPreparedData;
    private RenameLocalIndexPreparedData renameLocalIndexPreparedData;

    public void addAlterGlobalIndexPreparedData(AlterTablePreparedData alterGlobalIndexPreparedData) {
        this.globalIndexPreparedData.add(alterGlobalIndexPreparedData);
    }

    // FIXME(moyi) construct in constructor, instead of here
    public CreateIndexWithGsiPreparedData getOrNewCreateIndexWithGsi() {
        if (this.createIndexWithGsiPreparedData == null) {
            this.createIndexWithGsiPreparedData = new CreateIndexWithGsiPreparedData();
        }
        return this.createIndexWithGsiPreparedData;
    }

    // FIXME(moyi) construct in constructor, instead of here
    public DropIndexWithGsiPreparedData getOrNewDropIndexWithGsi() {
        if (this.dropIndexWithGsiPreparedData == null) {
            this.dropIndexWithGsiPreparedData = new DropIndexWithGsiPreparedData();
        }
        return this.dropIndexWithGsiPreparedData;
    }

    public void addAlterClusterIndex(AlterTablePreparedData alter) {
        this.clusteredIndexPrepareData.add(alter);
    }

    public boolean hasGsi() {
        return createIndexWithGsiPreparedData != null || dropIndexWithGsiPreparedData != null
            || renameGlobalIndexPreparedData != null || globalIndexPreparedData.size() > 0;
    }

}
