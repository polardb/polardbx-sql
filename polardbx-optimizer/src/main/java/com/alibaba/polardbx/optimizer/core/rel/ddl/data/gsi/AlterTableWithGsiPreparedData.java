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
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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

    //if value=true, the no-exist tablegroup will be created before create table/gsi
    private Map<String, Boolean> relatedTableGroupInfo = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

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

    @Override
    public void setDdlVersionId(@NotNull Long versionId) {
        super.setDdlVersionId(versionId);
        if (null != clusteredIndexPrepareData) {
            clusteredIndexPrepareData.forEach(p -> p.setDdlVersionId(versionId));
        }
        if (null != globalIndexPreparedData) {
            globalIndexPreparedData.forEach(p -> p.setDdlVersionId(versionId));
        }
        if (null != createIndexWithGsiPreparedData) {
            createIndexWithGsiPreparedData.setDdlVersionId(versionId);
        }
        if (null != dropIndexWithGsiPreparedData) {
            dropIndexWithGsiPreparedData.setDdlVersionId(versionId);
        }
        if (null != renameGlobalIndexPreparedData) {
            renameGlobalIndexPreparedData.setDdlVersionId(versionId);
        }
        if (null != globalIndexVisibilityPreparedData) {
            globalIndexVisibilityPreparedData.setDdlVersionId(versionId);
        }
        if (null != renameLocalIndexPreparedData) {
            renameLocalIndexPreparedData.setDdlVersionId(versionId);
        }
    }
}
