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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateLocalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DdlPreparedData;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

@Data
public class CreateIndexWithGsiPreparedData extends DdlPreparedData {

    private CreateGlobalIndexPreparedData globalIndexPreparedData;
    private List<CreateLocalIndexPreparedData> localIndexPreparedDataList = new ArrayList<>();

    public boolean hasLocalIndex() {
        return GeneralUtil.isNotEmpty(localIndexPreparedDataList);
    }

    public boolean hasGlobalIndex() {
        return globalIndexPreparedData != null;
    }

    public void addLocalIndexPreparedData(CreateLocalIndexPreparedData localIndex) {
        this.localIndexPreparedDataList.add(localIndex);
    }

    public void setDdlVersionId(@NotNull Long versionId) {
        super.setDdlVersionId(versionId);
        if (null != globalIndexPreparedData) {
            globalIndexPreparedData.setDdlVersionId(versionId);
        }
    }
}
