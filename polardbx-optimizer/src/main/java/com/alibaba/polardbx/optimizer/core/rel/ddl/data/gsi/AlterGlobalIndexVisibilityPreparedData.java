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

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class AlterGlobalIndexVisibilityPreparedData extends DdlPreparedData {
    private String primaryTableName;
    private String indexTableName;

    private String visibility;
    private boolean columnar;

    public String getPrimaryTableName() {
        return primaryTableName;
    }

    public void setPrimaryTableName(String primaryTableName) {
        this.primaryTableName = primaryTableName;
    }

    public String getIndexTableName() {
        return indexTableName;
    }

    public void setIndexTableName(String indexTableName) {
        this.indexTableName = indexTableName;
    }

    public String getVisibility() {
        return visibility;
    }

    public void setVisibility(String visibility) {
        this.visibility = visibility;
    }

    public boolean isColumnar() {
        return columnar;
    }

    public void setColumnar(boolean columnar) {
        this.columnar = columnar;
    }
}
