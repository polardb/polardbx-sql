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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropTablePreparedData;

public class DropGlobalIndexPreparedData extends DdlPreparedData {

    private DropTablePreparedData indexTablePreparedData;
    private String primaryTableName;
    private boolean ifExists;

    public DropGlobalIndexPreparedData(final String schemaName,
                                       final String primaryTableName,
                                       final String indexTableName,
                                       final boolean ifExists) {
        setSchemaName(schemaName);
        setTableName(indexTableName);
        DropTablePreparedData indexTablePreparedData =
            new DropTablePreparedData(schemaName, indexTableName, ifExists);
        indexTablePreparedData.setWithHint(false);
        this.indexTablePreparedData = indexTablePreparedData;
        this.primaryTableName = primaryTableName;
    }

    public DropTablePreparedData getIndexTablePreparedData() {
        return indexTablePreparedData;
    }

    public void setIndexTablePreparedData(DropTablePreparedData indexTablePreparedData) {
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

    public boolean isIfExists() {
        return this.ifExists;
    }

    public void setIfExists(final boolean ifExists) {
        this.ifExists = ifExists;
    }
}
