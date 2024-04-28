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

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DdlPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropTablePreparedData;

public class DropGlobalIndexPreparedData extends DdlPreparedData {

    private DropTablePreparedData indexTablePreparedData;
    private String primaryTableName;
    private boolean ifExists;
    private boolean repartition;
    private String repartitionTableName;
    private boolean isColumnar;
    private String originalIndexName;

    public DropGlobalIndexPreparedData(final String schemaName,
                                       final String primaryTableName,
                                       final String indexTableName,
                                       final boolean ifExists) {
        setSchemaName(schemaName);
        setTableName(indexTableName);
        DropTablePreparedData indexTablePreparedData =
            new DropTablePreparedData(schemaName, indexTableName, ifExists);
        try {
            TableMeta gsiTableMeta =
                OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(indexTableName);
            indexTablePreparedData.setTableVersion(gsiTableMeta.getVersion());
            this.isColumnar = gsiTableMeta.isColumnar();
        } catch (Exception ex) {
            //ignore, i.e repartition the indexTableName is not created yet
        }
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

    public boolean isRepartition() {
        return this.repartition;
    }

    public void setRepartition(boolean repartition) {
        this.repartition = repartition;
    }

    public boolean isColumnar() {
        return isColumnar;
    }

    public void setRepartitionTableName(String repartitionTableName) {
        this.repartitionTableName = repartitionTableName;
    }

    public String getRepartitionTableName() {
        return repartitionTableName;
    }

    public String getOriginalIndexName() {
        return originalIndexName;
    }

    public void setOriginalIndexName(String originalIndexName) {
        this.originalIndexName = originalIndexName;
    }
}
