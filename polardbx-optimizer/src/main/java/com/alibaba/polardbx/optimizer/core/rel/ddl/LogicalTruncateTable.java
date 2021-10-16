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

import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiTableMetaBean;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.TruncateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.TruncateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.TruncateTableWithGsiPreparedData;
import org.apache.calcite.rel.ddl.TruncateTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTruncateTable;

import java.util.Map;

public class LogicalTruncateTable extends BaseDdlOperation {

    private SqlTruncateTable sqlTruncateTable;
    private TruncateTablePreparedData truncateTablePreparedData;
    private TruncateTableWithGsiPreparedData truncateTableWithGsiPreparedData;

    public LogicalTruncateTable(TruncateTable truncateTable) {
        super(truncateTable);
        this.sqlTruncateTable = (SqlTruncateTable) truncateTable.sqlNode;
    }

    public static LogicalTruncateTable create(TruncateTable truncateTable) {
        return new LogicalTruncateTable(truncateTable);
    }

    public boolean isWithGsi() {
        return truncateTableWithGsiPreparedData != null && truncateTableWithGsiPreparedData.hasGsi();
    }

    public TruncateTablePreparedData getTruncateTablePreparedData() {
        return truncateTablePreparedData;
    }

    public TruncateTableWithGsiPreparedData getTruncateTableWithGsiPreparedData() {
        return truncateTableWithGsiPreparedData;
    }

    public void prepareData() {
        // A normal logical table or a primary table with GSIs.
        truncateTablePreparedData = preparePrimaryData();

        final GsiMetaBean gsiMetaBean =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);

        if (gsiMetaBean.withGsi(tableName)) {
            truncateTableWithGsiPreparedData = new TruncateTableWithGsiPreparedData();
            truncateTableWithGsiPreparedData.setPrimaryTablePreparedData(truncateTablePreparedData);

            final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
            for (Map.Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                TruncateGlobalIndexPreparedData indexTablePreparedData =
                    prepareGsiData(truncateTablePreparedData.getTableName(), gsiEntry.getKey());
                truncateTableWithGsiPreparedData.addIndexTablePreparedData(indexTablePreparedData);
            }
        }
    }

    public boolean isPurge() {
        // Forcibly truncate the table instead of putting it into the recycle bin if purge.
        return sqlTruncateTable.isPurge();
    }

    public SqlNode getTargetTable() {
        return sqlTruncateTable.getTargetTable();
    }

    private TruncateTablePreparedData preparePrimaryData() {
        TruncateTablePreparedData preparedData = new TruncateTablePreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);

        return preparedData;
    }

    private TruncateGlobalIndexPreparedData prepareGsiData(String primaryTableName, String indexTableName) {
        TruncateGlobalIndexPreparedData preparedData = new TruncateGlobalIndexPreparedData();

        TruncateTablePreparedData indexTablePreparedData = new TruncateTablePreparedData();
        indexTablePreparedData.setSchemaName(schemaName);
        indexTablePreparedData.setTableName(indexTableName);

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(indexTableName);
        preparedData.setIndexTablePreparedData(indexTablePreparedData);
        preparedData.setPrimaryTableName(primaryTableName);

        return preparedData;
    }

}
