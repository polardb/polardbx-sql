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
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropTableWithGsiPreparedData;
import org.apache.calcite.rel.ddl.DropTable;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlNode;

import java.util.Map;

public class LogicalDropTable extends BaseDdlOperation {

    private SqlDropTable sqlDropTable;
    private DropTablePreparedData dropTablePreparedData;
    private DropTableWithGsiPreparedData dropTableWithGsiPreparedData;

    private LogicalDropTable(DropTable dropTable) {
        super(dropTable);
        this.sqlDropTable = (SqlDropTable) dropTable.sqlNode;
    }

    public boolean ifExists() {
        return this.sqlDropTable.isIfExists();
    }

    public static LogicalDropTable create(DropTable dropTable) {
        return new LogicalDropTable(dropTable);
    }

    public boolean isWithGsi() {
        return dropTableWithGsiPreparedData != null && dropTableWithGsiPreparedData.hasGsi();
    }

    public DropTablePreparedData getDropTablePreparedData() {
        return dropTablePreparedData;
    }

    public DropTableWithGsiPreparedData getDropTableWithGsiPreparedData() {
        return dropTableWithGsiPreparedData;
    }

    public void prepareData() {
        // A normal logical table or a primary table with GSIs.
        dropTablePreparedData = preparePrimaryData();

        final GsiMetaBean gsiMetaBean =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getGsi(tableName, IndexStatus.ALL);

        if (gsiMetaBean.withGsi(tableName)) {
            dropTableWithGsiPreparedData = new DropTableWithGsiPreparedData();
            dropTableWithGsiPreparedData.setPrimaryTablePreparedData(dropTablePreparedData);

            final GsiTableMetaBean gsiTableMeta = gsiMetaBean.getTableMeta().get(tableName);
            for (Map.Entry<String, GsiIndexMetaBean> gsiEntry : gsiTableMeta.indexMap.entrySet()) {
                DropGlobalIndexPreparedData indexTablePreparedData =
                    prepareGsiData(dropTablePreparedData.getTableName(), gsiEntry.getKey());
                dropTableWithGsiPreparedData.addIndexTablePreparedData(indexTablePreparedData);
            }
        }
    }

    public boolean isPurge() {
        // Forcibly drop the table instead of putting it into the recycle bin if purge.
        return sqlDropTable.isPurge();
    }

    public SqlNode getTargetTable() {
        return sqlDropTable.getTargetTable();
    }

    private DropTablePreparedData preparePrimaryData() {
        DropTablePreparedData preparedData =
            new DropTablePreparedData(schemaName, tableName, sqlDropTable.isIfExists());
        SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        try {
            TableMeta tableMeta = sm.getTable(tableName);
            preparedData.setTableVersion(tableMeta.getVersion());
        } catch (Exception ex) {
            if (!sqlDropTable.isIfExists()) {
                throw ex;
            }
        }
        return preparedData;
    }

    private DropGlobalIndexPreparedData prepareGsiData(String primaryTableName, String indexTableName) {
        return new DropGlobalIndexPreparedData(schemaName, primaryTableName, indexTableName, false);
    }

}
