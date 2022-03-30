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

import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSetTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableSetTableGroup;

import java.util.Objects;

public class LogicalAlterTableSetTableGroup extends BaseDdlOperation {

    private AlterTableSetTableGroupPreparedData preparedData;

    public LogicalAlterTableSetTableGroup(DDL ddl) {
        super(ddl);
    }

    public void preparedData() {
        AlterTableSetTableGroup alterTableSetTableGroup = (AlterTableSetTableGroup) relDdl;
        String tableGroupName = alterTableSetTableGroup.getTableGroupName();
        String tableName = alterTableSetTableGroup.getTableName().toString();

        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
        Long curTableGroupId = partitionInfo.getTableGroupId();
        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig curTableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigById(curTableGroupId);

        preparedData = new AlterTableSetTableGroupPreparedData();
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(tableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setOriginalTableGroup(curTableGroupConfig.getTableGroupRecord().getTg_name());
        String primaryTableName;
        if (tableMeta.isGsi()) {
            //all the gsi table version change will be behavior by primary table
            assert
                tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
            primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(primaryTableName);
        } else {
            primaryTableName = tableName;
        }
        preparedData.setPrimaryTableName(primaryTableName);
        preparedData.setTableVersion(tableMeta.getVersion());
    }

    public AlterTableSetTableGroupPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableSetTableGroup create(DDL ddl) {
        return new LogicalAlterTableSetTableGroup(ddl);
    }

}
