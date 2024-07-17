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

import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupMovePartitionPreparedData;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupMovePartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupMovePartition;

public class LogicalAlterTableGroupMovePartition extends LogicalAlterTableMovePartition {

    public LogicalAlterTableGroupMovePartition(DDL ddl) {
        super(ddl, true);
    }

    public void preparedData(ExecutionContext ec, boolean usePhysicalBackfill) {
        AlterTableGroupMovePartition alterTableGroupMovePartition = (AlterTableGroupMovePartition) relDdl;
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupMovePartition.getAst();

        SqlAlterTableGroupMovePartition sqlAlterTableGroupMovePartition =
            (SqlAlterTableGroupMovePartition) sqlAlterTableGroup.getAlters().get(0);

        String tableGroupName = alterTableGroupMovePartition.getTableGroupName();
        preparedData = new AlterTableGroupMovePartitionPreparedData();

        doPrepare(sqlAlterTableGroupMovePartition, tableGroupName, usePhysicalBackfill);
    }

    public static LogicalAlterTableGroupMovePartition create(DDL ddl) {
        return new LogicalAlterTableGroupMovePartition(AlterTablePartitionHelper.fixAlterTableGroupDdlIfNeed(ddl));
    }

    @Override
    public boolean isSupportedByFileStorage() {
        // Columnar table group do not support move partition
        AlterTableGroupMovePartition alterTableGroupMovePartition = (AlterTableGroupMovePartition) relDdl;
        String tableGroupName = alterTableGroupMovePartition.getTableGroupName();
        return !TableGroupNameUtil.isColumnarTg(tableGroupName);
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        AlterTableGroupMovePartition alterTableGroupMovePartition = (AlterTableGroupMovePartition) relDdl;
        String tableGroupName = alterTableGroupMovePartition.getTableGroupName();
        return TableGroupNameUtil.isFileStorageTg(tableGroupName);
    }

    @Override
    public boolean checkIfBindFileStorage(ExecutionContext executionContext) {
        AlterTableGroupMovePartition alterTableGroupMovePartition = (AlterTableGroupMovePartition) relDdl;
        String tableGroupName = alterTableGroupMovePartition.getTableGroupName();
        return !CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, tableGroupName);
    }
}
