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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupRenamePartitionPreparedData;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupRenamePartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupRenamePartition;

public class LogicalAlterTableGroupRenamePartition extends LogicalAlterTableRenamePartition {

    public LogicalAlterTableGroupRenamePartition(DDL ddl) {
        super(ddl, true);
    }

    public static LogicalAlterTableGroupRenamePartition create(DDL ddl) {
        return new LogicalAlterTableGroupRenamePartition(AlterTablePartitionHelper.fixAlterTableGroupDdlIfNeed(ddl));
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        AlterTableGroupRenamePartition alterTableGroupRenamePartition = (AlterTableGroupRenamePartition) relDdl;
        String tableGroupName = alterTableGroupRenamePartition.getTableGroupName();
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST, "unarchive tablegroup " + tableGroupName);
    }

    @Override
    public void prepareData(ExecutionContext ec) {
        preparedData = new AlterTableGroupRenamePartitionPreparedData();
        AlterTableGroupRenamePartition alterTableGroupRenamePartition = (AlterTableGroupRenamePartition) relDdl;
        String tableGroupName = alterTableGroupRenamePartition.getTableGroupName();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupRenamePartition.getAst();
        assert sqlAlterTableGroup.getAlters().size() == 1;
        SqlAlterTableGroupRenamePartition sqlAlterTableGroupRenamePartition =
            (SqlAlterTableGroupRenamePartition) sqlAlterTableGroup.getAlters().get(0);
        preparedData.setSchemaName(schemaName);
        preparedData.setChangePartitionsPair(sqlAlterTableGroupRenamePartition.getChangePartitionsPair());
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSubPartitionRename(sqlAlterTableGroupRenamePartition.isSubPartitionsRename());
        preparedData.setRenameNothing(preparedData.partitionNameNoChange());
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        AlterTableGroupRenamePartition alterTableGroupRenamePartition = (AlterTableGroupRenamePartition) relDdl;
        String tableGroupName = alterTableGroupRenamePartition.getTableGroupName();
        return TableGroupNameUtil.isFileStorageTg(tableGroupName);
    }

    @Override
    public boolean checkIfBindFileStorage(ExecutionContext executionContext) {
        AlterTableGroupRenamePartition alterTableGroupRenamePartition = (AlterTableGroupRenamePartition) relDdl;
        String tableGroupName = alterTableGroupRenamePartition.getTableGroupName();
        return !CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, tableGroupName);
    }
}
