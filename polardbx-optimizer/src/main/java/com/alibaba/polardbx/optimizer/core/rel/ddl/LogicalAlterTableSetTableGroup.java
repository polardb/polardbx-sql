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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupUtils;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSetTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableSetTableGroup;
import org.apache.calcite.sql.SqlAlterTableSetTableGroup;

import java.util.Objects;

public class LogicalAlterTableSetTableGroup extends BaseDdlOperation {

    private AlterTableSetTableGroupPreparedData preparedData;

    public LogicalAlterTableSetTableGroup(DDL ddl) {
        super(ddl, ((AlterTableSetTableGroup) ddl).getObjectNames());
    }

    @Override
    public boolean isSupportedByCci(ExecutionContext ec) {
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "not support tablegroup in non-partitioning database");
        }
        // not allow change columnar tablegroup to others
        AlterTableSetTableGroup alterTableSetTableGroup = (AlterTableSetTableGroup) relDdl;
        String tableGroupName = alterTableSetTableGroup.getTableGroupName();
        if (StringUtils.isEmpty(tableGroupName)) {
            return true;
        }

        if (!TableGroupNameUtil.isColumnarTg(tableGroupName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "DDL of " + schemaName + "." + tableName + " involves file storage.");
        }

        return true;
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + tableName);
    }

    public void preparedData(ExecutionContext ec) {
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "not support tablegroup in non-partitioning database");
        }
        AlterTableSetTableGroup alterTableSetTableGroup = (AlterTableSetTableGroup) relDdl;
        String tableGroupName = alterTableSetTableGroup.getTableGroupName();

        TableMeta tableMeta = ec.getSchemaManager(schemaName).getTable(tableName);
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
        if (partitionInfo.isGsiBroadcastOrBroadcast()) {
            throw new TddlRuntimeException(ErrorCode.ERR_CHANGE_TABLEGROUP_FOR_BROADCAST_TABLE,
                "change tablegroup of broadcast table is not allow");
        }
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
        JoinGroupInfoRecord joinGroupInfoRecord =
            JoinGroupUtils.getJoinGroupInfoByTable(schemaName, primaryTableName, null);
        String joinGroupName = joinGroupInfoRecord == null ? null : joinGroupInfoRecord.joinGroupName;
        preparedData.setOriginalJoinGroup(joinGroupName);
        preparedData.setSourceSql(((SqlAlterTableSetTableGroup) alterTableSetTableGroup.getSqlNode()).getSourceSql());
        preparedData.setForce(alterTableSetTableGroup.isForce());
        preparedData.setImplicit(alterTableSetTableGroup.isImplicit());
    }

    public AlterTableSetTableGroupPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableSetTableGroup create(DDL ddl) {
        return new LogicalAlterTableSetTableGroup(ddl);
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        AlterTableSetTableGroup alterTableSetTableGroup = (AlterTableSetTableGroup) relDdl;
        String tableGroupName = alterTableSetTableGroup.getTableGroupName();
        if (TableGroupNameUtil.isFileStorageTg(tableGroupName)) {
            return true;
        }

        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        if (Engine.isFileStore(tableMeta.getEngine())) {
            return true;
        }
        return false;
    }
}
