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

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSetTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableSetTableGroup;
import org.apache.calcite.sql.SqlAlterTableSetTableGroup;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class LogicalAlterTableSetTableGroup extends BaseDdlOperation {

    private AlterTableSetTableGroupPreparedData preparedData;

    public LogicalAlterTableSetTableGroup(DDL ddl) {
        super(ddl, ((AlterTableSetTableGroup) ddl).getObjectNames());
    }

    public void preparedData(ExecutionContext ec) {
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
    }

    public AlterTableSetTableGroupPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableSetTableGroup create(DDL ddl) {
        return new LogicalAlterTableSetTableGroup(ddl);
    }

}
