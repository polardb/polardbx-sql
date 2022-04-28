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

package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionTableType;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPartitionBy;

import java.util.ArrayList;
import java.util.List;

public class CreatePartitionTableBuilder extends CreateTableBuilder {

    final PartitionTableType tblType;

    public CreatePartitionTableBuilder(DDL ddl, CreateTablePreparedData preparedData, ExecutionContext executionContext,
                                       PartitionTableType tblType) {
        super(ddl, preparedData, executionContext);
        this.tblType = tblType;
    }

    @Override
    public void buildTableRuleAndTopology() {
        this.tableTopology = PartitionInfoUtil.buildTargetTablesFromPartitionInfo(getPartitionInfo());
    }

    @Override
    public PartitionInfo getPartitionInfo() {
        if (partitionInfo == null) {
            partitionInfo = buildPartitionInfo();
        }
        this.tableTopology = PartitionInfoUtil.buildTargetTablesFromPartitionInfo(partitionInfo);
        return partitionInfo;
    }

    protected PartitionInfo buildPartitionInfo() {
        String tbName = null;
        TableMeta tableMeta = null;
        List<ColumnMeta> allColMetas = null;
        List<ColumnMeta> pkColMetas = null;
        String tableGroupName = preparedData.getTableGroupName() == null ? null :
            ((SqlIdentifier) preparedData.getTableGroupName()).getLastName();
        PartitionInfo partitionInfo = null;
        tableMeta = preparedData.getTableMeta();
        tbName = preparedData.getTableName();
        allColMetas = tableMeta.getAllColumns();
        pkColMetas = new ArrayList<>(tableMeta.getPrimaryKey());

        if (preparedData.getLocality() != null && tblType != PartitionTableType.SINGLE_TABLE) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                String.format("%s with locality", tblType.getTableTypeName()));
        }

        PartitionTableType partitionTableType = tblType;
        if (preparedData.isGsi() && preparedData.isBroadcast()) {
            partitionTableType = PartitionTableType.GSI_BROADCAST_TABLE;
        } else if (preparedData.isGsi() && !preparedData.isSharding() && preparedData.getPartitioning() == null) {
            partitionTableType = PartitionTableType.GSI_SINGLE_TABLE;
        }
        partitionInfo =
            PartitionInfoBuilder.buildPartitionInfoByPartDefAst(preparedData.getSchemaName(), tbName, tableGroupName,
                (SqlPartitionBy) preparedData.getPartitioning(), preparedData.getPartBoundExprInfo(), pkColMetas,
                allColMetas, partitionTableType, executionContext);
        partitionInfo.setTableType(partitionTableType);

        // Set auto partition flag only on primary table.
        if (tblType == PartitionTableType.PARTITION_TABLE) {
            assert relDdl.sqlNode instanceof SqlCreateTable;
            partitionInfo.setPartFlags(
                ((SqlCreateTable) relDdl.sqlNode).isAutoPartition() ? TablePartitionRecord.FLAG_AUTO_PARTITION : 0);
            // TODO(moyi) write a builder for autoFlag
            int autoFlag = ((SqlCreateTable) relDdl.sqlNode).isAutoSplit() ?
                TablePartitionRecord.PARTITION_AUTO_BALANCE_ENABLE_ALL : 0;
            partitionInfo.setAutoFlag(autoFlag);
        }

        return partitionInfo;
    }
}
