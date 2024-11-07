package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupOptimizePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupTruncatePartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupOptimizePartition;
import org.apache.calcite.rel.ddl.AlterTableGroupTruncatePartition;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableOptimizePartition;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;

import java.util.List;
import java.util.Set;

public class LogicalAlterTableGroupOptimizePartition extends LogicalAlterTableOptimizePartition {

    public LogicalAlterTableGroupOptimizePartition(DDL ddl) {
        super(ddl, true);
    }

    public void prepareData(ExecutionContext executionContext) {
        AlterTableGroupOptimizePartition alterTableGroupOptimizePartition = (AlterTableGroupOptimizePartition) relDdl;
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupOptimizePartition.getAst();

        SqlAlterTableOptimizePartition sqlAlterTableOptimizePartition =
            (SqlAlterTableOptimizePartition) sqlAlterTableGroup.getAlters().get(0);

        String tableGroupName = alterTableGroupOptimizePartition.getTableGroupName();

        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(null).getTableGroupInfoManager().getTableGroupConfigByName(tableGroupName);
        String firstTableInGroup = tableGroupConfig.getAllTables().get(0);
        PartitionInfo partitionInfo =
            executionContext.getSchemaManager().getTable(firstTableInGroup).getPartitionInfo();

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName, tableGroupName);

        Set<String> actualPartitionNames =
            getOptimizingPartitionNames(sqlAlterTableOptimizePartition, partitionInfo, tableGroupConfig);

        preparedData = new AlterTableGroupOptimizePartitionPreparedData();
        preparedData.setSchemaName(schemaName);
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setOptimizePartitionNames(actualPartitionNames);
    }

    public static LogicalAlterTableGroupOptimizePartition create(DDL ddl) {
        return new LogicalAlterTableGroupOptimizePartition(AlterTablePartitionHelper.fixAlterTableGroupDdlIfNeed(ddl));
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        final AlterTableGroupTruncatePartition alterTableGroupTruncatePartition =
            (AlterTableGroupTruncatePartition) relDdl;
        final String tableGroupName = alterTableGroupTruncatePartition.getTableGroupName();

        return TableGroupNameUtil.isFileStorageTg(tableGroupName);
    }
}
