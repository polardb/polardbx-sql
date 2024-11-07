package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableDropPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupDropPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupOptimizePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupTruncatePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableOptimizePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableTruncatePartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableDropPartition;
import org.apache.calcite.sql.SqlAlterTableOptimizePartition;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author chenghui.lch
 */
public class LogicalAlterTableOptimizePartition extends BaseDdlOperation {

    protected AlterTableGroupOptimizePartitionPreparedData preparedData;

    public LogicalAlterTableOptimizePartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableOptimizePartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    public void prepareData() {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();

        SqlAlterTableOptimizePartition sqlAlterTableOptimizePartition =
            (SqlAlterTableOptimizePartition) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);

        OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);

        TableMeta tableMeta = optimizerContext.getLatestSchemaManager().getTable(logicalTableName);

        PartitionInfo partitionInfo = optimizerContext.getPartitionInfoManager().getPartitionInfo(logicalTableName);

        TableGroupConfig tableGroupConfig =
            optimizerContext.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());

        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();

        Set<String> actualPartitionNames =
            getOptimizingPartitionNames(sqlAlterTableOptimizePartition, partitionInfo, tableGroupConfig);

        preparedData = new AlterTableOptimizePartitionPreparedData();
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);
        preparedData.setTableVersion(tableMeta.getVersion());
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());
        preparedData.setOptimizePartitionNames(actualPartitionNames);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    protected Set<String> getOptimizingPartitionNames(SqlAlterTableOptimizePartition sqlAlterTableOptimizePartition,
                                                      PartitionInfo partitionInfo,
                                                      TableGroupConfig tableGroupConfig) {
        Set<String> partitionNames = sqlAlterTableOptimizePartition.getPartitions().stream()
            .map(sqlNode -> ((SqlIdentifier) sqlNode).getLastName().toLowerCase()).collect(Collectors.toSet());

        String allPartitions = partitionNames.stream().filter(p -> p.equalsIgnoreCase("ALL")).findFirst().orElse(null);
        if (allPartitions != null) {
            return PartitionInfoUtil.getAllPartitionGroupNames(tableGroupConfig);
        }

        return PartitionInfoUtil.checkAndExpandPartitions(partitionInfo, tableGroupConfig, partitionNames,
            sqlAlterTableOptimizePartition.isSubPartition());
    }

    public AlterTableGroupOptimizePartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableOptimizePartition create(DDL ddl) {
        return new LogicalAlterTableOptimizePartition(ddl);
    }

}
