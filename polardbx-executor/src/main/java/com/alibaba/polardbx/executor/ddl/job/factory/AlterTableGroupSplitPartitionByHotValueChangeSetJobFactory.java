package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyExecutorInitTask;
import com.alibaba.polardbx.executor.ddl.job.task.changset.ChangeSetApplyFinishTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupItemPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author wumu
 */
public class AlterTableGroupSplitPartitionByHotValueChangeSetJobFactory extends AlterTableGroupChangeSetJobFactory {
    final AlterTableGroupSplitPartitionByHotValuePreparedData parentPrepareData;

    public AlterTableGroupSplitPartitionByHotValueChangeSetJobFactory(DDL ddl,
                                                                      AlterTableGroupSplitPartitionByHotValuePreparedData parentPrepareData,
                                                                      AlterTableGroupItemPreparedData preparedData,
                                                                      List<PhyDdlTableOperation> phyDdlTableOperations,
                                                                      TreeMap<String, List<List<String>>> tableTopology,
                                                                      Map<String, Set<String>> targetTableTopology,
                                                                      Map<String, Set<String>> sourceTableTopology,
                                                                      Map<String, Pair<String, String>> orderedTargetTableLocations,
                                                                      String targetPartition, boolean skipBackfill,
                                                                      ChangeSetApplyExecutorInitTask changeSetApplyExecutorInitTask,
                                                                      ChangeSetApplyFinishTask changeSetApplyFinishTask,
                                                                      ExecutionContext executionContext) {
        super(ddl, parentPrepareData, preparedData, phyDdlTableOperations, tableTopology, targetTableTopology,
            sourceTableTopology, orderedTargetTableLocations, targetPartition, skipBackfill,
            changeSetApplyExecutorInitTask, changeSetApplyFinishTask,
            ComplexTaskMetaManager.ComplexTaskType.SPLIT_HOT_VALUE, executionContext);
        this.parentPrepareData = parentPrepareData;
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();
        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
        PartitionInfo newPartitionInfo = generateNewPartitionInfo();
        if (!schemaChange(curPartitionInfo, newPartitionInfo)) {
            parentPrepareData.setSkipSplit(true);
            return new TransientDdlJob();
        }

        return super.doCreate();
    }

    @Override
    protected PartitionInfo generateNewPartitionInfo() {
        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();

        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);

        SqlNode sqlAlterTableGroupSpecNode = ((SqlAlterTableGroup) ddl.getSqlNode()).getAlters().get(0);
        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
            .getNewPartitionInfo(
                parentPrepareData,
                curPartitionInfo,
                true,
                sqlAlterTableGroupSpecNode,
                preparedData.getOldPartitionNames(),
                preparedData.getNewPartitionNames(),
                parentPrepareData.getTableGroupName(),
                null,
                preparedData.getInvisiblePartitionGroups(),
                orderedTargetTableLocations,
                executionContext);

        return newPartInfo;
    }

    public AlterTableGroupBasePreparedData getParentPrepareData() {
        return parentPrepareData;
    }
}
