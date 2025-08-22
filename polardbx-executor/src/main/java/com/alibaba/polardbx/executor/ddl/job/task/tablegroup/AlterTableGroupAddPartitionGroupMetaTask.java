package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.tablegroup.*;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.*;

@Getter
@TaskName(name = "AlterTableGroupAddPartitionGroupMetaTask")

public class AlterTableGroupAddPartitionGroupMetaTask extends BaseDdlTask {

    protected Long tableGroupId; //source tablegroup id
    protected List<String> targetDbList;
    protected List<String> newPartitions;
    protected List<String> localities;

    @JSONCreator
    public AlterTableGroupAddPartitionGroupMetaTask(String schemaName, Long tableGroupId,
                                                    List<String> targetDbList, List<String> newPartitions, List<String> localities) {
        super(schemaName);
        this.tableGroupId = tableGroupId;
        this.targetDbList = targetDbList;
        this.newPartitions = newPartitions;
        this.localities = localities;
        assert newPartitions.size() == targetDbList.size();
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        addNewPartitionGroup(metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }


    public void addNewPartitionGroup(Connection metaDbConnection) {
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        partitionGroupAccessor.setConnection(metaDbConnection);
        for (int i = 0; i < newPartitions.size(); i++) {
            PartitionGroupRecord partitionGroupRecord = new PartitionGroupRecord();
            partitionGroupRecord.visible = 1;
            partitionGroupRecord.meta_version = 1L;
            partitionGroupRecord.partition_name = newPartitions.get(i);
            partitionGroupRecord.tg_id = tableGroupId;

            partitionGroupRecord.phy_db = targetDbList.get(i);
            partitionGroupRecord.locality = localities.get(i);

            partitionGroupRecord.pax_group_id = 0L;
            partitionGroupAccessor.addNewPartitionGroup(partitionGroupRecord, false);
        }
    }

}
