package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

@Getter
@TaskName(name = "AlterTableGroupDropPartitionAddSubTaskMetaTask")
// here is add meta to partition_partitions_delta table for CDC mark usage only
public class AlterTableGroupDropPartitionAddSubTaskMetaTask extends BaseGmsTask {

    protected String tableName;
    protected TablePartitionRecord logTableRec;
    protected List<TablePartitionRecord> partRecList;
    protected Map<String, List<TablePartitionRecord>> subPartRecInfos;

    @JSONCreator
    public AlterTableGroupDropPartitionAddSubTaskMetaTask(String schemaName, String tableName,
                                                          TablePartitionRecord logTableRec,
                                                          List<TablePartitionRecord> partRecList,
                                                          Map<String, List<TablePartitionRecord>> subPartRecInfos) {
        super(schemaName, tableName);
        this.tableName = tableName;
        this.logTableRec = logTableRec;
        this.partRecList = partRecList;
        this.subPartRecInfos = subPartRecInfos;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);

        if (GeneralUtil.isNotEmpty(subPartRecInfos)) {
            for (Map.Entry<String, List<TablePartitionRecord>> entry : subPartRecInfos.entrySet()) {
                if (GeneralUtil.isEmpty(entry.getValue())) {
                    partRecList.removeIf(o -> o.getPartName().equalsIgnoreCase(entry.getKey()));
                    subPartRecInfos.remove(entry.getKey());
                }
            }
        }
        tablePartitionAccessor.addNewTablePartitionConfigs(logTableRec,
            partRecList,
            subPartRecInfos,
            true, true);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        tablePartitionAccessor.setConnection(metaDbConnection);
        tablePartitionAccessor
            .deleteTablePartitionConfigsForDeltaTable(schemaName, tableName);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected void updateTableVersion(Connection metaDbConnection) {
        //do nothing
    }

}
