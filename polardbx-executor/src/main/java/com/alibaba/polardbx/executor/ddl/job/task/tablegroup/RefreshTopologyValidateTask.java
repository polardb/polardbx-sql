package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.TableGroupValidator;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Getter
@TaskName(name = "RefreshTopologyValidateTask")
public class RefreshTopologyValidateTask extends BaseValidateTask {

    private Map<String, List<Pair<String, String>>> instGroupDbInfo;

    @JSONCreator
    public RefreshTopologyValidateTask(String schemaName, Map<String, List<Pair<String, String>>> instGroupDbInfo) {
        super(schemaName);
        this.instGroupDbInfo = instGroupDbInfo;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        for (Map.Entry<String, List<Pair<String, String>>> entry : instGroupDbInfo.entrySet()) {
            String storageInst = entry.getKey();
            if (GeneralUtil.isNotEmpty(entry.getValue())) {
                boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
                if (isNewPart) {
                    List<GroupDetailInfoRecord> records = DbTopologyManager.getGroupDetails(schemaName, storageInst);
                    if (GeneralUtil.isNotEmpty(records)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PHYSICAL_TOPOLOGY_CHANGING,
                            String.format("the dn[%s] is changing, please retry this command later",
                                storageInst));
                    }
                }
            }
        }
    }

    @Override
    protected String remark() {
        return "| " + getDescription();
    }

    @Override
    public String getDescription() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<Pair<String, String>>> entry : instGroupDbInfo.entrySet()) {
            sb.append("[dn:");
            sb.append(entry.getKey());
            sb.append(",");
            for (Pair<String, String> pair : entry.getValue()) {
                sb.append("(");
                sb.append(pair.getKey());
                sb.append(",");
                sb.append(pair.getValue());
                sb.append("),");
            }
            sb.append("],");
        }
        return sb.toString();
    }
}
