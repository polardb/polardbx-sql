package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.List;

/**
 * Update group info
 *
 * @author moyi
 * @since 2021/10
 */
@Getter
@TaskName(name = "UpdateGroupInfoTask")
public class UpdateGroupInfoTask extends BaseGmsTask {

    private List<String> groupNameList;
    private int beforeType;
    private int afterType;

    @JSONCreator
    public UpdateGroupInfoTask(String schemaName,
                               List<String> groupNameList,
                               int beforeType,
                               int afterType) {
        super(schemaName, "");
        this.groupNameList = groupNameList;
        this.beforeType = beforeType;
        this.afterType = afterType;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConn, ExecutionContext ec) {
        ScaleOutUtils.updateGroupType(schemaName, groupNameList, afterType, metaDbConn);
        /**
         * Update the opVersion of the topology of the schema
         * so all the cn nodes notice the groupNameList update to removing
         * from DbGroupInfoManager
         */
        String topologyDataId = MetaDbDataIdBuilder.getDbTopologyDataId(schemaName);
        MetaDbConfigManager.getInstance().notify(topologyDataId, metaDbConn);

        FailPoint.injectRandomExceptionFromHint(ec);
        FailPoint.injectRandomSuspendFromHint(ec);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConn, ExecutionContext ec) {
        ScaleOutUtils.updateGroupType(schemaName, groupNameList, beforeType, metaDbConn);

        FailPoint.injectRandomExceptionFromHint(ec);
        FailPoint.injectRandomSuspendFromHint(ec);
    }

    @Override
    public String getDescription() {
        return String.format("Update type of group (%s) to %d", StringUtils.join(groupNameList, ","), afterType);
    }

}
