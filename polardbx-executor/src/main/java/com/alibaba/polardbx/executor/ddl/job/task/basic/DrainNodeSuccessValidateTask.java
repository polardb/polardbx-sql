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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.Map;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "DrainNodeSuccessValidateTask")
public class DrainNodeSuccessValidateTask extends BaseValidateTask {
    private final static Logger LOG = SQLRecorderLogger.ddlLogger;

    private Map<String, String> targetGroupDnMap;

    @JSONCreator
    public DrainNodeSuccessValidateTask(String schemaName, Map<String, String> targetGroupDnMap) {
        super(schemaName);
        this.targetGroupDnMap = targetGroupDnMap;
        setExceptionAction(DdlExceptionAction.PAUSE);
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        targetGroupDnMap.forEach((group, dnId) -> {
            GroupDetailInfoRecord groupDetailInfo = getGroupDetailInfo(schemaName, group);
            if (groupDetailInfo == null) {
                throw DdlHelper.logAndThrowError(LOG,
                    String.format("Can not find group %s, schema name: %s", group, schemaName));
            }
            if (!StringUtils.equalsIgnoreCase(groupDetailInfo.getStorageInstId(), dnId)) {
                throw DdlHelper.logAndThrowError(LOG,
                    String.format(
                        "Group %s[%s] was not successfully moved to dn[%s], schema name: %s",
                        group, groupDetailInfo.getStorageInstId(), dnId));
            }
        });
    }

    private GroupDetailInfoRecord getGroupDetailInfo(String schema, String groupName) {
        GroupDetailInfoRecord result;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            result =
                groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndGroupName(InstIdUtil.getInstId(), schema,
                    groupName);
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException("Failed to get storage and phy db info", ex);
        }

        return result;
    }

    @Override
    protected String remark() {
        return "|DrainNodeSuccessValidateTask: " + targetGroupDnMap;
    }
}
