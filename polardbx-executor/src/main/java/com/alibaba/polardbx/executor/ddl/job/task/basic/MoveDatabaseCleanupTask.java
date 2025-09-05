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
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.tablegroup.ComplexTaskOutlineAccessor;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
@Getter
@TaskName(name = "MoveDatabaseCleanupTask")
public class MoveDatabaseCleanupTask extends BaseDdlTask {

    Map<String, String> sourceTargetGroupMap;
    private final static String VIRTUAL_STORAGE_ID = "VIRTUAL_STORAGE_ID";

    @JSONCreator
    public MoveDatabaseCleanupTask(String schemaName,
                                   Map<String, String> sourceTargetGroupMap) {
        super(schemaName);
        this.sourceTargetGroupMap = sourceTargetGroupMap;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        Map<String, List<String>> virtualStorageOutDatedGroup = new HashMap<>();
        List<String> uselessGroupList = new ArrayList<>();
        ComplexTaskOutlineAccessor complexTaskOutlineAccessor = new ComplexTaskOutlineAccessor();
        complexTaskOutlineAccessor.setConnection(metaDbConnection);
        /*complexTaskOutlineAccessor
            .deleteScaleOutSubTaskByJobId(getJobId(), ComplexTaskMetaManager.ComplexTaskType.MOVE_DATABASE.getValue());

        complexTaskOutlineAccessor
            .invalidScaleOutTaskByJobId(getJobId(), ComplexTaskMetaManager.ComplexTaskType.MOVE_DATABASE.getValue());*/
        complexTaskOutlineAccessor.deleteComplexTaskByJobId(schemaName, getJobId());
        virtualStorageOutDatedGroup.put(VIRTUAL_STORAGE_ID, uselessGroupList);
        for (Map.Entry<String, String> entry : sourceTargetGroupMap.entrySet()) {
            virtualStorageOutDatedGroup.get(VIRTUAL_STORAGE_ID).add(entry.getValue());
        }
        ScaleOutUtils
            .cleanUpUselessGroups(virtualStorageOutDatedGroup, schemaName,
                executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }
}
