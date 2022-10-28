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
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionSpecConfig;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

/**
 * Store locality of table into metadb
 *
 * @author moyi
 * @since 2021/07
 */
@Getter
@TaskName(name = "StoreTableLocalityTask")
public class StoreTableLocalityTask extends BaseGmsTask {

    private String locality;

    /**
     * Whether it's a drop
     */
    private final boolean drop;
    @JSONCreator
    public StoreTableLocalityTask(String schemaName, String logicalTableName, String locality, boolean drop) {
        super(schemaName, logicalTableName);
        this.locality = locality;
        this.drop = drop;
    }


    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        SchemaManager sm = OptimizerContext.getContext(getSchemaName()).getLatestSchemaManager();
        LocalityManager lm = LocalityManager.getInstance();

        TableMeta tableMeta = sm.getTable(getLogicalTableName());


        if(!drop){
            lm.setLocalityOfTable(tableMeta.getId(), locality);
        }else{
            lm.deleteLocalityOfTable(tableMeta.getId());
        }
        try {
            TableInfoManager.updateTableVersion(schemaName, logicalTableName, metaDbConnection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }
}
