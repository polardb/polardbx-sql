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

package com.alibaba.polardbx.executor.ddl.job.factory.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.builder.DropPartitionTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.DropTableBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropPartitionTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropPartitionTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableHideTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.StoreTableLocalityTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.DropOssFilesTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.UpdateFileRemoveTsTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropPartitionTable;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.utils.ITimestampOracle.BITS_LOGICAL_TIME;

public class AlterTableAsOfTimeStampJobFactory extends DdlJobFactory {
    private String schemaName;
    private String logicalTableName;
    private ExecutionContext executionContext;
    private AlterTablePreparedData alterTablePreparedData;

    public AlterTableAsOfTimeStampJobFactory(String schemaName,
                                           String logicalTableName,
                                           AlterTablePreparedData alterTablePreparedData,
                                           ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
        this.executionContext = executionContext;
        this.alterTablePreparedData = alterTablePreparedData;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "alter table as of timestamp");
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }
}

