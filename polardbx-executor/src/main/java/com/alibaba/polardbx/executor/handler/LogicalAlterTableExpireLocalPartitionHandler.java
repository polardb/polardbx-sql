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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.executor.ddl.job.factory.localpartition.ExpireLocalPartitionJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.server.DefaultServerConfigManager;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableExpireLocalPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LogicalAlterTableExpireLocalPartitionHandler extends LogicalCommonDdlHandler {

    private static final Logger logger = LoggerFactory.getLogger(LogicalAlterTableExpireLocalPartitionHandler.class);

    private static final String CREATE_TABLE_LIKE_FORMAT = " create table %s like %s engine = 'oss'";

    public LogicalAlterTableExpireLocalPartitionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        SqlAlterTable sqlAlterTable = (SqlAlterTable) logicalDdlPlan.getNativeSqlNode();
        SqlAlterTableExpireLocalPartition expireLocalPartition =
            (SqlAlterTableExpireLocalPartition) sqlAlterTable.getAlters().get(0);
        List<SqlIdentifier> partitions = expireLocalPartition.getPartitions();
        List<String> partitionNameList = CollectionUtils.isEmpty(partitions)?
            new ArrayList<>() : partitions.stream().map(e->e.getLastName()).collect(Collectors.toList());
        final String schemaName = logicalDdlPlan.getSchemaName();
        final String primaryTableName = logicalDdlPlan.getTableName();

        PolarPrivilegeUtils.checkPrivilege(schemaName, primaryTableName, PrivilegePoint.ALTER, executionContext);
        PolarPrivilegeUtils.checkPrivilege(schemaName, primaryTableName, PrivilegePoint.DROP, executionContext);

        ExpireLocalPartitionJobFactory jobFactory =
            new ExpireLocalPartitionJobFactory(schemaName, primaryTableName, partitionNameList, logicalDdlPlan.relDdl, executionContext);
        return jobFactory.create();
    }
}