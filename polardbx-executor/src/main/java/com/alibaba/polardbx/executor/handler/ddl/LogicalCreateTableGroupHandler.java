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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateTableGroupJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.TableGroupValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTableGroup;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class LogicalCreateTableGroupHandler extends LogicalCommonDdlHandler {

    public LogicalCreateTableGroupHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalCreateTableGroup logicalCreateTableGroup = (LogicalCreateTableGroup) logicalDdlPlan;
        return CreateTableGroupJobFactory
            .create(logicalCreateTableGroup.relDdl, logicalCreateTableGroup.getPreparedData(),
                executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalCreateTableGroup logicalCreateTableGroup = (LogicalCreateTableGroup) logicalDdlPlan;
        CreateTableGroupPreparedData preparedData = logicalCreateTableGroup.getPreparedData();
        String schemaName = preparedData.getSchemaName();
        if (schemaName == null) {
            schemaName = executionContext.getSchemaName();
        }
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "can't execute the create tablegroup command in non-partitioning database");
        }
        String tableGroupName = preparedData.getTableGroupName();
        TableGroupValidator.validateTableGroupName(tableGroupName);

        // Validate the plan on file storage first
        TableValidator.validateTableEngine(logicalCreateTableGroup, executionContext);

        boolean isIfNotExists = logicalCreateTableGroup.isIfNotExists();
        String locality = preparedData.getLocality();

        // validate the locality
        if (TStringUtil.isNotBlank(locality)) {
            LocalityDesc desc = LocalityInfoUtils.parse(locality);

            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
                storageInfoAccessor.setConnection(metaDbConn);

                List<StorageInfoRecord> storageList =
                    storageInfoAccessor.getStorageInfosByInstIdAndKind(InstIdUtil.getInstId(),
                        StorageInfoRecord.INST_KIND_MASTER);
                List<StorageInfoRecord> targetStorage =
                    storageList.stream().filter(x -> desc.matchStorageInstance(x.getInstanceId()))
                        .collect(Collectors.toList());
                if (CollectionUtils.isEmpty(targetStorage)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                        "no available storage found for locality: " + locality);
                }
            } catch (SQLException e) {
                MetaDbLogUtil.META_DB_LOG.error("Failed to access storage_info: " + e);
                throw GeneralUtil.nestedException(e);
            }
        }
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);

        if (tableGroupConfig != null) {
            if (isIfNotExists == false) {
                // throw exception
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("Create tablegroup error, tablegroup[%s] has already exist", tableGroupName));
            }
        }
        return false;
    }
}
