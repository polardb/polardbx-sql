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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateJoinGroupJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.JoinGroupValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoAccessor;
import com.alibaba.polardbx.gms.tablegroup.JoinGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateJoinGroup;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import org.apache.calcite.sql.SqlCreateJoinGroup;
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
public class LogicalCreateJoinGroupHandler extends LogicalCommonDdlHandler {

    public static Logger logger = LoggerFactory.getLogger(LogicalCreateJoinGroupHandler.class);

    public LogicalCreateJoinGroupHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalCreateJoinGroup logicalCreateJoinGroup = (LogicalCreateJoinGroup) logicalDdlPlan;
        SqlCreateJoinGroup sqlNode = (SqlCreateJoinGroup) logicalCreateJoinGroup.getNativeSqlNode();
        String schemaName = logicalCreateJoinGroup.getSchemaName();
        if (schemaName == null) {
            schemaName = executionContext.getSchemaName();
        }
        String joinGroupName = logicalCreateJoinGroup.getTableJoinName();

        return CreateJoinGroupJobFactory.create(schemaName, joinGroupName, sqlNode.getLocality(),
            logicalCreateJoinGroup.isIfNotExists(), executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalCreateJoinGroup logicalCreateJoinGroup = (LogicalCreateJoinGroup) logicalDdlPlan;
        SqlCreateJoinGroup sqlNode = (SqlCreateJoinGroup) logicalCreateJoinGroup.getNativeSqlNode();
        String schemaName = logicalCreateJoinGroup.getSchemaName();
        if (schemaName == null) {
            schemaName = executionContext.getSchemaName();
        }

        // validate if schema is auto partition mode
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "can't execute the create joingroup command in non-partitioning database");
        }

        // validate join group characters
        String joinGroupName = logicalCreateJoinGroup.getTableJoinName();
        JoinGroupValidator.validateJoinGroupName(joinGroupName);

        // validate the locality
        String locality = sqlNode.getLocality();
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

        // validate existence
        boolean isIfNotExists = logicalCreateJoinGroup.isIfNotExists();
        JoinGroupInfoAccessor joinGroupInfoAccessor = new JoinGroupInfoAccessor();
        try (Connection connection = MetaDbUtil.getConnection()) {
            joinGroupInfoAccessor.setConnection(connection);
            JoinGroupInfoRecord joinGroupInfoRecord =
                joinGroupInfoAccessor.getJoinGroupInfoByName(schemaName, joinGroupName, true);
            if (joinGroupInfoRecord != null) {
                if (isIfNotExists) {
                    return true;
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_JOIN_GROUP_ALREADY_EXISTS,
                        String.format("Create joingroup error, joingroup[%s] has already exist", joinGroupName));
                }
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }

        return false;
    }
}
