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

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.privilege.DbInfo;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.AlterDatabaseLocalityTask;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.AlterDatabaseModifyStorageInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.AlterDatabaseStorageInstValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.BackgroupRebalanceTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalAlterDatabaseSetLocalityFactory extends DdlJobFactory {
    protected final String schemaName;
    protected final String targetLocality;

    public LogicalAlterDatabaseSetLocalityFactory(String schemaName, String targetLocality) {
        this.schemaName = schemaName;
        this.targetLocality = targetLocality;
    }

    @Override
    protected void validate() {
    }

    protected Set<String> getUndeletableDnIds() {
        Set<String> nonDeletableStorage;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            nonDeletableStorage = DbTopologyManager.getNonDeletableStorageInst(metaDbConn);
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                "failed to fetch the non deletable storage list");
        }
        return nonDeletableStorage;
    }

    protected Set<String> getAllStorageInstIds(String instId) {
        Set<String> allStorageInstIds;
        StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            storageInfoAccessor.setConnection(metaDbConn);
            List<StorageInfoRecord> storageInfoRecords = storageInfoAccessor.getStorageInfosByInstId(instId);
            return storageInfoRecords.stream().map(o -> o.storageInstId).collect(Collectors.toSet());
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                "failed to fetch the non deletable storage list");
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        Boolean appendStoragePool = true;
        Boolean appendDn = true;
        List<String> storagePoolNames = new ArrayList<>();
        String instId = InstIdUtil.getInstId();
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        LocalityDesc targetLocalityDesc = LocalityInfoUtils.parse(targetLocality);
        String originalLocality = LocalityManager.getInstance().getLocalityOfDb(schemaName).getLocality();
        LocalityDesc originalLocalityDesc = LocalityInfoUtils.parse(originalLocality);
        if (!originalLocalityDesc.hasStoragePoolDefinition()) {
            Set<String> originalDnSet = originalLocalityDesc.getDnSet();
            Set<String> targetDnSet = targetLocalityDesc.getDnSet();
            Set<String> fullDnSet = getAllStorageInstIds(instId);
            Set<String> dnSet = new HashSet<>();
            Set<String> undeletableDnSet = getUndeletableDnIds();
            if (!fullDnSet.containsAll(targetDnSet)) {
                Set<String> invalidDnSet =
                    fullDnSet.stream().filter(o -> !targetDnSet.contains(o)).collect(Collectors.toSet());
                String errMsg =
                    String.format(
                        "invalid dn list! dn_id: '%s' not illegal",
                        StringUtils.join(invalidDnSet, ","));
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errMsg);
            }

            if (originalDnSet.containsAll(targetDnSet) && targetDnSet.containsAll(originalDnSet)) {
                String errMsg =
                    String.format(
                        "invalid dn list! '%s', the same as before",
                        StringUtils.join(targetDnSet, ","));
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errMsg);

            }
            if (originalDnSet.containsAll(targetDnSet)) {
                appendDn = false;
                dnSet = originalDnSet.stream().filter(o -> !targetDnSet.contains(o)).collect(Collectors.toSet());
                List<String> undeletableDnList =
                    originalDnSet.stream().filter(o -> undeletableDnSet.contains(o)).collect(Collectors.toList());
                if (!targetDnSet.containsAll(undeletableDnList)) {
                    String errMsg =
                        String.format(
                            "invalid dn list! '%s', must contains undeletableDnid '%s'",
                            StringUtils.join(targetDnSet, ","), StringUtils.join(undeletableDnSet, ","));
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errMsg);
                }
            } else if (targetDnSet.containsAll(originalDnSet)) {
                appendDn = true;
                dnSet = targetDnSet.stream().filter(o -> !originalDnSet.contains(o)).collect(Collectors.toSet());
            } else {
                String errMsg =
                    String.format(
                        "invalid dn list! '%s', must be subset of original dn list or superset of original dn list",
                        StringUtils.join(targetDnSet, ","));
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errMsg);
            }
            if (appendDn) {

                String rebalanceSql = "SCHEDULE REBALANCE DATABASE";
                DdlTask alterDatabaseModifyLocalityTask =
                    new AlterDatabaseLocalityTask(schemaName, instId, targetLocality);
                DdlTask backgroundAppendStoragePoolTask = new BackgroupRebalanceTask(schemaName, rebalanceSql);
                executableDdlJob.addSequentialTasks(ImmutableList.of(
                    alterDatabaseModifyLocalityTask,
                    backgroundAppendStoragePoolTask
                ));
            } else {
                String drainNodeListStr = StringUtils.join(dnSet, ",");
                String rebalanceSql = String.format("SCHEDULE REBALANCE DATABASE DRAIN_NODE = '%s'", drainNodeListStr);
                DdlTask alterDatabaseModifyLocalityTask =
                    new AlterDatabaseLocalityTask(schemaName, instId, targetLocality);
                DdlTask backgroundAppendStoragePoolTask = new BackgroupRebalanceTask(schemaName, rebalanceSql);
                executableDdlJob.addSequentialTasks(ImmutableList.of(
                    alterDatabaseModifyLocalityTask,
                    backgroundAppendStoragePoolTask
                ));
            }
        } else {
            if (originalLocalityDesc.getPrimaryStoragePoolName()
                .equalsIgnoreCase(targetLocalityDesc.getPrimaryStoragePoolName())) {
                List<String> originalStoragePoolNames = originalLocalityDesc.getStoragePoolNames();
                List<String> targetStoragePoolNames = targetLocalityDesc.getStoragePoolNames();
                if (originalStoragePoolNames.containsAll(targetStoragePoolNames)) {
                    if (targetStoragePoolNames.containsAll(originalStoragePoolNames)) {
                        String errMsg = String.format(
                            "invalid storage pool name list! '%s', the same with before.",
                            StringUtils.join(targetStoragePoolNames, ","),
                            originalLocalityDesc.getPrimaryStoragePoolName());
                        throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errMsg);

                    }
                    storagePoolNames =
                        originalStoragePoolNames.stream().filter(o -> !targetStoragePoolNames.contains(o))
                            .collect(Collectors.toList());
                    appendStoragePool = false;
                    if (storagePoolNames.contains(originalLocalityDesc.getPrimaryStoragePoolName())) {
                        String errMsg = String.format(
                            "invalid storage pool name list! '%s', must contain primary storage pool name '%s'.",
                            StringUtils.join(targetStoragePoolNames, ","),
                            originalLocalityDesc.getPrimaryStoragePoolName());
                        throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errMsg);

                    }
                } else if (targetStoragePoolNames.containsAll(originalStoragePoolNames)) {
                    storagePoolNames =
                        targetStoragePoolNames.stream().filter(o -> !originalStoragePoolNames.contains(o))
                            .collect(Collectors.toList());
                    appendStoragePool = true;
                } else {
                    String errMsg =
                        String.format(
                            "invalid storage pool name list! '%s', must be consistency with original settings.",
                            StringUtils.join(targetStoragePoolNames, ","));
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errMsg);
                }
            } else {
                String errMsg =
                    String.format("invalid primary storage pool name! '%s', must equal to original settings.",
                        targetLocalityDesc.getPrimaryStoragePoolName());
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, errMsg);
            }

            if (appendStoragePool) {

                DdlTask alterDatabaseStorageInstValidateTask =
                    new AlterDatabaseStorageInstValidateTask(schemaName, instId,
                        "append", storagePoolNames
                    );
                DdlTask alterDatabaseModifyStorageInfoTask =
                    new AlterDatabaseModifyStorageInfoTask(schemaName, instId, targetLocalityDesc.toString(),
                        storagePoolNames);
                String rebalanceSql = "refresh topology";
                DdlTask backgroundAppendStoragePoolTask = new BackgroupRebalanceTask(schemaName, rebalanceSql);
                executableDdlJob.addSequentialTasks(ImmutableList.of(
                    alterDatabaseStorageInstValidateTask,
                    alterDatabaseModifyStorageInfoTask,
                    backgroundAppendStoragePoolTask
                ));
            } else {
                DdlTask alterDatabaseStorageInstValidateTask =
                    new AlterDatabaseStorageInstValidateTask(schemaName, instId,
                        "append", storagePoolNames
                    );
                DdlTask alterDatabaseModifyStorageInfoTask =
                    new AlterDatabaseModifyStorageInfoTask(schemaName, instId, targetLocalityDesc.toString(),
                        storagePoolNames);
                String rebalanceSqlStmt = "rebalance database drain_node = '%s' drain_storage_pool='%s'";
                String storagePoolNamesStr = StringUtils.join(storagePoolNames, ",");
                List<String> drainNodeList = storagePoolNames.stream()
                    .map(o -> StoragePoolManager.getInstance().getStoragePoolInfo(o).getDnLists()).flatMap(
                        o -> o.stream()).collect(
                        Collectors.toList());
                String drainNodesStr = StringUtils.join(drainNodeList, ",");
                String rebalanceSql = String.format(rebalanceSqlStmt, drainNodesStr, storagePoolNamesStr);
                DdlTask backgroundAppendStoragePoolTask = new BackgroupRebalanceTask(schemaName, rebalanceSql);
                executableDdlJob.addSequentialTasks(ImmutableList.of(
                    alterDatabaseStorageInstValidateTask,
                    alterDatabaseModifyStorageInfoTask,
                    backgroundAppendStoragePoolTask
                ));

            }
        }

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        //TODO, we need some locks, may be not in this way.
//        resources.add(schemaName);
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
