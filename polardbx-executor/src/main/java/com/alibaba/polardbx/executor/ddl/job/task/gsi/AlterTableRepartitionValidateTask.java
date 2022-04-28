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

package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CheckOSSArchiveUtil;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "AlterTableRepartitionValidateTask")
public class AlterTableRepartitionValidateTask extends BaseValidateTask {
    private final String primaryTableName;
    private final String indexName;
    private final Map<String, List<String>> addColumnsIndexes;
    private final List<String> dropIndexes;
    private final TableGroupConfig createTableGroupConfig;

    private final List<TableGroupConfig> tableGroupConfigs = new ArrayList<>();
    private final Map<String, List<Long>> tableGroupIds = new HashMap<>();

    private transient TableMeta tableMeta;

    private final boolean checkSingleTgNotExists;
    private final boolean checkBroadcastTgNotExists;

    @JSONCreator
    public AlterTableRepartitionValidateTask(String schemaName, String primaryTableName, String indexName,
                                             Map<String, List<String>> addColumnsIndexes,
                                             List<String> dropIndexes,
                                             TableGroupConfig createTableGroupConfig,
                                             boolean checkSingleTgNotExists,
                                             boolean checkBroadcastTgNotExists) {
        super(schemaName);
        this.primaryTableName = primaryTableName;
        this.indexName = indexName;
        this.addColumnsIndexes = addColumnsIndexes;
        this.dropIndexes = dropIndexes;
        this.createTableGroupConfig = createTableGroupConfig;
        this.checkSingleTgNotExists = checkSingleTgNotExists;
        this.checkBroadcastTgNotExists = checkBroadcastTgNotExists;
        genTableGroupInfoForValidate();
        if (StringUtils.isEmpty(indexName) || StringUtils.isEmpty(primaryTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "validate",
                "The table name shouldn't be empty");
        }
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        doValidate(executionContext);
    }

    /**
     * check
     * 1. 支持GSI功能
     * 2. GSI name长度限制
     * 3. GSI全局唯一
     * 4. GSI没有跟其他index重名
     * 5. 已经存在的GSI的加列校验
     * 6. drop gsi校验
     */
    public void doValidate(ExecutionContext executionContext) {
        if (!TableValidator.checkIfTableExists(schemaName, primaryTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, schemaName, primaryTableName);
        }
        IndexValidator.validateIndexNonExistence(schemaName, primaryTableName, indexName);
        GsiValidator.validateGsiSupport(schemaName, executionContext);
        GsiValidator.validateCreateOnGsi(schemaName, indexName, executionContext);

        // validate gsi add columns
        for (Map.Entry<String, List<String>> entry : addColumnsIndexes.entrySet()) {
            String gsiName = entry.getKey();
            this.tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(gsiName);
            Set<String> columns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            columns.addAll(tableMeta.getAllColumns().stream().map(c -> c.getName()).collect(Collectors.toList()));

            TableValidator.validateTableExistence(schemaName, gsiName, executionContext);
            for (String columnName : entry.getValue()) {
                checkColumnNotExists(columns, columnName);
                columns.add(columnName);
            }
        }

        // validate current gsi existence
        for (String gsiName : dropIndexes) {
            GsiValidator.validateGsiExistence(schemaName, primaryTableName, gsiName, executionContext);
        }

        // validate table group
        if (tableGroupIds != null) {
            tableGroupIds.forEach(
                (key, value) -> TableValidator.validateTableInTableGroup(schemaName, key, value, executionContext));
        }

        TableValidator.validateTableGroupChange(schemaName, createTableGroupConfig);
        if (tableGroupConfigs != null) {
            tableGroupConfigs.forEach(e -> TableValidator.validateTableGroupChange(schemaName, e));
        }

        if (checkSingleTgNotExists) {
            TableValidator.validateTableGroupNoExists(schemaName, TableGroupNameUtil.SINGLE_DEFAULT_TG_NAME_TEMPLATE);
        }
        if (checkBroadcastTgNotExists) {
            TableValidator.validateTableGroupNoExists(schemaName, TableGroupNameUtil.BROADCAST_TG_NAME_TEMPLATE);
        }
        CheckOSSArchiveUtil.checkWithoutOSSGMS(schemaName, primaryTableName);
        //todo for partition table, maybe we need the corresponding physical table name checker
    }

    private void checkColumnNotExists(Set<String> columns, String columnName) {
        if (columns.stream().anyMatch(columnName::equalsIgnoreCase)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATE_COLUMN, columnName);
        }
    }

    private void genTableGroupInfoForValidate() {
        for (Map.Entry<String, List<String>> entry : addColumnsIndexes.entrySet()) {
            String gsiName = entry.getKey();
            genTableGroupInfoFromTbName(gsiName);
        }

        for (String gsiName : dropIndexes) {
            genTableGroupInfoFromTbName(gsiName);
        }
    }

    private void genTableGroupInfoFromTbName(String tbName) {
        if (tbName != null) {
            tbName = tbName.toLowerCase();
        }

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        PartitionInfo partitionInfo = oc.getPartitionInfoManager().getPartitionInfo(tbName);
        if (partitionInfo != null) {
            Long groupId = partitionInfo.getTableGroupId();
            TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager().getTableGroupConfigById(groupId);
            tableGroupConfigs.add(tableGroupConfig);
            tableGroupIds.put(tbName, ImmutableList.of(groupId));
        }
    }

    @Override
    protected String remark() {
        return "|primaryTableName: " + primaryTableName;
    }
}
