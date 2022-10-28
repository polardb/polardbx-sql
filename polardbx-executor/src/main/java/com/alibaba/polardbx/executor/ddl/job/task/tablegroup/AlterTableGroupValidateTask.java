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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.CheckOSSArchiveUtil;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.TableGroupValidator;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TablesAccessor;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;

import java.sql.Connection;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

@Getter
@TaskName(name = "AlterTableGroupValidateTask")
public class AlterTableGroupValidateTask extends BaseValidateTask {

    private String tableGroupName;
    private Map<String, Long> tablesVersion;
    private boolean compareTablesList;
    private Set<String> targetPhysicalGroups;
    private boolean allowEmptyGroup;
    private final static Logger LOG = SQLRecorderLogger.ddlEngineLogger;

    @JSONCreator
    public AlterTableGroupValidateTask(String schemaName, String tableGroupName, Map<String, Long> tablesVersion,
                                       boolean compareTablesList, Set<String> targetPhysicalGroups) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
        this.tablesVersion = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.tablesVersion.putAll(tablesVersion);
        this.compareTablesList = compareTablesList;
        this.targetPhysicalGroups = targetPhysicalGroups;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        TableGroupValidator
            .validateTableGroupInfo(schemaName, tableGroupName, false, executionContext.getParamManager());
        if (GeneralUtil.isNotEmpty(tablesVersion)) {
            for (Map.Entry<String, Long> tableVersionInfo : tablesVersion.entrySet()) {
                Long newTableVersion =
                    executionContext.getSchemaManager(schemaName).getTable(tableVersionInfo.getKey()).getVersion();
                if (newTableVersion.longValue() != tableVersionInfo.getValue().longValue()) {
                    try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
                        TablesAccessor tablesAccessor = new TablesAccessor();
                        tablesAccessor.setConnection(conn);
                        TablesRecord tablesRecord = tablesAccessor.query(schemaName, tableVersionInfo.getKey(), false);
                        if (tablesRecord != null) {
                            LOG.warn(String.format("current tablesRecord details in execution phase: %s",
                                tablesRecord.toString()));
                        } else {
                            LOG.warn(String.format("current tablesRecord details: %s.%s %s", schemaName,
                                tableVersionInfo.getKey(), " not exists"));
                        }
                    } catch (Throwable t) {
                        throw new TddlNestableRuntimeException(t);
                    }
                    throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                        String.format(
                            "the metadata of tableGroup[%s].[%s] is too old, version:[%d<->%d], please retry this command",
                            tableGroupName, tableVersionInfo.getKey(), tableVersionInfo.getValue(), newTableVersion));
                }
            }
        }
        if (compareTablesList) {
            TableGroupConfig tableGroupConfig =
                OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                    .getTableGroupConfigByName(tableGroupName);

            Set<String> primaryTableNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

            final SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
            for (TablePartRecordInfoContext tablePartRecordInfoContext : tableGroupConfig.getAllTables()) {
                String primaryTableName = tablePartRecordInfoContext.getTableName();
                TableMeta tableMeta = schemaManager.getTable(primaryTableName);
                if (tableMeta.isGsi()) {
                    //all the gsi table version change will be behavior by primary table
                    assert
                        tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                    primaryTableName = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                }
                primaryTableNames.add(primaryTableName);
            }

            if (primaryTableNames.size() != tablesVersion.size()) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                    String.format("the metadata of tableGroup[%s] is too old, please retry this command",
                        tableGroupName));
            } else {
                for (String tableName : primaryTableNames) {
                    if (!tablesVersion.containsKey(tableName)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                            String.format(
                                "the metadata of tableGroup[%s] is too old, miss table[%s], please retry this command",
                                tableGroupName, tableName));
                    }
                }
            }
        }

        for (String group : GeneralUtil.emptyIfNull(targetPhysicalGroups)) {
            TableGroupValidator.validatePhysicalGroupIsNormal(schemaName, group);
        }
        CheckOSSArchiveUtil.checkTableGroupWithoutOSSGMS(schemaName, tableGroupName);

    }

    @Override
    protected String remark() {
        return "|tableGroupName: " + tableGroupName;
    }
}
