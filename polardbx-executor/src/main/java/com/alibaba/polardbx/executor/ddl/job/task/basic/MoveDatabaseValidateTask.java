package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * @author chenghui.lch
 */

@Getter
@TaskName(name = "MoveDatabaseValidateTask")
public class MoveDatabaseValidateTask extends BaseValidateTask {

    String targetSchema;

    /**
     * key: primaryTableNames
     * val: tableVersion
     */
    Map<String, Long> primaryTableVersions;

    public MoveDatabaseValidateTask(String schemaName, String targetSchema, Map<String, Long> primaryTableVersions ) {
        super(schemaName);
        this.primaryTableVersions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.primaryTableVersions.putAll(primaryTableVersions);
        this.targetSchema = targetSchema;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        final SchemaManager schemaManager = executionContext.getSchemaManager(targetSchema);
        if (GeneralUtil.isNotEmpty(primaryTableVersions)) {
            for (Map.Entry<String, Long> tableVersionInfo : primaryTableVersions.entrySet()) {
                TableMeta meta = schemaManager.getTable(tableVersionInfo.getKey());
                Long newTableVersion = meta.getVersion();

                if (newTableVersion.longValue() != tableVersionInfo.getValue().longValue()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                        String.format(
                            "the metadata of current schema[%s].[%s] is too old, version:[%d<->%d], please retry this command",
                            schemaName, tableVersionInfo.getKey(), tableVersionInfo.getValue(), newTableVersion));
                }
            }
        }

        Set<String> latestPrimaryTableNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        List<String> allTableAndIndexNamesOfSchema = ScaleOutPlanUtil.getLogicalTables(schemaName);
        for (int i = 0; i < allTableAndIndexNamesOfSchema.size(); i++) {
            String tblName = allTableAndIndexNamesOfSchema.get(i);
            TableMeta tableMeta = schemaManager.getTable(tblName);
            if (tableMeta.isGsi()) {
                //all the gsi table version change will be behavior by primary table
                assert
                    tableMeta.getGsiTableMetaBean() != null && tableMeta.getGsiTableMetaBean().gsiMetaBean != null;
                String primaryTbl = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                latestPrimaryTableNames.add(primaryTbl);
            } else {
                latestPrimaryTableNames.add(tblName);
            }
        }

        if (latestPrimaryTableNames.size() != primaryTableVersions.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                String.format("the metadata of schema[%s] is too old, please retry this command",
                    schemaName));
        } else {
            for (String tableName : latestPrimaryTableNames) {
                if (!primaryTableVersions.containsKey(tableName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_TABLEGROUP_META_TOO_OLD,
                        String.format(
                            "the metadata of schema[%s] is too old, maybe miss table[%s], please retry this command",
                            schemaName, tableName));
                }
            }
        }
    }

}