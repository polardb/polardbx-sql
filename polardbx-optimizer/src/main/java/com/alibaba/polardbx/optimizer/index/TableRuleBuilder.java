package com.alibaba.polardbx.optimizer.index;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.TableRuleUtil;
import com.alibaba.polardbx.optimizer.utils.newrule.RuleUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Optional;

public class TableRuleBuilder {

    public static TableRule buildShardingTableRule(String tableName, TableMeta tableToSchema, SqlNode dbPartitionBy,
                                                   SqlNode dbPartitions, SqlNode tbPartitionBy, SqlNode tbPartitions,
                                                   OptimizerContext optimizerContext,
                                                   ExecutionContext executionContext) {
        return TableRuleUtil.buildShardingTableRule(tableName, tableToSchema, dbPartitionBy, dbPartitions,
            tbPartitionBy, tbPartitions, optimizerContext, executionContext);
    }

    public static TableRule buildSingleTableRule(String schemaName,
                                                 String tableName,
                                                 LocalityDesc locality,
                                                 OptimizerContext optimizerContext,
                                                 boolean randomPhyTableNameEnabled) {
        String defaultDb = locality == null ?
            optimizerContext.getRuleManager().getDefaultDbIndex(null) :
            chooseGroupWithLocality(schemaName, locality);
        TableRule tableRule = new TableRule();

        tableRule.setRandomTableNamePatternEnabled(randomPhyTableNameEnabled);
        TableRuleUtil.populateExistingRandomSuffix(tableName, tableRule, optimizerContext, randomPhyTableNameEnabled);

        if (randomPhyTableNameEnabled) {
            tableName = RuleUtils.genTableNameWithRandomSuffix(tableRule, tableName);
        }

        tableRule.setDbNamePattern(defaultDb);
        tableRule.setTbNamePattern(tableName);

        tableRule.init();

        return tableRule;
    }

    public static TableRule buildBroadcastTableRule(String tableName, TableMeta tableMeta) {
        return buildBroadcastTableRule(tableName, tableMeta,
            OptimizerContext.getContext(tableMeta.getSchemaName()),
            true);
     }

    public static TableRule buildBroadcastTableRuleWithoutRandomPhyTableName(String tableName, TableMeta tableMeta) {
        return buildBroadcastTableRule(tableName, tableMeta, OptimizerContext.getContext(tableMeta.getSchemaName()),
            false);
    }

    public static TableRule buildBroadcastTableRule(String tableName, TableMeta tableMeta,
                                                    OptimizerContext optimizerContext,
                                                    boolean randomPhyTableNameEnabled) {
        return TableRuleUtil.buildBroadcastTableRule(tableName, tableMeta, optimizerContext, randomPhyTableNameEnabled);
    }

    private static String chooseGroupWithLocality(String schemaName, LocalityDesc locality) {
        String storageInstId = locality.getDnList().get(0);
        List<GroupDetailInfoRecord> groups = DbTopologyManager.getGroupDetails(schemaName, storageInstId);
        Optional<GroupDetailInfoRecord> targetGroup = locality.chooseGroup(groups);
        if (!targetGroup.isPresent()) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("no storage instance match locality %s", locality));
        }
        return targetGroup.get().groupName;
    }

}
