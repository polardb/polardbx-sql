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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.clearspring.analytics.util.Lists;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ReplaceTblWithPhyTblVisitor extends ReplaceTableNameWithSomethingVisitor {

    private String schemaName;
    private boolean withSingleTbl;
    private boolean withPartitionTbl;
    private String uniqGroupName;
    private boolean isPartitionHint;

    private String broadcastGroupName;
    private int shardingNum = -1;
    private long tableGroupId = -1;

    public ReplaceTblWithPhyTblVisitor(String defaultSchemaName, ExecutionContext executionContext,
                                       boolean hasDirectHint) {
        super(defaultSchemaName, executionContext);
        //Preconditions.checkArgument(!DbInfoManager.getInstance().isNewPartitionDb(defaultSchemaName));
        this.schemaName = defaultSchemaName;
        this.withSingleTbl = false;
        this.withPartitionTbl = false;
        if (!hasDirectHint && StringUtils.isNotEmpty(executionContext.getPartitionHint())) {
            isPartitionHint = true;
        }
    }

    @Override
    protected SqlNode buildSth(SqlNode sqlNode) {
        if (ConfigDataMode.isFastMock()) {
            return sqlNode;
        }
        if (!(sqlNode instanceof SqlIdentifier)) {
            return sqlNode;
        }

        List<String> names = ((SqlIdentifier) sqlNode).names;
        assert names.size() != 0;
        final String logicalTableName = names.get(names.size() - 1);
        if (isPartitionHint && names.size() > 1) {
            String schema = names.get(names.size() - 2);
            if (StringUtils.isNotEmpty(schema) && !schemaName.equalsIgnoreCase(schema)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_HINT,
                    "partition hint visit table crossing schema");
            }
        }
        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        if (tddlRuleManager == null) {
            return sqlNode;
        }
        String pHint = ec.getPartitionHint();
        final TableRule tableRule = tddlRuleManager.getTableRule(logicalTableName);
        if (isPartitionHint) {
            Assert.assertTrue(StringUtils.isNotEmpty(pHint));
            if (tableRule == null) {
                TableMeta tb =
                    OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
                Assert.assertTrue(tb != null);
                // auto table
                PartitionInfo pi = tb.getPartitionInfo();
                if (pi.isGsiBroadcastOrBroadcast()) {
                    broadcastGroupName = pi.defaultDbIndex();
                    String physicalTblName =
                        pi.getPhysicalPartitionTopology(null).values().iterator().next().get(0).getPhyTable();
                    return new SqlIdentifier(physicalTblName, SqlParserPos.ZERO);
                }
                // record and check table group id
                if (tableGroupId == -1L) {
                    tableGroupId = pi.getTableGroupId();
                } else if (tableGroupId != pi.getTableGroupId()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_HINT,
                        "different table group in partition hint mode");
                }
                List<String> partitionList = Lists.newArrayList();
                partitionList.add(pHint);
                Map<String, List<PhysicalPartitionInfo>> physicalPartitionInfos =
                    pi.getPhysicalPartitionTopology(partitionList);
                if (physicalPartitionInfos != null && physicalPartitionInfos.size() == 1) {
                    Map.Entry<String, List<PhysicalPartitionInfo>> entry =
                        physicalPartitionInfos.entrySet().iterator().next();
                    String groupName = entry.getKey();
                    if (entry.getValue().size() == 1) {
                        if (uniqGroupName == null) {
                            uniqGroupName = groupName.toLowerCase(Locale.ROOT).trim();
                        } else {
                            // error
                            if (!uniqGroupName.equalsIgnoreCase(groupName)) {
                                // should not happen
                                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                                    "Unsupported to use direct HINT for multi partition");
                            }
                        }
                        String physicalTblName = entry.getValue().get(0).getPhyTable();
                        return new SqlIdentifier(physicalTblName, SqlParserPos.ZERO);
                    }
                } else {
                    // should not happen
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_HINT,
                        "Unsupported to use direct HINT for part table that has multi physical tables in one partition");
                }

                return sqlNode;
            } else {
                // drds table partition hint: groupname_xxxx
                Pair<String, Integer> topologyTarget = decodePartitionHintForShardingTable(pHint);
                uniqGroupName = topologyTarget.getKey();
                Map<String, Set<String>> topology = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
                topology.putAll(tableRule.getActualTopology());
                if (topology.containsKey(topologyTarget.getKey())) {
                    Assert.assertTrue(topology.get(topologyTarget.getKey()) != null);
                    String[] tbls = topology.get(topologyTarget.getKey()).toArray(new String[0]);

                    // record and check shardingNum
                    if (shardingNum == -1) {
                        shardingNum = tbls.length;
                    } else if (shardingNum != tbls.length) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_HINT,
                            "different sharding table num in partition hint mode");
                    }

                    Arrays.sort(tbls, CaseInsensitive.CASE_INSENSITIVE_ORDER);
                    if (topologyTarget.getValue() < tbls.length) {
                        String physicalTblName = tbls[topologyTarget.getValue()];
                        return new SqlIdentifier(physicalTblName, SqlParserPos.ZERO);
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_HINT,
                            "table index overflow in target group from direct HINT for sharding table");
                    }
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_HINT,
                        "cannot find target group from direct HINT for sharding table");
                }
            }
        }

        if (tableRule == null) {
            return sqlNode;
        }
        // don't replace table with sharding db and  tb
        if (tableRule.getActualTopology().size() > 1) {
            for (Map.Entry<String, Set<String>> dbEntry : tableRule.getActualTopology().entrySet()) {
                if (dbEntry.getValue().size() > 1) {
                    return new SqlIdentifier(logicalTableName, SqlParserPos.ZERO);
                }
            }
        }

        this.withSingleTbl |= tddlRuleManager.isTableInSingleDb(logicalTableName);
        this.withPartitionTbl |= tddlRuleManager.isShard(logicalTableName);

        return new SqlIdentifier(tableRule.getTbNamePattern(), SqlParserPos.ZERO);
    }
    /**
     * partition hint decode for sharding table
     * groupname_00xx
     */
    private Pair<String, Integer> decodePartitionHintForShardingTable(String pHint) {
        int split = pHint.lastIndexOf(':');
        String gName = pHint;
        Integer tblNum = 0;
        if (split != -1) {
            gName = pHint.substring(0, split);
            tblNum = Integer.parseInt(pHint.substring(split + 1));
        }

        Pair<String, Integer> gNameTblNum = new Pair<>(gName, tblNum);
        return gNameTblNum;
    }

    @Override
    protected boolean addAliasForDelete(SqlNode delete) {
        if (delete instanceof SqlDelete) {
            return ((SqlDelete) delete).getSubQueryTableMap().keySet().stream().anyMatch(k -> k.isA(SqlKind.QUERY));
        }

        return super.addAliasForDelete(delete);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public boolean shouldChooseSingleGroup() {
        // with single table and without partition table
        if (withSingleTbl && !withPartitionTbl) {
            return true;
        }
        return false;
    }

    public boolean shouldSkipSingleGroup() {
        return withPartitionTbl;
    }

    public String getUniqGroupName() {
        if (StringUtils.isEmpty(uniqGroupName)) {
            return broadcastGroupName;
        }
        return uniqGroupName;
    }
}
