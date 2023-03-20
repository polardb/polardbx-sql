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

package com.alibaba.polardbx.optimizer.index;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.meta.ShardFunctionMeta;

import java.util.List;

/**
 * @author dylan
 */
public class HumanReadableRule {

    public String tableName;

    public boolean broadcast;

    public List<String> dbPartitionKeys;

    // example for dbPartitionPolicy
    // RANGE_HASH(`buyer_id`, `order_id`, 10)
    // HASH
    public String dbPartitionPolicy;

    public int dbCount;

    public List<String> tbPartitionKeys;

    public String tbPartitionPolicy;

    public int tbCount;

    public PartitionInfo partitionInfo;

    public HumanReadableRule(String tableName, boolean broadcast, List<String> dbPartitionKeys,
                             String dbPartitionPolicy, int dbCount, List<String> tbPartitionKeys,
                             String tbPartitionPolicy, int tbCount) {
        this.tableName = tableName;
        this.broadcast = broadcast;
        this.dbPartitionKeys = dbPartitionKeys;
        this.dbPartitionPolicy = dbPartitionPolicy;
        this.dbCount = dbCount;
        this.tbPartitionKeys = tbPartitionKeys;
        this.tbPartitionPolicy = tbPartitionPolicy;
        this.tbCount = tbCount;
    }

    public HumanReadableRule(PartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    public static HumanReadableRule getHumanReadableRule(PartitionInfo partitionInfo) {
        return new HumanReadableRule(partitionInfo);
    }

    public static HumanReadableRule getHumanReadableRule(TableRule table) {

        String dbPartitionPolicy = null;
        String tbPartitionPolicy = null;

        int dbCount = 1;
        int tbCount = 1;

        if (!GeneralUtil.isEmpty(table.getDbRuleStrs())) {

            if (table.getDbShardFunctionMeta() != null) {
                ShardFunctionMeta dbShardFunctionMeta = table.getDbShardFunctionMeta();
                String funcName = dbShardFunctionMeta.buildCreateTablePartitionFunctionStr();
                dbPartitionPolicy = funcName;
            } else {
                String dbRule = table.getDbRuleStrs()[0];

                if (TStringUtil.containsIgnoreCase(dbRule, "yyyymm")) {
                    dbPartitionPolicy = "yyyymm";
                } else if (TStringUtil.containsIgnoreCase(dbRule, "yyyyweek")) {
                    dbPartitionPolicy = "yyyyweek";
                } else if (TStringUtil.containsIgnoreCase(dbRule, "yyyydd")) {
                    dbPartitionPolicy = "yyyydd";
                } else if (TStringUtil.containsIgnoreCase(dbRule, "mmdd")) {
                    dbPartitionPolicy = "mmdd";
                } else if (TStringUtil.containsIgnoreCase(dbRule, "mm")) {
                    dbPartitionPolicy = "mm";
                } else if (TStringUtil.containsIgnoreCase(dbRule, "dd")) {
                    dbPartitionPolicy = "dd";
                } else if (TStringUtil.containsIgnoreCase(dbRule, "week")) {
                    dbPartitionPolicy = "week";
                } else if (TStringUtil.containsIgnoreCase(dbRule, "hashCode")) {
                    dbPartitionPolicy = "hash";
                } else if (TStringUtil.containsIgnoreCase(dbRule, "longValue")) {
                    dbPartitionPolicy = "hash";
                }
            }
        }

        if (!GeneralUtil.isEmpty(table.getTbRulesStrs())) {

            if (table.getTbShardFunctionMeta() != null) {
                ShardFunctionMeta tbShardFunctionMeta = table.getTbShardFunctionMeta();
                String funcName = tbShardFunctionMeta.buildCreateTablePartitionFunctionStr();
                tbPartitionPolicy = funcName;
            } else {
                String tbRule = table.getTbRulesStrs()[0];

                if (TStringUtil.containsIgnoreCase(tbRule, "yyyymm")) {
                    tbPartitionPolicy = "yyyymm";
                } else if (TStringUtil.containsIgnoreCase(tbRule, "yyyyweek")) {
                    tbPartitionPolicy = "yyyyweek";
                } else if (TStringUtil.containsIgnoreCase(tbRule, "yyyydd")) {
                    tbPartitionPolicy = "yyyydd";
                } else if (TStringUtil.containsIgnoreCase(tbRule, "mmdd")) {
                    tbPartitionPolicy = "mmdd";
                } else if (TStringUtil.containsIgnoreCase(tbRule, "mm")) {
                    tbPartitionPolicy = "mm";
                } else if (TStringUtil.containsIgnoreCase(tbRule, "dd")) {
                    tbPartitionPolicy = "dd";
                } else if (TStringUtil.containsIgnoreCase(tbRule, "week")) {
                    tbPartitionPolicy = "week";
                } else if (TStringUtil.containsIgnoreCase(tbRule, "hashCode")) {
                    tbPartitionPolicy = "hash";
                } else if (TStringUtil.containsIgnoreCase(tbRule, "longValue")) {
                    tbPartitionPolicy = "hash";
                }
            }

        }

        dbCount = table.getActualTopology().size();
        tbCount = 1;

        for (String group : table.getActualTopology().keySet()) {
            tbCount = table.getActualTopology().get(group).size();
            break;
        }

        return new HumanReadableRule(table.getVirtualTbName(), table.isBroadcast(), table.getDbPartitionKeys(),
            dbPartitionPolicy, dbCount, table.getTbPartitionKeys(), tbPartitionPolicy, tbCount);
    }

    public boolean isSingle() {
        return !isPartition() && !isBroadcast() && dbPartitionPolicy == null && tbPartitionPolicy == null;
    }

    public boolean isBroadcast() {
        return !isPartition() && broadcast;
    }

    public boolean isSharding() {
        return !isPartition() && !isSingle() && !isBroadcast();
    }

    public boolean isPartition() {
        return partitionInfo != null;
    }

    public String getPartitionPolicyDigest() {
        if (isPartition()) {
            return partitionInfo.getPartitionBy().toString();
        } else if (isBroadcast()) {
            return "broadcast";
        } else if (isSingle()) {
            return "single";
        } else if (isSharding()) {
            String result = "";
            if (dbPartitionPolicy != null) {
                String dbPartitionPolicyRemoveKey = dbPartitionPolicy.toLowerCase();
                for (String key : dbPartitionKeys) {
                    dbPartitionPolicyRemoveKey = dbPartitionPolicy.replace("`" + key.toLowerCase() + "`", "");
                }
                result += dbPartitionPolicyRemoveKey;
            }

            result += "_" + dbCount;

            if (tbPartitionPolicy != null) {
                String tbPartitionPolicyRemoveKey = tbPartitionPolicy.toLowerCase();
                for (String key : tbPartitionKeys) {
                    tbPartitionPolicyRemoveKey = tbPartitionPolicy.replace("`" + key.toLowerCase() + "`", "");
                }
                result += tbPartitionPolicyRemoveKey;
            }

            result += "_" + tbCount;

            return result;
        } else {
            throw new AssertionError("unknown type");
        }
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }
}
