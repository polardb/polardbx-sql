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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.ddl.newengine.DdlConstants.RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME;

public class TruncateUtil {
    final public static String TRUNCATE_SUFFIX = "_trun_";
    // In new part db, we can not create table whose name length is greater than 64 - 6(physical table suffix) = 58
    final public static int MAX_TMP_TABLE_NAME_LENGTH = 58;

    public static String generateTmpTableName(String tableName, String tmpTableSuffix) {
        // if origin table name is too long, we will truncate origin table name and then add suffix
        if (tableName.length() + tmpTableSuffix.length() >= MAX_TMP_TABLE_NAME_LENGTH) {
            tableName = tableName.substring(0, MAX_TMP_TABLE_NAME_LENGTH - tmpTableSuffix.length());
        }

        return tableName + tmpTableSuffix;
    }

    private static String removeNewPartDbIndexNameSuffix(String indexName) {
        // xxxx_$xxxx
        return indexName.substring(0, indexName.length() - 6);
    }

    public static Map<String, String> generateTmpIndexTableMap(List<String> originIndexTableNames,
                                                               List<String> tmpIndexTableNames,
                                                               String tmpTableSuffix,
                                                               boolean isNewPartDb) {
        Map<String, String> tmpIndexTableMap = new HashMap<>();
        if (!isNewPartDb) {
            for (String originIndexTableName : originIndexTableNames) {
                tmpIndexTableMap.put(originIndexTableName, generateTmpTableName(originIndexTableName, tmpTableSuffix));
            }
        } else {
            Map<String, String> m1 = new HashMap<>();
            Map<String, String> m2 = new HashMap<>();

            for (String originIndexTableName : originIndexTableNames) {
                m1.put(removeNewPartDbIndexNameSuffix(originIndexTableName), originIndexTableName);
            }
            for (String tmpIndexTableName : tmpIndexTableNames) {
                m2.put(removeNewPartDbIndexNameSuffix(tmpIndexTableName), tmpIndexTableName);
            }

            for (Map.Entry<String, String> entry : m1.entrySet()) {
                tmpIndexTableMap.put(entry.getValue(), m2.get(entry.getKey()));
            }
        }
        return tmpIndexTableMap;
    }

    public static String generateTmpTableRandomSuffix() {
        return TRUNCATE_SUFFIX + RandomStringUtils.randomAlphanumeric(RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME)
            .toLowerCase();
    }

    public static boolean isTruncateTmpPrimaryTable(String tableName) {
        int index = tableName.lastIndexOf(TRUNCATE_SUFFIX);
        return index != -1 && (index + TRUNCATE_SUFFIX.length() + RANDOM_SUFFIX_LENGTH_OF_PHYSICAL_TABLE_NAME
            == tableName.length());
    }

    public static String getTmpTbNamePattern(String schemaName, String tmpTableName) {
        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        TableRule tableRule = tddlRuleManager.getTableRule(tmpTableName);

        if (tableRule == null) {
            // Failed since we can't find the table rule. And we don't check it from meta db for now.
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "not found table rule for table '" + tmpTableName + "'");
        }

        String newTbNamePattern = tableRule.getTbNamePattern();
        return newTbNamePattern;
    }

    public static Map<String, Set<String>> getTmpTableTopology(String schemaName, String tmpTableName) {
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tmpTableName);
        return partitionInfo.getTopology();
    }
}

