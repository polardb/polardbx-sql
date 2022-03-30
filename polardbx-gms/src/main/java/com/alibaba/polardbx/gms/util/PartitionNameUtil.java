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

package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author chenghui.lch
 */
public class PartitionNameUtil {

    static final int MAX_PART_NAME_LENGTH = 16;
    static final String PART_NAME_TEMPLATE = "p%s";
    static final String SUB_PART_NAME_TEMPLATE = "p%ssp%s";
    public static String PART_PHYSICAL_TABLENAME_PATTERN = "%s_%05d";
    public static final int MAX_PART_POSTFIX_NUM = 99999;
    public static final Logger LOGGER = LoggerFactory.getLogger("DDL_ENGINE_LOG");

    public static String toLowerCase(String partName) {
        if (partName == null) {
            return partName;
        }
        return partName.trim().toLowerCase();
    }

    public static String autoBuildPartitionName(Long partPosi) {
        String partName = String.format(PART_NAME_TEMPLATE, partPosi);
        return toLowerCase(partName);
    }

    public static boolean isDefaultPartNamePattern(String prefix) {
        if (StringUtils.isNotEmpty(prefix)) {
            prefix = prefix + "%s";
            if (prefix.equalsIgnoreCase(PART_NAME_TEMPLATE)) {
                return true;
            }
        }
        return false;
    }

    public static String autoBuildPartitionNameWithUserDefPrefix(String prefix, int partPosi) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix);
        sb.append(partPosi);
        return toLowerCase(sb.toString());
    }

    public static String autoBuildSubPartitionName(Long subPartPosiInTemp, Long partPosi) {
        String subPartName = String.format(SUB_PART_NAME_TEMPLATE, subPartPosiInTemp, partPosi);
        return toLowerCase(subPartName);
    }

    public static String autoBuildPartitionPhyTableName(String logTbName, Long phyTblIdex) {
        String partName = String.format(PART_PHYSICAL_TABLENAME_PATTERN, logTbName, phyTblIdex);
        return partName;
    }

    public static List<String> autoGeneratePartitionNames(TableGroupConfig tableGroupConfig, int newNameCount) {
        List<String> newPartitionNames = new ArrayList<>();

        TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
        int tableGroupType =
            tableGroupRecord != null ? tableGroupRecord.tg_type :
                TableGroupRecord.TG_TYPE_PARTITION_TBL_TG;
        boolean isBroadCastTg = (tableGroupType == TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG);
        Set<Integer> existingPostfix = new HashSet<>();
        for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
            int curIndex = 0;
            try {
                curIndex = Integer.parseInt(record.partition_name.substring(1));
            } catch (NumberFormatException e) {
                curIndex = 0;
            }
            existingPostfix.add(curIndex);
        }
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            try {
                conn.setAutoCommit(false);
                TableGroupAccessor accessor = new TableGroupAccessor();
                accessor.setConnection(conn);
                List<TableGroupRecord> tableGroupRecords = accessor
                    .getTableGroupsBySchemaAndName(tableGroupRecord.getSchema(), tableGroupRecord.getTg_name(), true);
                assert tableGroupRecords.size() == 1;
                tableGroupRecord = tableGroupRecords.get(0);

                int minPostfix = tableGroupRecord.getInited();
                if (isBroadCastTg) {
                    minPostfix = 0;
                }
                while (newNameCount > 0) {
                    int nextPostfix = minPostfix + 1;
                    minPostfix++;
                    if (minPostfix >= MAX_PART_POSTFIX_NUM) {
                        //recycle
                        minPostfix = 0;
                    }
                    if (existingPostfix.contains(nextPostfix)) {
                        continue;
                    }
                    newPartitionNames.add(autoBuildPartitionName(Long.valueOf(nextPostfix)));
                    newNameCount--;
                }

                accessor.updateInitedById(tableGroupRecord.getId(), minPostfix);
                conn.commit();
            } finally {
                MetaDbUtil.endTransaction(conn, LOGGER);
            }
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }

        return newPartitionNames;
    }

    //todo(luoyanxin) concurrent job prepare the same partitionName issue
    public static List<String> autoGeneratePartitionNamesWithUserDefPrefix(String prefix, int newNameCount) {
        List<String> newPartitionNames = new ArrayList<>();
        assert newNameCount > 0;
        int i = 1;
        while (newNameCount > 0) {
            newPartitionNames.add(autoBuildPartitionNameWithUserDefPrefix(prefix, i++));
            newNameCount--;
        }
        return newPartitionNames;
    }

    public static boolean validatePartName(String partName, boolean isKeyWords) {

        if (partName.length() > MAX_PART_NAME_LENGTH) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String
                    .format("Failed to execute this command because the length of partName[%s] is too long", partName));
        }

        for (int i = 0; i < partName.length(); i++) {
            if (!DbNameUtil.isWord(partName.charAt(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format(
                        "Failed to execute this command because the partName[%s] contains some invalid characters",
                        partName));
            }
        }

        if (isKeyWords) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("Failed to execute this command because the string of partName[%s] is a keyword",
                    partName));
        }

        return true;
    }

}
