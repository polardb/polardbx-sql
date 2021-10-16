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
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionNameUtil {

    static final int MAX_PART_NAME_LENGTH = 16;
    static final String PART_NAME_TEMPLATE = "p%s";
    static final String SUB_PART_NAME_TEMPLATE = "p%ssp%s";
    public static String PART_PHYSICAL_TABLENAME_PATTERN = "%s_%05d";

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

    public static String autoBuildSubPartitionName(Long subPartPosiInTemp, Long partPosi) {
        String subPartName = String.format(SUB_PART_NAME_TEMPLATE, subPartPosiInTemp, partPosi);
        return toLowerCase(subPartName);
    }

    public static String autoBuildPartitionPhyTableName(String logTbName, Long phyTblIdex) {
        String partName = String.format(PART_PHYSICAL_TABLENAME_PATTERN, logTbName, phyTblIdex);
        return partName;
    }

    public static List<String> autoGeneratePartitionNames(TableGroupConfig tableGroupConfig, int newNameCount) {
        long maxIndex = 0;
        List<String> newPartitionNames = new ArrayList<>();
        for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
            int curIndex = 0;
            try {
                curIndex = Integer.parseInt(record.partition_name.substring(1));
            } catch (NumberFormatException e) {
                curIndex = 0;
            }
            if (curIndex > maxIndex) {
                maxIndex = curIndex;
            }
        }
        while (newNameCount > 0) {
            newPartitionNames.add(autoBuildPartitionName(maxIndex + 1));
            newNameCount--;
            maxIndex++;
        }
        return newPartitionNames;
    }

    public static boolean validatePartName(String partName) {

        if (partName.length() > MAX_PART_NAME_LENGTH) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("Failed to create database because the length of partName[%s] is too long", partName));
        }

        for (int i = 0; i < partName.length(); i++) {
            if (!DbNameUtil.isWord(partName.charAt(i))) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("Failed to create database because the partName[%s] contains some invalid characters",
                        partName));
            }
        }

        if (DbNameUtil.isInvalidKeyWorkds(partName.toLowerCase())) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("Failed to create database because the string of partName[%s] is a keyword", partName));
        }

        return true;
    }

}
