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

import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class GroupInfoUtil {
    /**
     * dbName_%06d_group
     */
    private static final String GROUP_NAME_TEMPLATE = "%s_%06d_group";
    private static final String GROUP_NAME_TEMPLATE_FOR_PARTITIONED_TABLES = "%s_p%05d_group";

    /**
     * dbName_%06d
     */
    private static final String PHY_DB_NAME_TEMPLATE = "%s_%06d";

    public static final String PHY_DB_NAME_TEMPLATE_FOR_PARTITIONED_TABLES = "%s_p%05d";

    private static final String GROUP_NAME_SINGLE_TEMPLATE = "%s_single_group";

    private static final String PHY_DB_NAME_SINGLE_TEMPLATE = "%s_single";

    private static final String GROUP_NAME_POSTFIX_TEMPLATE = "xxxxxx_group";

    private static final String GROUP_NAME_POSTFIX_TEMPLATE_FOR_PARTITIONED_TABLES = "pxxxxx_group";

    private static final String SINGLE_GROUP_POSTFIX_TEMPLATE = "_single_group";

    /**
     * xxx_group@xxxx_storage_inst_id
     */
    private static final String HA_GROUP_KEY_TEMPLATE = "%s@%s";

    public static void sortGroupNames(List<String> groupNameList) {
        Collections.sort(groupNameList);
    }

    /**
     * For legacy db-partition
     */
    public static String buildGroupName(String dbName, int groupIdx) {
        return buildGroupName(dbName, groupIdx, false);
    }

    /**
     * For legacy partition-table or db-partition
     */
    public static String buildGroupName(String dbName, int groupIdx, boolean forPartitionTable) {
        String grpName = null;
        if (groupIdx < 0) {
            grpName = String.format(GROUP_NAME_SINGLE_TEMPLATE, dbName);
        } else {
            grpName = String
                .format(forPartitionTable ? GROUP_NAME_TEMPLATE_FOR_PARTITIONED_TABLES : GROUP_NAME_TEMPLATE,
                    dbName, groupIdx);
        }
        // all group name use upper case
        grpName = grpName.toUpperCase();
        return grpName;
    }

    public static String buildGroupNameFromPhysicalDb(String physicalDb) {
        physicalDb = StringUtils.stripEnd(physicalDb, "sS");
        String groupName = physicalDb + "_group";
        return groupName.toUpperCase();
    }

    public static String buildPhysicalDbNameFromGroupName(String groupName) {
        String physicalDbName = groupName.substring(0, groupName.length() - "_group".length());
        return physicalDbName.toLowerCase();
    }

    public static boolean isSingleGroup(String grpName) {
        if (StringUtils.endsWithIgnoreCase(grpName, SINGLE_GROUP_POSTFIX_TEMPLATE)) {
            return true;
        }
        return false;
    }

    public static String buildAtomKey(String groupKey, String storageInstId, String currAddr, String phyDbName) {
        if (groupKey == null) {
            groupKey = "";
        }
        if (storageInstId == null) {
            storageInstId = "";
        }
        Pair<String, Integer> ipPort = null;
        if (currAddr == null) {
            ipPort = new Pair<>("", 0);
        } else {
            ipPort = AddressUtils.getIpPortPairByAddrStr(currAddr);
        }
        if (phyDbName == null) {
            phyDbName = "";
        }
        String atomKey = String
            .format("dsKey_%s#%s#%s-%s#%s", groupKey, storageInstId, ipPort.getKey(), String.valueOf(ipPort.getValue()),
                phyDbName);
        return atomKey.toLowerCase();
    }

    public static String buildWeightStr(int r, int w) {
        String atomKey = String.format("r%sw%s", r, w);
        return atomKey.toLowerCase();
    }

    public static String buildPhyDbName(String dbName, int groupIdx, boolean forPartitionTable) {
        String phyDbName = null;
        if (groupIdx < 0) {
            phyDbName = String.format(PHY_DB_NAME_SINGLE_TEMPLATE, dbName);
        } else {
            phyDbName = String
                .format(forPartitionTable ? PHY_DB_NAME_TEMPLATE_FOR_PARTITIONED_TABLES : PHY_DB_NAME_TEMPLATE, dbName,
                    groupIdx);
        }
        // all group name use lower case
        phyDbName = phyDbName.toLowerCase();
        return phyDbName;
    }

    // from *_xxxxxx_group to *_Sxxxxx_group
    public static String buildScaloutGroupName(String sourceGroupName) {
        assert sourceGroupName.toLowerCase().lastIndexOf("_group") != -1;
        assert !sourceGroupName.toLowerCase().endsWith(SINGLE_GROUP_POSTFIX_TEMPLATE);
        int len = sourceGroupName.length() - GROUP_NAME_POSTFIX_TEMPLATE.length();
        String scaleGroup = sourceGroupName.substring(0, len) + "S" + sourceGroupName.substring(len + 1);
        scaleGroup = scaleGroup.toUpperCase();
        return scaleGroup;
    }

    /**
     * Build physical database name for scaleout
     */
    public static String buildScaleOutPhyDbName(String schemaName, String sourceGroupName) {
        String sourcePhyDb = DbTopologyManager.getPhysicalDbNameByGroupKey(schemaName, sourceGroupName);
        String targetPhyDb = sourcePhyDb + "s";
        return targetPhyDb;
    }

    public static String buildHaGroupKey(String storageInstId, String groupName) {
        String haGroupKey = String.format(HA_GROUP_KEY_TEMPLATE, groupName.toUpperCase(), storageInstId);
        return haGroupKey;
    }

}
