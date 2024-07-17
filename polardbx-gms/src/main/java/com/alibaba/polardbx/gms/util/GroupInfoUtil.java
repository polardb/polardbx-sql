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

import com.alibaba.polardbx.common.partition.MurmurHashUtils;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.util.HexBin;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
     * The max length of logical db_name prefix during building new group_key, default value is 36
     * it cannot be change!!!
     *
     * <pre>
     *     The max len of log_db_name is 64,
     *     The max len of phy_db_name is 64,
     *     The max len of gtrid and bqual is 63,
     *     butt he format of bqual is 'xxxx_group@0000',
     *     so the allowed len of group_key is 63-5=58.
     *
     *     The len of fixed postfix of group_key is 13, e.g "_P00000_group",
     *     so the max len of group_key_prefix is 58-13=45.
     *
     *     The len of fixed postfix of group_index is 7, e.g "_P00000",
     *     and then the actual allowed len of phy_db_name is 45+7=52.
     *
     *     The len of hexStr of hashVal of dbName 9, such e.g. "_2827BA7D",
     *     so the max len of db_name_prefix of group_key_prefix is 45-9=36.
     *     That means when the len of db_name is longer then 36, its group_key_prefix
     *     will be made up of 32-len-db-name-prefix and 9-len-full-name-hash-hex-str.
     *     for example,
     *     if
     *      db_name='abcd012345678901234567890123456789012345678901234567890123456789', len:64
     *     its
     *      db_name_prefix='ABCD01234567890123456789012345678901', len:36;
     *      full_db_name_hash_hex_str='_2827BA7D' , len:9;
     *      group_key_prefix='ABCD01234567890123456789012345678901_2827BA7D', len:45;
     *      group_key = 'ABCD01234567890123456789012345678901_2827BA7D_P00000_GROUP', len:58;
     *      phy_db_name = 'ABCD01234567890123456789012345678901_2827BA7D_P00000', len:52 ;
     *      xid's bqual = 'ABCD01234567890123456789012345678901_2827BA7D_P00000_GROUP@0000', len:63
     *
     * </pre>
     */
    private static final int MAX_DB_NAME_PREFIX_LENGTH = 36;

    /**
     * The prefix of group key build from dbName, format: dbPrefix_36len + "_" + hashCodeHexStr_8len= 45en
     */
    private static final String GROUP_NAME_PREFIX_TEMPLATE = "%s_%s";

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

    public static final String GROUP_NAME_FOR_IMPORTED_DATABASE = "%s_group";

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
        String grpNamePrefix = buildGroupNamePrefixByDbName(dbName);
        if (groupIdx < 0) {
            grpName = java.lang.String.format(GROUP_NAME_SINGLE_TEMPLATE, grpNamePrefix);
        } else {
            grpName = java.lang.String
                .format(forPartitionTable ? GROUP_NAME_TEMPLATE_FOR_PARTITIONED_TABLES : GROUP_NAME_TEMPLATE,
                    grpNamePrefix, groupIdx);
        }
        // all group name use upper case
        grpName = grpName.toUpperCase();
        return grpName;
    }

    public static String buildGroupNamePattern(String dbName, boolean forPartitionTable) {
        String grpNamePrefixStr = buildGroupNamePrefixByDbName(dbName);
        String grpNamePattern = java.lang.String
            .format(forPartitionTable ? GROUP_NAME_TEMPLATE_FOR_PARTITIONED_TABLES : GROUP_NAME_TEMPLATE,
                grpNamePrefixStr, 0);
        if (forPartitionTable) {
            grpNamePattern = grpNamePattern.toUpperCase().replace("P00000", "P{0000}");
        } else {
            grpNamePattern = grpNamePattern.toUpperCase().replace("000000", "{000000}");
        }
        return grpNamePattern;
    }

    public static String buildSchemaNameFromGroupName(String groupName) {
        return DbTopologyManager.getDbNameByGroupKey(groupName);
    }

    public static String buildGroupNameFromPhysicalDb(String physicalDb) {
        /**
         * 修复正常physicalDb的名字后缀带s时，被误strip掉的问题
         **/
        String shareStorageMode =
            MetaDbInstConfigManager.getInstance()
                .getInstProperty(ConnectionProperties.SHARE_STORAGE_MODE,
                    ConnectionParams.SHARE_STORAGE_MODE.getDefault());
        if ("True".equalsIgnoreCase(shareStorageMode)) {
            physicalDb = StringUtils.stripEnd(physicalDb, "sS");
        }
        String groupName = physicalDb + "_group";
        return groupName.toUpperCase();
    }

    public static String buildPhysicalDbNameFromGroupName(String groupName) {
        if (groupName == null) {
            return null;
        }
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
        String atomKey = java.lang.String
            .format("dsKey_%s#%s#%s-%s#%s", groupKey, storageInstId, ipPort.getKey(),
                java.lang.String.valueOf(ipPort.getValue()),
                phyDbName);
        return atomKey.toLowerCase();
    }

    public static String parseStorageId(String atomKey) {
        String[] rets = atomKey.split("#");
        if (rets.length > 1) {
            return rets[1];
        } else {
            return "";
        }
    }

    public static String buildWeightStr(int r, int w) {
        String atomKey = java.lang.String.format("r%sw%s", r, w);
        return atomKey.toLowerCase();
    }

    public static String buildPhyDbName(String dbName, int groupIdx, boolean forPartitionTable) {
        String phyDbName = null;
        String grpNamePrefixStr = buildGroupNamePrefixByDbName(dbName);
        if (groupIdx < 0) {
            phyDbName = java.lang.String.format(PHY_DB_NAME_SINGLE_TEMPLATE, grpNamePrefixStr);
        } else {
            phyDbName = java.lang.String
                .format(forPartitionTable ? PHY_DB_NAME_TEMPLATE_FOR_PARTITIONED_TABLES : PHY_DB_NAME_TEMPLATE,
                    grpNamePrefixStr,
                    groupIdx);
        }
        // all group name use lower case
        phyDbName = phyDbName.toLowerCase();
        return phyDbName;
    }

    // from *_xxxxxx_group to *_Sxxxxx_group
    public static String buildScaleOutGroupName(String sourceGroupName) {
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
        String sourcePhyDb = DbTopologyManager.getPhysicalDbNameByGroupKeyFromMetaDb(schemaName, sourceGroupName);
        String targetPhyDb = sourcePhyDb + "s";
        return targetPhyDb;
    }

    public static String buildHaGroupKey(String storageInstId, String groupName) {
        String haGroupKey = java.lang.String.format(HA_GROUP_KEY_TEMPLATE, groupName.toUpperCase(), storageInstId);
        return haGroupKey;
    }

    private static String buildGroupNamePrefixByDbName(String dbName) {
        if (StringUtils.isEmpty(dbName)) {
            return "";
        }
        if (SystemDbHelper.isDBBuildIn(dbName)) {
            /**
             * all the build-in dbname use full-db as group_name prefix
             */
            return dbName;
        }
        int length = dbName.length();
        String grpPrefixStr = dbName;
        if (length > GroupInfoUtil.MAX_DB_NAME_PREFIX_LENGTH) {
            String dbNamePrefixStr = dbName.substring(0, GroupInfoUtil.MAX_DB_NAME_PREFIX_LENGTH);
            String fullDbNameHashCodeHexStr = doMurmur3Hash32(dbName);
            grpPrefixStr = String.format(GROUP_NAME_PREFIX_TEMPLATE, dbNamePrefixStr, fullDbNameHashCodeHexStr);
        }
        return grpPrefixStr;
    }

    private static byte[] intToBytesByBigEnd(int intVal) {
        byte[] bytes = ByteBuffer.allocate(4).putInt(intVal).array();
        return bytes;
    }

    private static String doMurmur3Hash32(String dbName) {
        String dbNameLowerCase = dbName.toLowerCase();
        byte[] rawData = dbNameLowerCase.getBytes(StandardCharsets.UTF_8);
        int hashIntVal = MurmurHashUtils.murmurHash32WithZeroSeed(rawData);
        byte[] hashValBytes = intToBytesByBigEnd(hashIntVal);
        String hashCodeHexStr = HexBin.encode(hashValBytes);
        return hashCodeHexStr;
    }
}
