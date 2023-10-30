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

package com.alibaba.polardbx.qatest.ddl.sharding.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.cdc.CdcManager;
import com.alibaba.polardbx.cdc.SysTableUtil;
import com.alibaba.polardbx.cdc.entity.DDLExtInfo;
import com.alibaba.polardbx.cdc.entity.LogicMeta;
import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.cdc.SysTableUtil.CDC_DDL_RECORD_TABLE;

/**
 * Created by ziyang.lb
 **/
public abstract class CdcBaseTest extends AsyncDDLBaseNewDBTestCase {

    //Sharding表，建表时不指定gsi
    protected final static String CREATE_T_DDL_TEST_TABLE =
        "CREATE TABLE IF NOT EXISTS `%s` (\n"
            + "  `ID` BIGINT(20) NOT NULL auto_increment,\n"
            + "  `JOB_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `EXT_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `TV_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `SCHEMA_NAME` VARCHAR(200) NOT NULL,\n"
            + "  `TABLE_NAME`  VARCHAR(200) NOT NULL,\n"
            + "  `GMT_CREATED` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `DDL_SQL`  TEXT NOT NULL,\n"
            + "   PRIMARY KEY (`ID`),\n"
            + "   KEY `idx1` (`SCHEMA_NAME`)\n"
            + "   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 dbpartition by hash(ID) tbpartition by hash(JOB_ID) tbpartitions 16\n";

    //Sharding表，建表时指定gsi
    protected final static String CREATE_T_DDL_TEST_TABLE_GSI =
        "CREATE TABLE IF NOT EXISTS `%s` (\n"
            + "  `ID` BIGINT(20) NOT NULL auto_increment,\n"
            + "  `JOB_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `EXT_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `TV_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `SCHEMA_NAME` VARCHAR(200) NOT NULL,\n"
            + "  `TABLE_NAME`  VARCHAR(200) NOT NULL,\n"
            + "  `GMT_CREATED` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `DDL_SQL`  TEXT NOT NULL,\n"
            + "   PRIMARY KEY (`ID`),\n"
            + "   KEY `idx1` (`SCHEMA_NAME`),\n"
            + "   GLOBAL INDEX `%s`(`TV_ID`) COVERING(`JOB_ID`,`GMT_CREATED`) DBPARTITION BY HASH(`TV_ID`),\n"
            + "   GLOBAL INDEX `%s`(`EXT_ID`) DBPARTITION BY HASH(`EXT_ID`)"
            + "   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 dbpartition by hash(ID) tbpartition by hash(JOB_ID) tbpartitions 16\n";

    //Sharding表，建表时指定gsi
    protected final static String CREATE_T_DDL_TEST_TABLE_CLUSTER_GSI =
        "CREATE TABLE IF NOT EXISTS `%s` (\n"
            + "  `ID` BIGINT(20) NOT NULL auto_increment,\n"
            + "  `JOB_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `EXT_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `TV_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `SCHEMA_NAME` VARCHAR(200) NOT NULL,\n"
            + "  `TABLE_NAME`  VARCHAR(200) NOT NULL,\n"
            + "  `GMT_CREATED` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `DDL_SQL`  TEXT NOT NULL,\n"
            + "  `c1` bigint,\n"
            + "  `c2` bigint,\n"
            + "   PRIMARY KEY (`ID`),\n"
            + "   KEY `idx1` (`SCHEMA_NAME`),\n"
            + "   clustered index gsi_idx1 (c1) dbpartition by hash(c1),\n"
            + "   clustered index gsi_idx2(c2) dbpartition by hash(c2),\n"
            + "   GLOBAL INDEX `%s`(`TV_ID`) COVERING(`JOB_ID`,`GMT_CREATED`) DBPARTITION BY HASH(`TV_ID`),\n"
            + "   GLOBAL INDEX `%s`(`EXT_ID`) DBPARTITION BY HASH(`EXT_ID`)"
            + "   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 dbpartition by hash(ID) tbpartition by hash(JOB_ID) tbpartitions 16\n";

    //广播表
    //这个语句不指定charset，验证一下下游是否对字符编码有妥善处理
    protected final static String CREATE_T_DDL_TEST_TABLE_BROADCAST =
        "CREATE TABLE IF NOT EXISTS `%s` (\n"
            + "  `ID` BIGINT(20) NOT NULL auto_increment,\n"
            + "  `JOB_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `EXT_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `TV_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `SCHEMA_NAME` VARCHAR(200) NOT NULL,\n"
            + "  `TABLE_NAME`  VARCHAR(200) NOT NULL,\n"
            + "  `GMT_CREATED` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `DDL_SQL`  TEXT NOT NULL,\n"
            + "   PRIMARY KEY (`ID`),\n"
            + "   KEY `idx1` (`SCHEMA_NAME`)\n"
            + "   ) BROADCAST\n";

    //不带主键的表
    //这个语句的charset指定为utf8,构造和引擎默认使用的utf8mb4不一样的场景，验证一下下游是否对字符编码有妥善处理
    protected final static String CREATE_T_DDL_TEST_TABLE_WITHOUT_PRIMARY =
        "CREATE TABLE IF NOT EXISTS `%s` (\n"
            + "  `ID` BIGINT(20) NOT NULL auto_increment,\n"
            + "  `JOB_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `EXT_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `TV_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `SCHEMA_NAME` VARCHAR(200) NOT NULL,\n"
            + "  `TABLE_NAME`  VARCHAR(200) NOT NULL,\n"
            + "  `GMT_CREATED` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `DDL_SQL`  TEXT NOT NULL,\n"
            + "   PRIMARY KEY (`ID`),\n"
            + "   KEY `idx1` (`SCHEMA_NAME`)\n"
            + "   ) ENGINE=InnoDB DEFAULT CHARSET=utf8 dbpartition by hash(ID) tbpartition by hash(JOB_ID) tbpartitions 16\n";

    //single表
    protected final static String CREATE_T_DDL_TEST_TABLE_SINGLE =
        "CREATE TABLE IF NOT EXISTS `%s` (\n"
            + "  `ID` BIGINT(20) NOT NULL auto_increment,\n"
            + "  `JOB_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `EXT_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `TV_ID` BIGINT(20) NOT NULL DEFAULT 0,\n"
            + "  `SCHEMA_NAME` VARCHAR(200) NOT NULL,\n"
            + "  `TABLE_NAME`  VARCHAR(200) NOT NULL,\n"
            + "  `GMT_CREATED` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `DDL_SQL`  TEXT NOT NULL,\n"
            + "   PRIMARY KEY (`ID`),\n"
            + "   KEY `idx1` (`SCHEMA_NAME`)\n"
            + "   ) ENGINE=InnoDB DEFAULT CHARSET=utf8\n";

    protected int getDdlCount() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt
                .executeQuery("select count(id) from __cdc__." + SysTableUtil.CDC_DDL_RECORD_TABLE)) {
                while (resultSet.next()) {
                    return resultSet.getInt(1);
                }
            }

        }

        return 0;
    }

    protected void doDml(AtomicLong jobIdSeed, String tableName, int dmlCount) throws SQLException {
        try (PreparedStatement ps = tddlConnection.prepareStatement(
            "INSERT INTO `" + tableName + "`(JOB_ID,EXT_ID,TV_ID,SCHEMA_NAME,TABLE_NAME,GMT_CREATED,DDL_SQL)"
                + "VALUES(?,?,?,?,?,NOW(),?)")) {
            for (int i = 0; i < dmlCount; i++) {
                ps.setLong(1, jobIdSeed.incrementAndGet());
                ps.setLong(2, i);
                ps.setLong(3, i);
                ps.setString(4, "ddl_test");
                ps.setString(5, "ddl_test");
                ps.setString(6, "'alter table t_ddl_test add column add1 varchar(20) not null default '111''");
                ps.execute();
            }
        }
    }

    /**
     * 获取CDC打标表中tokenHints所对应的记录
     */
    protected String getDdlRecordSql(String tokenHints) throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(
                "select ddl_sql from __cdc__." + SysTableUtil.CDC_DDL_RECORD_TABLE
                    + " where ddl_sql like '%" + tokenHints + "%' ")) {
                while (resultSet.next()) {
                    return resultSet.getString(1);
                }
            }
        }

        return "";
    }

    protected int getDdlRecordSqlCount(String tokenHints) throws SQLException {
        int count = 0;
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(
                "select ddl_sql from __cdc__." + SysTableUtil.CDC_DDL_RECORD_TABLE
                    + " where ddl_sql like '%" + tokenHints + "%' ")) {
                while (resultSet.next()) {
                    count++;
                }
            }
        }

        return count;
    }

    protected DDLExtInfo getDdlExtInfo(String tokenHints) throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(
                "select EXT from __cdc__." + SysTableUtil.CDC_DDL_RECORD_TABLE
                    + " where ddl_sql like '%" + tokenHints + "%' ")) {
                while (resultSet.next()) {
                    String ext = resultSet.getString(1);
                    return JSONObject.parseObject(ext, DDLExtInfo.class);
                }
            }
        }
        return null;
    }

    protected Map<String, Set<String>> getDdlRecordTopology(String tokenHints, String tableName)
        throws SQLException {
        Map<String, Set<String>> result = Maps.newHashMap();
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(
                "select ddl_sql,meta_info from __cdc__." + SysTableUtil.CDC_DDL_RECORD_TABLE
                    + " where ddl_sql like '%" + tokenHints + "%' and table_name = '" + tableName + "'")) {
                while (resultSet.next()) {
                    String metaInfoStr = resultSet.getString("meta_info");
                    CdcManager.MetaInfo metaInfo = JSONObject.parseObject(metaInfoStr, CdcManager.MetaInfo.class);
                    LogicMeta.LogicTableMeta logicTableMeta = metaInfo.getLogicTableMeta();
                    logicTableMeta.getPhySchemas().forEach(phySchema -> {
                        Assert.assertTrue(StringUtils.isNotBlank(phySchema.getSchema()));
                    });
                    result = logicTableMeta.getPhySchemas().stream()
                        .collect(Collectors.toMap(LogicMeta.PhySchema::getGroup,
                            v -> Sets.newHashSet(
                                v.getPhyTables().stream().map(String::toLowerCase).collect(Collectors.toList()))));
                }
            }
        }

        logger.info("ddl record topology is " + JSONObject.toJSONString(result));
        return result;
    }

    protected Map<String, Set<String>> queryTopology(String tableName) throws SQLException {
        Map<String, Set<String>> result = new HashMap<>();
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery("show topology from " + tableName)) {
                while (resultSet.next()) {
                    String groupName = resultSet.getString("GROUP_NAME");
                    String phyTableName = resultSet.getString("TABLE_NAME");

                    Set<String> value = result.computeIfAbsent(groupName, k -> new HashSet<>());
                    value.add(phyTableName.toLowerCase());
                }
            }
        }

        logger.info("table`s topology is " + JSONObject.toJSONString(result));
        return result;
    }

    protected List<String> getPartitionList(String tableName) throws SQLException {
        List<String> result = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery("show topology from " + tableName)) {
                while (resultSet.next()) {
                    String name = resultSet.getString("PARTITION_NAME");
                    result.add(name);
                }
            }
        }
        return result;
    }

    protected String getSecondLevelPartitionByGroupName(String groupNameIn, String tableName) throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery("show topology from " + tableName)) {
                while (resultSet.next()) {
                    String groupName = resultSet.getString("GROUP_NAME");
                    String partitionName = resultSet.getString("SUBPARTITION_NAME");
                    if (StringUtils.equals(groupName, groupNameIn)) {
                        return partitionName;
                    }
                }
            }
        }
        return "";
    }

    protected String getFirstLevelPartitionByGroupName(String groupNameIn, String tableName) throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery("show topology from " + tableName)) {
                while (resultSet.next()) {
                    String groupName = resultSet.getString("GROUP_NAME");
                    String partitionName = resultSet.getString("PARTITION_NAME");
                    if (StringUtils.equals(groupName, groupNameIn)) {
                        return partitionName;
                    }
                }
            }
        }
        return "";
    }

    protected Map<String, String> getMasterGroupStorageMap() throws SQLException {
        Map<String, String> result = new HashMap<>();
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("show datasources")) {
                while (rs.next()) {
                    String storageInstId = rs.getString("storage_inst_id");
                    int writeWeight = rs.getInt("WRITE_WEIGHT");
                    if (writeWeight <= 0) {
                        continue;
                    }
                    String group = rs.getString("group");
                    if (!group.equals("MetaDB")) {
                        result.put(group, storageInstId);
                    }
                }
            }
        }
        return result;
    }

    protected String buildTokenHints() {
        return String.format("/* CDC_TOKEN : %s */", UUID.randomUUID());
    }

    protected Timestamp getLastDdlRecordTimestamp(String schemaName) throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(
                "select max(GMT_CREATED) from __cdc__." + SysTableUtil.CDC_DDL_RECORD_TABLE + " where SCHEMA_NAME ='"
                    + schemaName + "'")) {
                while (resultSet.next()) {
                    return resultSet.getTimestamp(1);
                }
            }
        }

        return new Timestamp(0);
    }

    protected List<Map<String, String>> getDdlRecordListNewerThan(String schemaName, Timestamp ts) throws SQLException {
        ArrayList<Map<String, String>> list = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(
                "select ddl_sql,ext from __cdc__." + SysTableUtil.CDC_DDL_RECORD_TABLE + " where GMT_CREATED > '" + ts
                    + "' and SCHEMA_NAME ='" + schemaName + "'")) {
                while (resultSet.next()) {
                    Map<String, String> map = new HashMap<>();
                    map.put("ddl_sql", resultSet.getString(1));
                    map.put("ext", resultSet.getString(2));
                    list.add(map);
                }
            }
        }

        return list;
    }

    protected String getDdlRecordSqlByTableName(String tableName) throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(
                "select ddl_sql from __cdc__." + CDC_DDL_RECORD_TABLE
                    + " where table_name = '" + tableName + "' ")) {
                while (resultSet.next()) {
                    return resultSet.getString(1);
                }
            }
        }

        return "";
    }
}
