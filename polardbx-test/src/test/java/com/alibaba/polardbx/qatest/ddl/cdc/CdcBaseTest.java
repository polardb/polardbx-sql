package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.cdc.CdcManager;
import com.alibaba.polardbx.cdc.CdcTableUtil;
import com.alibaba.polardbx.cdc.SQLHelper;
import com.alibaba.polardbx.common.cdc.entity.DDLExtInfo;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlCheckContext;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.PartitionType;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.cdc.CdcTableUtil.CDC_DDL_RECORD_TABLE;
import static com.alibaba.polardbx.qatest.ddl.cdc.util.CdcTestUtil.getServerId4Check;
import static com.alibaba.polardbx.qatest.ddl.cdc.util.CdcTestUtil.removeDdlIdComments;
import static com.alibaba.polardbx.qatest.ddl.cdc.util.CdcTestUtil.removeImplicitTgSyntax;

/**
 * Created by ziyang.lb
 **/
public class CdcBaseTest extends AsyncDDLBaseNewDBTestCase {
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

    protected long lastQueryDdlRecordTimestamp;
    protected String serverId;
    protected ImplicitTableGroupChecker implicitTableGroupChecker = new ImplicitTableGroupChecker();

    @SneakyThrows
    @Before
    public void before() {
        lastQueryDdlRecordTimestamp = System.currentTimeMillis();
        Thread.sleep(1000);
    }

    protected DdlCheckContext newDdlCheckContext() {
        return new DdlCheckContext(k -> {
            if (StringUtils.contains(k, ".")) {
                String[] array = StringUtils.split(k, ".");
                return getDdlRecordInfoList(array[0], array[1]);
            } else {
                return getDdlRecordInfoList(k, null);
            }
        });
    }

    protected void executeSql(Statement stmt, String sql) throws SQLException {
        logger.info("execute sql : " + sql);
        stmt.execute(sql);
    }

    protected void commonCheckExistsAfterDdlWithCallback(DdlCheckContext checkContext, String schemaName,
                                                         String tableName, String sql,
                                                         Consumer<Pair<List<DdlRecordInfo>, List<DdlRecordInfo>>> consumer) {
        List<DdlRecordInfo> beforeMarkList = checkContext.getMarkList(schemaName);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName, sql);
        List<DdlRecordInfo> afterMarkList = checkContext.updateAndGetMarkList(schemaName);
        consumer.accept(Pair.of(beforeMarkList, afterMarkList));
    }

    protected void commonCheckExistsAfterDdl(DdlCheckContext checkContext, String schemaName, String tableName,
                                             String sql) {
        List<DdlRecordInfo> beforeMarkList = checkContext.getMarkList(schemaName);
        List<DdlRecordInfo> beforeMarkListTable = beforeMarkList.stream()
            .filter(i -> StringUtils.equals(tableName, i.getTableName()))
            .collect(Collectors.toList());

        List<DdlRecordInfo> afterMarkList = checkContext.updateAndGetMarkList(schemaName);
        List<DdlRecordInfo> afterMarkListTable = afterMarkList.stream()
            .filter(i -> StringUtils.equals(tableName, i.getTableName()))
            .collect(Collectors.toList());

        Assert.assertEquals(beforeMarkList.size() + 1, afterMarkList.size());
        Assert.assertEquals(beforeMarkListTable.size() + 1, afterMarkListTable.size());
        Assert.assertEquals(tableName, afterMarkList.get(0).getTableName());
        Assert.assertEquals(getServerId4Check(serverId), afterMarkList.get(0).getDdlExtInfo().getServerId());

        assertSqlEquals(sql, removeImplicitTgSyntax(afterMarkList.get(0).getEffectiveSql()));
    }

    protected void commonCheckNotExistsAfterDdl(DdlCheckContext checkContext, String schemaName, String tableName) {
        List<DdlRecordInfo> beforeMarkList1 = checkContext.getMarkList(schemaName + "." + tableName);
        List<DdlRecordInfo> beforeMarkList2 = checkContext.getMarkList(schemaName);
        List<DdlRecordInfo> afterMarkList1 = checkContext.updateAndGetMarkList(schemaName + "." + tableName);
        List<DdlRecordInfo> afterMarkList2 = checkContext.updateAndGetMarkList(schemaName);

        Assert.assertEquals(beforeMarkList1.size(), afterMarkList1.size());
        Assert.assertEquals(beforeMarkList2.size(), afterMarkList2.size());

    }

    @SneakyThrows
    protected String queryTableGroup(String schemaName, String tableName) {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("show full tablegroup")) {
                while (rs.next()) {
                    String tableSchema = rs.getString("TABLE_SCHEMA");
                    String tables = rs.getString("TABLES");
                    String tableGroup = rs.getString("TABLE_GROUP_NAME");
                    String[] tableNames = org.apache.commons.lang3.StringUtils.split(tables, ",");
                    List<String> tableList = Lists.newArrayList(tableNames);

                    if (schemaName.equals(tableSchema) && tableList.contains(tableName)) {
                        return tableGroup;
                    }
                }
            }
        }

        throw new RuntimeException("can`t find table group for table " + tableName);
    }

    protected String getMovePartitionSql4Table(String tableName)
        throws SQLException {
        Map<String, String> map = getMasterGroupStorageMap();
        String fromStorage = null;
        String toStorage = null;
        String partition = null;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String temp = getFirstLevelPartitionByGroupName(entry.getKey(), tableName);
            if (org.apache.commons.lang3.StringUtils.isNotBlank(temp)) {
                fromStorage = entry.getValue();
                partition = temp;
                break;
            }
        }
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!org.apache.commons.lang3.StringUtils.equals(fromStorage, entry.getValue())) {
                toStorage = entry.getValue();
            }
        }

        return String
            .format("ALTER TABLE %s move PARTITIONS %s to '%s'", tableName, partition, toStorage);
    }

    protected String getMovePartitionSql(String tableGroup, String tableName, PartitionType partitionType)
        throws SQLException {
        Map<String, String> map = getMasterGroupStorageMap();
        String fromGroup = null;
        String fromStorage = null;
        String toStorage = null;
        String partition = null;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String temp = getFirstLevelPartitionByGroupName(entry.getKey(), tableName);
            if (org.apache.commons.lang3.StringUtils.isNotBlank(temp)) {
                fromStorage = entry.getValue();
                partition = temp;
                break;
            }
        }
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!org.apache.commons.lang3.StringUtils.equals(fromStorage, entry.getValue())) {
                toStorage = entry.getValue();
            }
        }

        return String
            .format("ALTER TABLEGROUP %s move PARTITIONS %s to '%s'", tableGroup, partition, toStorage);
    }

    protected String getCurrentDbName(Statement stmt) throws SQLException {
        String dbName = null;
        ResultSet resultSet = stmt.executeQuery("select database()");
        if (resultSet.next()) {
            dbName = resultSet.getString(1);
        }
        return dbName;
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

    @SneakyThrows
    protected List<DdlRecordInfo> getDdlRecordInfoList(String schemaName, String tableName) {
        List<Map<String, String>> list = getDdlRecordList(schemaName, tableName);
        return list.stream().map(this::parseMapToDdlRecordInfo).collect(Collectors.toList());
    }

    @SneakyThrows
    protected List<DdlRecordInfo> getDdlRecordInfoListByToken(String tokenHints) {
        List<Map<String, String>> list = getDdlRecordListByToken(tokenHints);
        return list.stream().map(this::parseMapToDdlRecordInfo).collect(Collectors.toList());
    }

    private DdlRecordInfo parseMapToDdlRecordInfo(Map<String, String> r) {
        DdlRecordInfo ddlRecordInfo = new DdlRecordInfo();
        ddlRecordInfo.setId(r.get("ID"));
        ddlRecordInfo.setJobId(r.get("JOB_ID"));
        ddlRecordInfo.setDdlSql(r.get("DDL_SQL"));
        ddlRecordInfo.setSqlKind(r.get("SQL_KIND"));
        ddlRecordInfo.setSchemaName(r.get("SCHEMA_NAME"));
        ddlRecordInfo.setTableName(r.get("TABLE_NAME"));
        ddlRecordInfo.setVisibility(Integer.parseInt(r.get("VISIBILITY")));

        String extStr = r.get("EXT");
        if (StringUtils.isNotBlank(extStr)) {
            DDLExtInfo ddlExtInfo = JSONObject.parseObject(extStr, DDLExtInfo.class);
            ddlRecordInfo.setDdlExtInfo(ddlExtInfo);
        }

        String metaInfoStr = r.get("META_INFO");
        if (StringUtils.isNotBlank(metaInfoStr)) {
            CdcManager.MetaInfo metaInfo = JSONObject.parseObject(metaInfoStr, CdcManager.MetaInfo.class);
            ddlRecordInfo.setMetaInfo(metaInfo);
        }

        return ddlRecordInfo;
    }

    private List<Map<String, String>> getDdlRecordList(String schemaName, String tableName) throws SQLException {
        List<Map<String, String>> list = new ArrayList<>();
        String sql;
        if (StringUtils.isNotBlank(tableName)) {
            sql = "select * from __cdc__." + CdcTableUtil.CDC_DDL_RECORD_TABLE
                + " where schema_name = '" + schemaName + "' and table_name  = '" + tableName
                + "' and gmt_created >= ? " + " order by gmt_created desc, id desc";
        } else {
            sql = "select * from __cdc__." + CdcTableUtil.CDC_DDL_RECORD_TABLE
                + " where schema_name = '" + schemaName + "' and gmt_created >=? order by gmt_created desc, id desc";
        }

        try (PreparedStatement stmt = tddlConnection.prepareStatement(sql)) {
            stmt.setDate(1, new Date(lastQueryDdlRecordTimestamp));
            try (ResultSet resultSet = stmt.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int colCnt = metaData.getColumnCount();
                while (resultSet.next()) {
                    Map<String, String> map = new HashMap<>();
                    for (int i = 1; i <= colCnt; i++) {
                        String columnName = metaData.getColumnName(i);
                        String value = resultSet.getString(i);
                        map.put(columnName, value);
                    }
                    list.add(map);
                }
            }
        }
        return list;
    }

    private List<Map<String, String>> getDdlRecordListByToken(String tokenHints) throws SQLException {
        List<Map<String, String>> list = new ArrayList<>();
        String sql = "select * from __cdc__." + CdcTableUtil.CDC_DDL_RECORD_TABLE
            + " where ddl_sql like '%" + tokenHints + "%' "
            + " and gmt_created >=? order by gmt_created desc, id desc";

        try (PreparedStatement stmt = tddlConnection.prepareStatement(sql)) {
            stmt.setDate(1, new Date(lastQueryDdlRecordTimestamp));
            try (ResultSet resultSet = stmt.executeQuery()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int colCnt = metaData.getColumnCount();
                while (resultSet.next()) {
                    Map<String, String> map = new HashMap<>();
                    for (int i = 1; i <= colCnt; i++) {
                        String columnName = metaData.getColumnName(i);
                        String value = resultSet.getString(i);
                        map.put(columnName, value);
                    }
                    list.add(map);
                }
            }
        }
        return list;
    }

    @SneakyThrows
    protected Map<String, Set<String>> queryTopology(String tableName) {
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

        // see -> com.alibaba.polardbx.cdc.MetaBuilder.tryFilterTargetDb
        List<String> tblTypeList = JdbcUtil.executeQueryAndGetColumnResult(
            "select * from table_partitions where table_name = '" + tableName + "'",
            getMetaConnection(), "tbl_type");
        if (!tblTypeList.isEmpty() && Integer.parseInt(tblTypeList.get(0)) == PartitionTableType.BROADCAST_TABLE
            .getTableTypeIntValue() && result.size() > 1) {
            String validKey = result.keySet().stream().min(Comparator.comparing(String::toString)).get();
            result.entrySet().removeIf(e -> !StringUtils.equals(validKey, e.getKey()));
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
        String seed = "/*+TDDL:CMD_EXTRA(CDC_RANDOM_DDL_TOKEN=\"%s\")*/";
        return String.format(seed, UUID.randomUUID());
    }

    protected Timestamp getLastDdlRecordTimestamp(String schemaName) throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(
                "select max(GMT_CREATED) from __cdc__." + CdcTableUtil.CDC_DDL_RECORD_TABLE + " where SCHEMA_NAME ='"
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
                "select ddl_sql,ext from __cdc__." + CdcTableUtil.CDC_DDL_RECORD_TABLE + " where GMT_CREATED > '" + ts
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

    protected List<String> getGsiList(String dbname, String tableName) throws SQLException {
        ArrayList<String> result = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(
                "show global index from " + dbname + "." + tableName)) {
                while (resultSet.next()) {
                    result.add(resultSet.getString("KEY_NAME"));
                }
            }
        }
        return result;
    }

    protected String getMarkSqlForImplicitTableGroup(String dbName, String tableName) throws SQLException {
        List<DdlRecordInfo> list = getDdlRecordInfoList(dbName, tableName);
        return list.get(0).getEffectiveSql();
    }

    protected Set<String> getTableGroups(Statement stmt) throws SQLException {
        Set<String> set = new HashSet<>();
        try (ResultSet resultSet = stmt.executeQuery("show tablegroup")) {
            while (resultSet.next()) {
                set.add(resultSet.getString("TABLE_GROUP_NAME"));
            }
        }
        return set;
    }

    protected void assertSqlEquals(String inputSql, String markSql) {
        SQLStatement inputStmt = SQLHelper.parseSql(inputSql);
        SQLStatement markStmt = SQLHelper.parseSql(markSql);
        removeDdlIdComments(inputStmt);
        removeDdlIdComments(markStmt);

        Assert.assertEquals(inputStmt.toString(), markStmt.toString());

        // 1. 曾经出现过如下SQL会被parse为MySqlHintStatement + SQLGrantStatement 的情况，是不符合预期的
        // 2. 我们的parser对带hints的sql进行parse处理时，还有不完善的地方，其它场景也会有会类似不符合预期的行为，这里加一个判断
        ///*+TDDL:CMD_EXTRA(CDC_RANDOM_DDL_TOKEN="8f4ccd00-3999-4b22-8160-57dc7e2ec56c")*/GRANT SELECT,UPDATE ON `null`.* TO 'cdc_user_6516803699950266368'@'127.0.0.1'
        Assert.assertNotEquals(markStmt.getClass(), MySqlHintStatement.class);
    }
}
