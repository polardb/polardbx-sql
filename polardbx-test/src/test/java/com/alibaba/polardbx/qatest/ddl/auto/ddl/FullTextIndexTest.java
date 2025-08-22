package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class FullTextIndexTest extends DDLBaseNewDBTestCase {

    private static final Logger logger = LoggerFactory.getLogger(FullTextIndexTest.class);

    static String databaseName = "auto_fulltext_index_test";
    private final Map<String, String> tableDefs = Arrays.asList(new Object[][]{
                    {"t1", "create table t1(id bigint auto_increment, content text, primary key(id), FULLTEXT INDEX fulltext_idx (content)) tablegroup=%s partition by key(id) partitions 3"},
                    {"t2", "create table t2(id bigint auto_increment, content text, primary key(id)) tablegroup=%s partition by key(id) partitions 3"},
                    {"t3", "create table t3(id bigint auto_increment, content text, primary key(id)) tablegroup=%s partition by key(id) partitions 3"}
            }).stream()
            .collect(Collectors.toMap(
                    pair -> (String) pair[0],
                    pair -> (String) pair[1]
            ));

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @BeforeClass
    public static void setUpTestSuite() {
        try (Connection tddlConn = ConnectionManager.getInstance().getDruidPolardbxConnection();) {
            String dropDb = String.format("drop database if exists %s", databaseName);
            JdbcUtil.executeUpdateSuccess(tddlConn, dropDb);
            JdbcUtil.executeUpdateSuccess(tddlConn, "use polardbx");
            String createDb = String.format("create database %s mode=auto", databaseName);
            JdbcUtil.executeUpdateSuccess(tddlConn, createDb);
        } catch (Exception e) {
            logger.error("", e);
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testMoveTable() throws SQLException {
        String tgName = "mytg1";
        String useDb = String.format("use %s", databaseName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, useDb);
        JdbcUtil.executeSuccess(tddlConnection, String.format("create tablegroup if not exists %s", tgName));
        String insertSql = "insert into %s(content) values(?)";
        String querySql = "SELECT * FROM t1 " +
                "WHERE MATCH(content) AGAINST('ab' IN BOOLEAN MODE);";
        for (Map.Entry<String, String> entry : tableDefs.entrySet()) {
            String dropTable = String.format("drop table if exists %s", entry.getKey());
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
            String createTable = entry.getValue();
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTable, tgName));
            int i = 0;
            try (PreparedStatement ps = JdbcUtil.preparedStatement(String.format(insertSql, entry.getKey()), tddlConnection)) {
                while (i < 100) {
                    ps.setString(1, RandomUtils.getStringBetween(20, 1000));
                    ps.addBatch();
                    i++;
                }
                ps.executeBatch();
            }
            if (entry.getKey().equalsIgnoreCase("t1")) {
                JdbcUtil.executeSuccess(tddlConnection, querySql);
            }

            if (entry.getKey().equalsIgnoreCase("t2")) {
                JdbcUtil.executeSuccess(tddlConnection, "alter table t2 add fulltext index ft1(content);");
            }
        }
        String showTopology = "show topology from t1";
        ResultSet rs = JdbcUtil.executeQuery(showTopology, tddlConnection);
        Set<String> storageIds = new HashSet<>();
        List<String> storageIdList = new ArrayList<>();
        while (rs.next()) {
            String storageId = rs.getString("DN_ID");
            if (!storageIds.contains(storageId)) {
                storageIds.add(storageId);
            }
        }
        storageIdList.addAll(storageIds);
        String moveTgSql = "alter tablegroup %s move partitions p1,p2,p3 to '%s'";
        List<String> planInfos = new ArrayList<>();
        rs = JdbcUtil.executeQuery("explain " + String.format(moveTgSql, tgName, storageIdList.get(0)), tddlConnection);
        while (rs.next()) {
            planInfos.add(rs.getString(1));
        }

        Assert.assertTrue(planInfos.stream().anyMatch(planInfo -> planInfo.contains("LOGICAL_BACKFILL")));

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(moveTgSql, tgName, storageIdList.get(0)));
        JdbcUtil.executeSuccess(tddlConnection, querySql);
        String moveTbSql = "alter table %s move partitions p1,p2,p3 to '%s'";
        if (storageIdList.size() > 1) {
            for (Map.Entry<String, String> entry : tableDefs.entrySet()) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(moveTbSql, entry.getKey(), storageIdList.get(1)));
            }
            JdbcUtil.executeSuccess(tddlConnection, querySql);
        }
    }

    @Test
    public void testFullTextIndexType() throws SQLException {
        String tgName = "mytg1";
        String useDb = String.format("use %s", databaseName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, useDb);
        JdbcUtil.executeSuccess(tddlConnection, String.format("create tablegroup if not exists %s", tgName));
        String showIndex = "show index from %s";
        for (Map.Entry<String, String> entry : tableDefs.entrySet()) {
            String dropTable = String.format("drop table if exists %s", entry.getKey());
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropTable);
            String createTable = entry.getValue();
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(createTable, tgName));
            ResultSet rs = JdbcUtil.executeQuery(String.format(showIndex, entry.getKey()), tddlConnection);
            while (rs.next()) {
                String indexType = rs.getString("Index_type");
                String key_name = rs.getString("Key_name");
                if (key_name.equalsIgnoreCase("fulltext_idx")) {
                    Assert.assertEquals(indexType, "FULLTEXT");
                }
            }
            if (entry.getKey().equalsIgnoreCase("t2")) {
                JdbcUtil.executeSuccess(tddlConnection, "alter table t2 add fulltext index fulltext_idx(content);");
                rs = JdbcUtil.executeQuery(String.format(showIndex, entry.getKey()), tddlConnection);
                while (rs.next()) {
                    String indexType = rs.getString("Index_type");
                    String key_name = rs.getString("Key_name");
                    if (key_name.equalsIgnoreCase("fulltext_idx")) {
                        Assert.assertEquals(indexType, "FULLTEXT");
                    }
                }
            }
        }
    }

    private boolean isPhysicalBackfillEnable() throws SQLException {
        String queryInstConfig = String.format("select param_key, param_val from inst_config where param_key='%s'",
                ConnectionProperties.PHYSICAL_BACKFILL_ENABLE);
        try (Connection conn = getMetaConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(queryInstConfig)) {
            if (rs.next()) {
                return rs.getString(2).equalsIgnoreCase("true");
            } else {
                return true;
            }
        }
    }
}
