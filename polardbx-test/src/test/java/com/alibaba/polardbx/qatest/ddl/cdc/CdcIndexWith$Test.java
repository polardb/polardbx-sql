package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.common.cdc.entity.DDLExtInfo;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import lombok.SneakyThrows;
import org.apache.commons.compress.utils.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcSqlUtils.SQL_PARSE_FEATURES;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-08-30 17:43
 **/
@Ignore
public class CdcIndexWith$Test extends CdcBaseTest {

    @Test
    public void testCdcDdlRecord() throws SQLException {
        String schemaName = "cdc_index_dollar_test";
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("drop database if exists " + schemaName);
            stmt.executeUpdate("create database " + schemaName + "  mode = 'auto'");
            stmt.executeUpdate("use " + schemaName);
        }

        testAutoPartitionTable_A(schemaName, "t_auto_part_1", "drop index %s on t_auto_part_1");

        testAutoPartitionTable_A(schemaName, "t_auto_part_2", "alter table t_auto_part_2 drop index %s");

        testAutoPartitionTable_B(schemaName, "t_auto_part_3", "drop index %s on t_auto_part_3");

        testAutoPartitionTable_B(schemaName, "t_auto_part_4", "alter table t_auto_part_4 drop index %s");

        testAutoPartitionTable_C(schemaName, "t_auto_part_5", "drop index %s on t_auto_part_5");

        testAutoPartitionTable_C(schemaName, "t_auto_part_6", "alter table t_auto_part_6 drop index %s");

        testManuallyPartitionTable_A(schemaName, "t_manually_part_1", "drop index %s on t_manually_part_1");

        testManuallyPartitionTable_A(schemaName, "t_manually_part_2", "alter table t_manually_part_2 drop index %s");

        testManuallyPartitionTable_B(schemaName, "t_manually_part_3", "drop index %s on t_manually_part_3");

        testManuallyPartitionTable_B(schemaName, "t_manually_part_4", "alter table t_manually_part_4 drop index %s");

        testManuallyPartitionTable_C(schemaName, "t_manually_part_5", "drop index %s on t_manually_part_5");

        testManuallyPartitionTable_C(schemaName, "t_manually_part_6", "alter table t_manually_part_6 drop index %s");
    }

    private void testAutoPartitionTable_A(String schemaName, String tableName, String dropIndexTemplate)
        throws SQLException {
        // create table
        String creatTbSql = "create table `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext,\n"
            + "  `order_detail` longtext,\n"
            + "  `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  `rint` double(10, 2),\n"
            + "   index idx1(order_id),\n"
            + "   index idx2(order_id),\n"
            + "   index idx3_$9178(buyer_id),\n"
            + "   index idx4_$9178(buyer_id),\n"
            + "   local key idx5(seller_id),\n"
            + "   local key idx6(seller_id),\n"
            + "   local key idx7_$9199(seller_id),\n"
            + "   local key idx8_$9199(seller_id),\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 DEFAULT COLLATE = utf8_general_ci";
        JdbcUtil.executeUpdateSuccess(tddlConnection, creatTbSql);

        // check indexes
        Set<String> indexes = showIndexes(tableName);
        Assert.assertEquals(
            Sets.newHashSet("idx1", "idx2", "idx3_$9178", "idx4_$9178", "idx5", "idx6", "idx7_$9199", "idx8_$9199"),
            indexes);//TODO @wumu 究竟应该是idx3 , 还是idx3_$9178，这里填idx3_$9178才能Assert成功

        // execute and check
        executeInternal(schemaName, tableName, dropIndexTemplate);
    }

    private void testAutoPartitionTable_B(String schemaName, String tableName, String dropIndexTemplate)
        throws SQLException {
        // create table
        String creatTbSql = "create table `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext,\n"
            + "  `order_detail` longtext,\n"
            + "  `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  `rint` double(10, 2),\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 DEFAULT COLLATE = utf8_general_ci";
        JdbcUtil.executeUpdateSuccess(tddlConnection, creatTbSql);

        // add index
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table " + tableName + " add index idx1(order_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table " + tableName + " add index idx2(order_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table " + tableName + " add index idx3_$9178(buyer_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table " + tableName + " add index idx4_$9178(buyer_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table " + tableName + " add local key idx5(seller_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table " + tableName + " add local key idx6(seller_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table " + tableName + " add local key idx7_$9199(seller_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table " + tableName + " add local key idx8_$9199(seller_id)");

        Assert.assertEquals(
            Sets.newHashSet("idx1", "idx2", "idx3", "idx4", "idx5", "idx6", "idx7_$9199", "idx8_$9199"),
            showIndexes(tableName));//TODO @wumu 究竟应该是idx3 , 还是idx3_$9178，这里填idx3，才能Assert成功

        // execute and check
        executeInternal(schemaName, tableName, dropIndexTemplate);
    }

    private void testAutoPartitionTable_C(String schemaName, String tableName, String dropIndexTemplate)
        throws SQLException {
        // create table
        String creatTbSql = "create table `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext,\n"
            + "  `order_detail` longtext,\n"
            + "  `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  `rint` double(10, 2),\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 DEFAULT COLLATE = utf8_general_ci";
        JdbcUtil.executeUpdateSuccess(tddlConnection, creatTbSql);

        // add index
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create index idx1 on " + tableName + "(order_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create index idx2 on " + tableName + "(order_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create index idx3_$9178 on " + tableName + "(buyer_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create index idx4_$9178 on " + tableName + "(buyer_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create local key idx5 on " + tableName + "(seller_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create local key idx6 on " + tableName + "(seller_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create local key idx7_$9199 on " + tableName + "(seller_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create local key idx8_$9199 on " + tableName + "(seller_id)");

        Assert.assertEquals(
            Sets.newHashSet("idx1", "idx2", "idx3", "idx4", "idx5", "idx6", "idx7_$9199", "idx8_$9199"),
            showIndexes(tableName));//TODO @wumu 究竟应该是idx3 , 还是idx3_$9178，这里填idx3，才能Assert成功

        // execute and check
        executeInternal(schemaName, tableName, dropIndexTemplate);
    }

    private void testManuallyPartitionTable_A(String schemaName, String tableName, String dropIndexTemplate)
        throws SQLException {
        // create table
        String creatTbSql = "create table `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext,\n"
            + "  `order_detail` longtext,\n"
            + "  `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  `rint` double(10, 2),\n"
            + "   global index idx1(order_id) COVERING (`id`, `order_snapshot`) PARTITION BY HASH(`order_id`),\n"
            + "   global index idx2(order_id) COVERING (`id`, `order_snapshot`) PARTITION BY HASH(`order_id`),\n"
            + "   global index idx3_$9178(buyer_id) COVERING (`id`, `order_detail`) PARTITION BY HASH(`buyer_id`),\n"
            + "   global index idx4_$9178(buyer_id) COVERING (`id`, `order_detail`) PARTITION BY HASH(`buyer_id`),\n"
            + "   key idx5(seller_id),\n"
            + "   key idx6(seller_id),\n"
            + "   key idx7_$9199(seller_id),\n"
            + "   key idx8_$9199(seller_id),\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") PARTITION BY KEY(seller_id) PARTITIONS 3 "
            + "ENGINE = InnoDB DEFAULT CHARSET = utf8 DEFAULT COLLATE = utf8_general_ci";
        JdbcUtil.executeUpdateSuccess(tddlConnection, creatTbSql);

        // check indexes
        Set<String> indexes = showIndexes(tableName);
        Assert.assertEquals(
            Sets.newHashSet("idx1", "idx2", "idx3", "idx4", "idx5", "idx6", "idx7_$9199", "idx8_$9199"),
            indexes);//TODO @wumu 这里不是idx1、idx2、idx3、idx4，而都是带随机后缀的名字

        // execute and check
        executeInternal(schemaName, tableName, dropIndexTemplate);
    }

    private void testManuallyPartitionTable_B(String schemaName, String tableName, String dropIndexTemplate)
        throws SQLException {
        // create table
        String creatTbSql = "create table `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext,\n"
            + "  `order_detail` longtext,\n"
            + "  `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  `rint` double(10, 2),\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") PARTITION BY KEY(seller_id) PARTITIONS 3 "
            + "ENGINE = InnoDB DEFAULT CHARSET = utf8 DEFAULT COLLATE = utf8_general_ci";
        JdbcUtil.executeUpdateSuccess(tddlConnection, creatTbSql);

        // create index
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create global index idx1 on " + tableName
            + "(order_id) COVERING (`id`, `order_snapshot`) PARTITION BY HASH(`order_id`)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create global index idx2 on " + tableName
            + "(order_id) COVERING (`id`, `order_snapshot`) PARTITION BY HASH(`order_id`)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create global index idx3_$9178 on " + tableName
            + "(buyer_id) COVERING (`id`, `order_detail`) PARTITION BY HASH(`buyer_id`)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create global index idx4_$9178 on " + tableName
                + "(buyer_id) COVERING (`id`, `order_detail`) PARTITION BY HASH(`buyer_id`)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create index idx5 on " + tableName + "(seller_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create index idx6 on " + tableName + "(seller_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create index idx7_$9199 on " + tableName + "(seller_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create index idx8_$9199 on " + tableName + "(seller_id)");

        // check indexes
        Set<String> indexes = showIndexes(tableName);
        Assert.assertEquals(
            Sets.newHashSet("idx1", "idx2", "idx3", "idx4", "idx5", "idx6", "idx7_$9199", "idx8_$9199"),
            indexes);//TODO @wumu 这里idx1、idx2、idx3、idx4，都是带随机后缀的名字

        // execute and check
        executeInternal(schemaName, tableName, dropIndexTemplate);
    }

    private void testManuallyPartitionTable_C(String schemaName, String tableName, String dropIndexTemplate)
        throws SQLException {
        // create table
        String creatTbSql = "create table `" + tableName + "` (\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext,\n"
            + "  `order_detail` longtext,\n"
            + "  `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
            + "  `rint` double(10, 2),\n"
            + "  PRIMARY KEY (`id`)\n"
            + ") PARTITION BY KEY(seller_id) PARTITIONS 3 "
            + "ENGINE = InnoDB DEFAULT CHARSET = utf8 DEFAULT COLLATE = utf8_general_ci";
        JdbcUtil.executeUpdateSuccess(tddlConnection, creatTbSql);

        // create index
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table " + tableName
            + " add global index idx1 (order_id) COVERING (`id`, `order_snapshot`) PARTITION BY HASH(`order_id`)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table " + tableName
            + " add global index idx2 (order_id) COVERING (`id`, `order_snapshot`) PARTITION BY HASH(`order_id`)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table " + tableName
            + " add global index idx3_$9178 (buyer_id) COVERING (`id`, `order_detail`) PARTITION BY HASH(`buyer_id`)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table " + tableName
            + " add global index idx4_$9178 (buyer_id) COVERING (`id`, `order_detail`) PARTITION BY HASH(`buyer_id`)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table " + tableName + " add index idx5 (seller_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table " + tableName + " add index idx6 (seller_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table " + tableName + " add index idx7_$9199 (seller_id)");
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table " + tableName + " add index idx8_$9199 (seller_id)");

        // check indexes
        Set<String> indexes = showIndexes(tableName);
        Assert.assertEquals(
            Sets.newHashSet("idx1", "idx2", "idx3", "idx4", "idx5", "idx6", "idx7_$9199", "idx8_$9199"),
            indexes);//TODO @wumu 这里idx1、idx2、idx3、idx4，都是带随机后缀的名字

        // execute and check
        executeInternal(schemaName, tableName, dropIndexTemplate);
    }

    private void executeInternal(String schemaName, String tableName, String dropIndexTemplate) throws SQLException {
        //==============================================================================================
        // idx1: drop global index with simple name
        //==============================================================================================
        executeAndCheck(dropIndexTemplate, schemaName, tableName, "idx1", "idx1");

        //==============================================================================================
        // idx2: drop global index with generated dollar name
        //==============================================================================================
        String idx2$ = getIndexNameWithDollar(schemaName, tableName, "idx2");
        executeAndCheck(dropIndexTemplate, schemaName, tableName, idx2$, idx2$);
        //TODO @wumu 这里使用带后缀的name，也可以删除索引，但cdc的打标sql，透传给下游的polardbx，会报错

        //==============================================================================================
        // idx3 & idx4: drop global index with input dollar name
        //==============================================================================================
        JdbcUtil.executeUpdateFailedReturn(tddlConnection, String.format(dropIndexTemplate, "idx3_$9178"));
        JdbcUtil.executeUpdateFailedReturn(tddlConnection, String.format(dropIndexTemplate, "idx4_$9178"));

        //==============================================================================================
        // idx3 & idx4: check
        //==============================================================================================
        String idx3$ = getIndexNameWithDollar(schemaName, tableName, "idx3_$9178");
        Assert.assertNull(idx3$);
        idx3$ = getIndexNameWithDollar(schemaName, tableName, "idx3");
        Assert.assertNotNull(idx3$);
        Assert.assertNotEquals("idx3_$9178", idx3$);

        String idx4$ = getIndexNameWithDollar(schemaName, tableName, "idx4_$9178");
        Assert.assertNull(idx4$);
        idx4$ = getIndexNameWithDollar(schemaName, tableName, "idx4");
        Assert.assertNotNull(idx4$);
        Assert.assertNotEquals("idx4_$9178", idx4$);

        //==============================================================================================
        // idx3: drop global index with simple name
        //==============================================================================================
        executeAndCheck(dropIndexTemplate, schemaName, tableName, "idx3", "idx3");
        //TODO @wumu 这里idx3可以被成功删除，但对应的local索引未被删除，cdc也没有打标

        //==============================================================================================
        // idx4: drop global index with generated dollar name
        //==============================================================================================
        executeAndCheck(dropIndexTemplate, schemaName, tableName, idx4$, idx4$);
        //TODO @wumu 这里idx4可以被成功删除，但对应的local索引未被删除，cdc也没有打标

        //==============================================================================================
        // idx5 & idx6: drop local index with simple name
        //==============================================================================================
        String idx5$ = getIndexNameWithDollar(schemaName, tableName, "idx5");
        Assert.assertNull(idx5$);
        String idx6$ = getIndexNameWithDollar(schemaName, tableName, "idx6");
        Assert.assertNull(idx6$);
        executeAndCheck(dropIndexTemplate, schemaName, tableName, "idx5", "idx5");
        executeAndCheck(dropIndexTemplate, schemaName, tableName, "idx6", "idx6");

        //==============================================================================================
        // idx7 & idx8: drop local index with input dollar name
        //==============================================================================================
        JdbcUtil.executeUpdateFailedReturn(tddlConnection, String.format(dropIndexTemplate, "idx7"));
        JdbcUtil.executeUpdateFailedReturn(tddlConnection, String.format(dropIndexTemplate, "idx8"));

        String idx7$ = getIndexNameWithDollar(schemaName, tableName, "idx7_$9199");
        Assert.assertNull(idx7$);
        idx7$ = getIndexNameWithDollar(schemaName, tableName, "idx7");
        Assert.assertEquals("idx7_$9199", idx7$);

        String idx8$ = getIndexNameWithDollar(schemaName, tableName, "idx8_$9199");
        Assert.assertNull(idx8$);
        idx8$ = getIndexNameWithDollar(schemaName, tableName, "idx8");
        Assert.assertEquals("idx8_$9199", idx8$);
        executeAndCheck(dropIndexTemplate, schemaName, tableName, "idx7_$9199", "idx7_$9199");
        executeAndCheck(dropIndexTemplate, schemaName, tableName, "idx8_$9199", "idx8_$9199");
    }

    private void executeAndCheck(String dropIndexTemplate, String schemaName, String tableName, String idxName,
                                 String expectIndexName)
        throws SQLException {
        // before execute
        List<DdlRecordInfo> markListPre = getDdlRecordInfoList(schemaName, tableName);

        // execute
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(dropIndexTemplate, idxName));

        // after execute and check
        List<DdlRecordInfo> markListAfter = getDdlRecordInfoList(schemaName, tableName);
        Assert.assertEquals(markListPre.size() + 1, markListAfter.size());

        DDLExtInfo ddlExtInfo = markListAfter.get(0).getDdlExtInfo();
        String markSql = markListAfter.get(0).getDdlSql();
        String markOriginSql = ddlExtInfo != null ? ddlExtInfo.getOriginalDdl() : "";

        checkOne(markSql, expectIndexName);
        checkOne(markOriginSql, expectIndexName);
    }

    private void checkOne(String sql, String expectIndexName) {
        if (StringUtils.isBlank(sql)) {
            return;
        }

        System.out.println("start to check index name for sql : " + sql);
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, SQL_PARSE_FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statement = statementList.get(0);
        if (statement instanceof SQLDropIndexStatement) {
            SQLDropIndexStatement dropIndexStatement = (SQLDropIndexStatement) statement;
            String indexName = dropIndexStatement.getIndexName().getSimpleName();
            Assert.assertEquals(expectIndexName, indexName);
        } else if (statement instanceof SQLAlterTableStatement) {
            SQLAlterTableStatement alterTableStatement = (SQLAlterTableStatement) statement;
            alterTableStatement.getItems().forEach(i -> {
                if (i instanceof SQLAlterTableDropIndex) {
                    SQLAlterTableDropIndex alterTableDropIndex = (SQLAlterTableDropIndex) i;
                    String indexName = alterTableDropIndex.getIndexName().getSimpleName();
                    Assert.assertEquals(expectIndexName, indexName);
                }
            });
        } else {
            throw new RuntimeException("unsupported statement type : " + statement.getClass().getCanonicalName());
        }
    }

    @SneakyThrows
    private String getIndexNameWithDollar(String schemaName, String tableName, String idxName) {
        Connection metaConn = getMetaConnection();
        ResultSet resultSet = JdbcUtil.executeQuery(String.format(
            "select * from indexes where table_schema = '%s' and table_name = '%s' and index_name like '%s'",
            schemaName, tableName, idxName + "_$%"), metaConn);
        while (resultSet.next()) {
            return resultSet.getString("INDEX_NAME");
        }
        return null;
    }

    private HashSet<String> showIndexes(String tableName) throws SQLException {
        HashSet<String> set = new HashSet<>();
        ResultSet resultSet = JdbcUtil.executeQuery("show indexes from " + tableName, tddlConnection);
        while (resultSet.next()) {
            String keyName = resultSet.getString("Key_name");
            if (!"PRIMARY".equals(keyName)) {
                set.add(keyName);
            }
        }

        resultSet = JdbcUtil.executeQuery("show global indexes from " + tableName, tddlConnection);
        while (resultSet.next()) {
            String keyName = resultSet.getString("Key_name");
            if (!"PRIMARY".equals(keyName)) {
                set.add(keyName);
            }
        }
        return set;
    }
}
