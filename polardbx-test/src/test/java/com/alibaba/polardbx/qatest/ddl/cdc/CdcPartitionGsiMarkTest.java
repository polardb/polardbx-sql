package com.alibaba.polardbx.qatest.ddl.cdc;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CdcPartitionGsiMarkTest extends CdcGsiMarkBaseTest {

    @Before
    public void before() {
        this.GSI_TEST_DB = "gsi_test_auto";
        this.CAST_TB_WITH_GSI = "CREATE TABLE `%s`(\n"
            + "  `id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
            + "  `order_id` varchar(20) DEFAULT NULL,\n"
            + "  `buyer_id` varchar(20) DEFAULT NULL,\n"
            + "  `seller_id` varchar(20) DEFAULT NULL,\n"
            + "  `order_snapshot` longtext DEFAULT NULL,\n"
            + "  `order_detail` longtext DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  UNIQUE KEY `l_i_order` (`order_id`),\n"
            + "  GLOBAL INDEX `g_i_seller` (`seller_id`)  partition by hash(`seller_id`),\n"
            + "  GLOBAL UNIQUE INDEX `g_i_buyer` (`buyer_id`) COVERING (order_snapshot) partition by hash(`buyer_id`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8";
        this.MODE = "auto";
        this.CREATE_GSI_DDL = "CREATE GLOBAL INDEX `%s` on `%s`(`%s`) COVERING (%s) partition by hash(%s)";
        this.ALTER_ADD_GSI = "alter table %s add GLOBAL INDEX `%s`(`%s`) COVERING (%s) partition by hash(%s)";
        this.ADDL_CLUSTER_INDEX = "create clustered index %s on %s(%s) partition by hash(`%s`)";
    }

    @Test
    public void testAutoPartitionTableWithGSI() throws SQLException {
        String indexName = "g_i_buyer";
        String tableName = "t_order_gsi1";
        try (Connection conn = getPolardbxConnection()) {
            Statement ps = conn.createStatement();
            prepareTestDatabase(ps);
            createTable(ps, tableName, "");
            dropGSI(ps, tableName, indexName);
            createGSI(ps, tableName, indexName);
            dropConveringColumn(ps, tableName);
            dropGSI(ps, tableName, indexName);
            addClusterIndex(ps, tableName, indexName, INDEX_COLUMN);
            dropConveringColumn(ps, tableName);
        }
    }

    @Test
    public void testPartitionTableWithGSI() throws SQLException {
        String indexName = "g_i_buyer";
        String tableName = "t_order_gsi1";
        try (Connection conn = getPolardbxConnection()) {
            Statement ps = conn.createStatement();
            prepareTestDatabase(ps);
            createTable(ps, tableName, "partition by hash(`order_id`)");
            dropGSI(ps, tableName, indexName);
            createGSI(ps, tableName, indexName);
            dropConveringColumn(ps, tableName);
            dropGSI(ps, tableName, indexName);
            alterAddGSI(ps, tableName, indexName);
            rename(ps, tableName, indexName, indexName + "_target");
            dropGSI(ps, tableName, indexName + "_target");
            addClusterIndex(ps, tableName, indexName, INDEX_COLUMN);
            dropConveringColumn(ps, tableName);
        }
    }

    protected void createTable(Statement stmt, String tableName, String partitionBy) throws SQLException {
        String token = buildTokenHints();
        useDB(stmt, GSI_TEST_DB);
        String ddl = token + String.format(CAST_TB_WITH_GSI, tableName) + " " + partitionBy;
        stmt.execute(ddl);
    }

}
