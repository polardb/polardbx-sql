package com.alibaba.polardbx.qatest.ddl.sharding.cdc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CdcShardGsiMarkTest extends CdcGsiMarkBaseTest {

    @Test
    public void testTableWithGSI() throws SQLException {
        String indexName = "g_i_buyer";
        String tableName = "t_order_gsi1";
        try (Connection conn = getPolardbxConnection()) {
            Statement ps = conn.createStatement();
            prepareTestDatabase(ps);
            createTable(ps, tableName);
            dropGSI(ps, tableName, indexName);
            createGSI(ps, tableName, indexName);
            dropConveringColumn(ps, tableName);
            dropGSI(ps, tableName, indexName);
            alterAddGSI(ps, tableName, indexName);
            rename(ps, tableName, indexName, indexName + "_target");
            dropGSI(ps, tableName, indexName + "_target");
            addClusterIndex(ps, tableName, indexName, INDEX_COLUMN);
            dropConveringColumn(ps, tableName);
            dropGSI(ps, tableName, indexName);
        }
    }

}
