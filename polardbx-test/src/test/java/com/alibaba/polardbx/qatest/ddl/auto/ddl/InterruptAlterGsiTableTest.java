package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class InterruptAlterGsiTableTest extends InterruptAlterTableTest {

    protected static final String HINT_ALLOW_MODIFY_GSI = "/*+TDDL:CMD_EXTRA(ALLOW_ALTER_GSI_INDIRECTLY=TRUE)*/";

    @Test
    @Override
    public void t01_normal_alter() throws SQLException {
        ALTER_TABLE = HINT_ALLOW_MODIFY_GSI + ALTER_TABLE;
        createGSI("global", "gsi_1", "c3");
        createGSI("unique global", "ugsi_1", "c4");
    }

    protected void createGSI(String indexType, String indexName, String columnName) throws SQLException {
        String sql = String.format("alter table %s add %s index %s(%s) covering(c2) partition by key(%s) partitions 4",
            TEST_TABLE, indexType, indexName, columnName, columnName);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql)) {
            ps.executeUpdate();
        }
    }

}

