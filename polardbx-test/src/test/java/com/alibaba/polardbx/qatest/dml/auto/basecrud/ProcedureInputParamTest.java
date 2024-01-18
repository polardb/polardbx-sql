package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

@FileStoreIgnore
public class ProcedureInputParamTest extends BaseTestCase {
    protected Connection tddlConnection;

    private String PROCEDURE_NAME = "procedure_input_param_test";

    final private String TABLE_NAME = "t_input_param_test";

    private String DROP_PROCEDURE = "DROP PROCEDURE IF EXISTS %s";

    @Before
    public void getConnection() {
        this.tddlConnection = getPolardbxConnection();
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_PROCEDURE, PROCEDURE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, "drop table if exists " + TABLE_NAME);
        JdbcUtil.executeSuccess(tddlConnection, "create table " + TABLE_NAME + " (c1 char(255), c2 longblob, c3 blob)");
    }

    @Test
    public void testInputBlobType() throws SQLException {
        String createProcedure =
            String.format(
                "create procedure %s(in data longblob, in data2 blob) begin insert into %s values ('111', data, data2); end;",
                PROCEDURE_NAME, TABLE_NAME);
        JdbcUtil.executeSuccess(tddlConnection, createProcedure);
        byte[] value1 = new byte[] {(byte) 0xa4, (byte) 0xa5};
        byte[] value2 = new byte[] {(byte) 0xa4, (byte) 0xa5, (byte) 0x1};
        try (PreparedStatement statement = tddlConnection.prepareStatement(
            String.format("call %s(?, ?)", PROCEDURE_NAME));) {
            statement.setBytes(1, value1);
            statement.setBytes(2, value2);
            statement.executeUpdate();
        }

        ResultSet rs = JdbcUtil.executeQuery("select * from " + TABLE_NAME, tddlConnection);
        if (rs.next()) {
            Blob blob = rs.getBlob("c2");
            Assert.assertTrue(Arrays.equals(blob.getBytes(1, (int) blob.length()), value1));
            blob = rs.getBlob("c3");
            Assert.assertTrue(Arrays.equals(blob.getBytes(1, (int) blob.length()), value2));
        } else {
            Assert.fail("should have result");
        }
    }

    @After
    public void clearData() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_PROCEDURE, PROCEDURE_NAME));
    }
}
