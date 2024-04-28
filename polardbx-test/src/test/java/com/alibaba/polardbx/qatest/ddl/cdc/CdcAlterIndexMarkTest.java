package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.DdlScope;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-08-30 17:43
 **/
public class CdcAlterIndexMarkTest extends CdcBaseTest {

    @Test
    public void testCdcDdlRecord() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate("drop database if exists cdc_alter_index_test");
            stmt.executeUpdate("create database cdc_alter_index_test  mode = 'auto'");
            stmt.executeUpdate("use cdc_alter_index_test");
            stmt.executeUpdate("create table if not exists lc_rc_tp1 ( "
                + "a bigint unsigned not null, "
                + "b bigint unsigned not null, "
                + "c datetime NOT NULL, "
                + "d varchar(16) NOT NULL, "
                + "e varchar(16) NOT NULL ) "
                + "partition by list columns (a,b) "
                + "subpartition by range columns (c,d) subpartition template ( "
                + "subpartition sp0 values less than ('2020-01-01','a'), "
                + "subpartition sp1 values less than (maxvalue,maxvalue) ) ( partition p0 values in ((5,5),(6,6)), partition p1 values in ((7,7),(8,8)) )");

            executeAndCheck(stmt,
                "create global index `gsi_lc` on `lc_rc_tp1` ( a,b,c,d ) partition by list columns (a,b) ( partition p0 values in ((5,5),(6,6),(9,9)), partition p1 values in ((7,7),(8,8)) )",
                "lc_rc_tp1",
                "CREATE GLOBAL INDEX `gsi_lc` ON `lc_rc_tp1` (a, b, c, d) PARTITION BY LIST COLUMNS (a, b) (\n"
                    + "\tPARTITION p0 VALUES IN ((5, 5), (6, 6), (9, 9)), \n"
                    + "\tPARTITION p1 VALUES IN ((7, 7), (8, 8))\n"
                    + ")", CdcDdlMarkVisibility.Public.getValue());

            executeAndCheck(stmt,
                "create global index `gsi_rc` on `lc_rc_tp1` ( c,d,a,b ) partition by range columns (c,d) ( partition sp0 values less than ('2021-01-01','a'), partition sp1 values less than (maxvalue,maxvalue) )",
                "lc_rc_tp1",
                "CREATE GLOBAL INDEX `gsi_rc` ON `lc_rc_tp1` (c, d, a, b) PARTITION BY RANGE COLUMNS (c, d) (\n"
                    + "\tPARTITION sp0 VALUES LESS THAN ('2021-01-01', 'a'),\n"
                    + "\tPARTITION sp1 VALUES LESS THAN (maxvalue, maxvalue)\n"
                    + ")", CdcDdlMarkVisibility.Public.getValue());

            executeAndCheck(stmt,
                "create global index `gsi_lc_rc` on `lc_rc_tp1` ( a,b,c,d ) partition by list columns (a,b) subpartition by range columns (c,d) subpartition template ( subpartition sp0 values less than ('2021-01-01','a'), subpartition sp1 values less than (maxvalue,maxvalue) ) ( partition p0 values in ((5,5),(6,6),(9,9)), partition p1 values in ((7,7),(8,8)) )",
                "lc_rc_tp1",
                "CREATE GLOBAL INDEX `gsi_lc_rc` ON `lc_rc_tp1` (a, b, c, d) PARTITION BY LIST COLUMNS (a, b)\n"
                    + "SUBPARTITION BY RANGE COLUMNS (c, d) (\n"
                    + "\tSUBPARTITION sp0 VALUES LESS THAN ('2021-01-01', 'a'),\n"
                    + "\tSUBPARTITION sp1 VALUES LESS THAN (maxvalue, maxvalue)\n"
                    + ") (\n"
                    + "\tPARTITION p0 VALUES IN ((5, 5), (6, 6), (9, 9)), \n"
                    + "\tPARTITION p1 VALUES IN ((7, 7), (8, 8))\n"
                    + ")", CdcDdlMarkVisibility.Public.getValue());

            executeAndCheck(stmt,
                "/*# add **/alter index gsi_lc on table lc_rc_tp1 add partition ( partition p2 values in ((11,11),(10,10)) )",
                "lc_rc_tp1",
                "ALTER INDEX gsi_lc ON TABLE lc_rc_tp1\n"
                    + "\tADD PARTITION (PARTITION p2 VALUES IN ((11, 11), (10, 10)))",
                CdcDdlMarkVisibility.Protected.getValue());

            executeAndCheck(stmt,
                "alter index gsi_lc_rc on table lc_rc_tp1 modify partition p1 add values ( (15,15) )",
                "lc_rc_tp1"
                , "ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
                    + "\tMODIFY PARTITION p1 ADD VALUES ((15, 15))", CdcDdlMarkVisibility.Protected.getValue());

            executeAndCheck(stmt,
                "/*# split **/alter index gsi_lc_rc on table lc_rc_tp1 split subpartition sp1 into ( subpartition sp1 values less than ('2022-01-01','a'), subpartition sp2 values less than (maxvalue, maxvalue) )",
                "lc_rc_tp1",
                "ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
                    + "\tSPLIT SUBPARTITION sp1 INTO (SUBPARTITION sp1 VALUES LESS THAN ('2022-01-01', 'a'), SUBPARTITION sp2 VALUES LESS THAN (maxvalue, maxvalue))",
                CdcDdlMarkVisibility.Protected.getValue());

            executeAndCheck(stmt,
                "alter index gsi_lc_rc on table lc_rc_tp1 merge subpartitions sp1,sp2 to sp1",
                "lc_rc_tp1",
                "ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
                    + "\tMERGE SUBPARTITIONS sp1, sp2 TO sp1",
                CdcDdlMarkVisibility.Protected.getValue()
            );

            executeAndCheck(stmt,
                "/*# reorg*/alter index gsi_lc_rc on table lc_rc_tp1 reorganize subpartition sp0,sp1 into ( subpartition sp4 values less than ('2021-01-01','a'), subpartition sp5 values less than ('2028-01-01','a'), subpartition sp3 values less than (maxvalue, maxvalue) )",
                "lc_rc_tp1",
                "ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
                    + "\tREORGANIZE SUBPARTITION sp0, sp1 INTO (SUBPARTITION sp4 VALUES LESS THAN ('2021-01-01', 'a'), SUBPARTITION sp5 VALUES LESS THAN ('2028-01-01', 'a'), SUBPARTITION sp3 VALUES LESS THAN (maxvalue, maxvalue))",
                CdcDdlMarkVisibility.Protected.getValue());

            executeAndCheck(stmt,
                "/*# rename*/alter index gsi_lc_rc on table lc_rc_tp1 rename subpartition sp4 to sp0, sp3 to sp1",
                "lc_rc_tp1",
                "ALTER INDEX gsi_lc_rc ON TABLE lc_rc_tp1\n"
                    + "\tRENAME SUBPARTITION sp4 TO sp0, sp3 TO sp1", CdcDdlMarkVisibility.Protected.getValue());
        }
    }

    private void executeAndCheck(Statement stmt, String ddl, String sequenceName, String expectedToStringSql,
                                 int expectVisibility)
        throws SQLException {
        String tokenHints = buildTokenHints();
        String sql = tokenHints + ddl;
        stmt.executeUpdate(sql);

        String schemaName = getCurrentDbName(stmt);

        List<DdlRecordInfo> list = getDdlRecordInfoListByToken(tokenHints);
        Assert.assertEquals(1, list.size());
        Assert.assertEquals(sql, list.get(0).getDdlSql());
        Assert.assertEquals(schemaName, list.get(0).getSchemaName());
        Assert.assertEquals(sequenceName, list.get(0).getTableName());
        Assert.assertEquals(DdlScope.Schema.getValue(), list.get(0).getDdlExtInfo().getDdlScope());
        Assert.assertEquals(expectVisibility, list.get(0).getVisibility());
        Assert.assertEquals(true, list.get(0).getDdlExtInfo().getGsi());

        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(ddl));
        List<SQLStatement> parseResult = parser.parseStatementList();
        String toStringSql = parseResult.get(0).toString();
        Assert.assertEquals(expectedToStringSql, toStringSql);

    }
}
