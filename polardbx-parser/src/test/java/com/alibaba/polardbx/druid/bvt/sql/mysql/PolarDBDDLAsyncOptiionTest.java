package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import org.junit.Assert;
import org.junit.Ignore;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class PolarDBDDLAsyncOptiionTest extends MysqlTest {

    public void testDDLManageJob() {
        String sql = "continue ddl 1234 async=true";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        SQLStatement result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "continue ddl 1234 async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "continue ddl 1234";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);

        sql = "cancel ddl 1234 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "cancel ddl 1234 async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "cancel ddl 1234";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);

        sql = "rollback ddl 1234 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "rollback ddl 1234 async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "rollback ddl 1234";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);

        sql = "pause ddl 1234 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "pause ddl 1234 async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "pause ddl 1234";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);

        sql = "pause rebalance 1234 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "pause rebalance 1234 async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "pause rebalance 1234";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);

        sql = "continue rebalance 1234 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "continue rebalance 1234 async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "continue rebalance 1234";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);

        sql = "cancel rebalance 1234 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "cancel rebalance 1234 async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "cancel rebalance 1234";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);

        sql = "resume rebalance 1234 async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "resume rebalance 1234";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DRDSAsyncDDL);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);
    }

    public void testOptTable() {
        String sql = "optimize table tb async=true";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        SQLStatement result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "optimize table tb async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "optimize table tb";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);
    }

    public void testAnalyzeTable() {
        String sql = "analyze table tb async=true";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        SQLStatement result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "analyze table tb async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "analyze table tb";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);
    }

    public void testAlterTable() {
        String sql = "alter table tb add index i1(a) async=true";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        SQLStatement result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "alter table tb add index i1(a) async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "alter table tb add index i1(a)";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);

        sql = "alter table tb add global index i1(a) partition by key(a) partitions 2 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "alter table tb add global index i1(a) partition by key(a) partitions 2 async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "alter table tb add global index i1(a) partition by key(a) partitions 2";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);

        sql = "alter table tb modify column a bigint async=true;";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "alter table tb modify column a bigint async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "alter table tb modify column a bigint";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);

        sql = "alter table tb add column a bigint async=true;";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "alter table tb add column a bigint async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "alter table tb add column a bigint";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);

        sql = "alter table tb split partition p1 async=true;";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "alter table tb split partition p1 async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "alter table tb split partition p1";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);

        sql = "alter table tb move partitions p1,p2 to 'dn' async=true;";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "alter table tb move partitions p1,p2 to 'dn' async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "alter table tb move partitions p1,p2 to 'dn'";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);
    }

    public void testAlterTableGroup() {
        String sql = "alter tablegroup tb move partitions p1,p2 to 'dn' async=true;";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        SQLStatement result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "alter tablegroup tb move partitions p1,p2 to 'dn' async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "alter tablegroup tb move partitions p1,p2 to 'dn'";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);

        sql = "alter tablegroup tb split partition p1 async=true;";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "alter tablegroup tb split partition p1 async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "alter tablegroup tb split partition p1";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);

        sql = "alter tablegroup tb merge partitions p1,p2 to p12 async=true;";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "alter tablegroup tb merge partitions p1,p2 to p12 async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "alter tablegroup tb merge partitions p1,p2 to p12";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);
    }

    public void testCreateGlobalIndex() {
        String sql = "create global index g1 on tb(a) partition by key(a) partitions 2 async=true";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        SQLStatement result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "create global index g1 on tb(a) partition by key(a) partitions 2 async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "create global index g1 on tb(a) partition by key(a) partitions 2";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);
    }

    public void testCreateLocalIndex() {
        String sql = "create index i1 on tb(a) async=true";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        SQLStatement result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "create index i1 on tb(a) async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "create index i1 on tb(a)";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);
    }

    public void testMoveDatabase() {
        String sql = "move database db_dsad_group_000001 to 'dn' async=true";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        SQLStatement result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.TRUE);

        sql = "move database db_dsad_group_000001 to 'dn' async=false";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), Boolean.FALSE);

        sql = "move database db_dsad_group_000001 to 'dn'";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        result = parser.parseStatementList().get(0);
        Assert.assertEquals(result.getAsync(), null);
    }

    public void testNegativeTest() {
        String sql = "create table t1(a int) async=true";
        SQLStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        SQLStatement result = null;
        try {
            result = parser.parseStatementList().get(0);
            Assert.assertTrue(false);
        } catch (Exception ex) {
            Assert.assertTrue(true);
        }

        sql = "drop table t1 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        try {
            result = parser.parseStatementList().get(0);
            Assert.assertTrue(false);
        } catch (Exception ex) {
            Assert.assertTrue(true);
        }

        sql = "create view v1 as select * from t1 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        try {
            result = parser.parseStatementList().get(0);
            Assert.assertTrue(false);
        } catch (Exception ex) {
            Assert.assertTrue(true);
        }

        sql = "drop view t1 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        try {
            result = parser.parseStatementList().get(0);
            Assert.assertTrue(false);
        } catch (Exception ex) {
            Assert.assertTrue(true);
        }

        sql = "create sequence q1 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        try {
            result = parser.parseStatementList().get(0);
            Assert.assertTrue(false);
        } catch (Exception ex) {
            Assert.assertTrue(true);
        }

        sql = "drop sequence q1 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        try {
            result = parser.parseStatementList().get(0);
            Assert.assertTrue(false);
        } catch (Exception ex) {
            Assert.assertTrue(true);
        }

        sql = "create tablegroup tg1 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        try {
            result = parser.parseStatementList().get(0);
            Assert.assertTrue(false);
        } catch (Exception ex) {
            Assert.assertTrue(true);
        }

        sql = "drop tablegroup tg1 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        try {
            result = parser.parseStatementList().get(0);
            Assert.assertTrue(false);
        } catch (Exception ex) {
            Assert.assertTrue(true);
        }

        sql = "create database tg1 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        try {
            result = parser.parseStatementList().get(0);
            Assert.assertTrue(false);
        } catch (Exception ex) {
            Assert.assertTrue(true);
        }

        sql = "drop database tg1 async=true";
        parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        try {
            result = parser.parseStatementList().get(0);
            Assert.assertTrue(false);
        } catch (Exception ex) {
            Assert.assertTrue(true);
        }
    }

}
